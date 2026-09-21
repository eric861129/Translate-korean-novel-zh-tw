"""小段產物與共用閱讀紀錄；呼叫者提供實際校對，完整評分留在分節及全書。"""

from copy import deepcopy
from pathlib import Path
import re
from types import SimpleNamespace
from collections.abc import Mapping


class ChunkViews(Mapping):
    """保留既有 views 查詢介面，只在取得某個小段時載入其原分節。"""

    def __init__(self, flow):
        self.flow = flow
        self.chunks = {row["id"]: row for row in flow.plan["chunks"]}
        self.sections = {row["id"]: row for row in flow.manifest["sections"]}

    def __iter__(self):
        return iter(self.chunks)

    def __len__(self):
        return len(self.chunks)

    def __contains__(self, key):
        return key in self.chunks

    def __getitem__(self, key):
        row = self.chunks[key]
        section = self.flow.c.load_section(self.flow.project, self.sections[row["section_id"]])
        blocks = section["blocks"]
        if blocks:
            ids = [b["id"] for b in blocks]
            blocks = blocks[ids.index(row["first_source_id"]):ids.index(row["last_source_id"]) + 1]
        return {**section, "blocks": blocks, "source_sha256": row["source_sha256"]}


def input_data(core, path):
    """固定 JSON 介面；stdin 與檔案均嚴格使用 UTF-8。"""
    import sys
    data = core.json.loads(sys.stdin.buffer.read().decode("utf-8-sig")) if str(path) == "-" else core.load_json(Path(path))
    core.require(isinstance(data, dict), "輸入 JSON 必須是物件。")
    return data


def expand_review(core, data, section, manifest):
    """精簡輸入只展開欄位；狀態、分數、位置及判斷都必須由作者明列。"""
    if "format" not in data:
        return data
    core.require(data["format"] == "compact-review-v1", "不支援的複核輸入格式。")
    result = deepcopy(data)
    result.pop("format")
    evidence_ids = {section["id"], *(b["id"] for b in section["blocks"])}

    def expand_rows(rows, keys, fields, allowed):
        core.require(isinstance(rows, dict) and set(rows) == set(keys), "精簡複核項目缺漏或含未知項目。")
        expanded = {}
        for key, row in rows.items():
            core.require(isinstance(row, list) and len(row) == 3, f"{key} 必須明列判定或分數、來源位置、具體結論三欄。")
            core.require(isinstance(row[1], list) and row[1] and all(isinstance(x, str) and x in allowed for x in row[1]),
                         f"{key} 缺少有效來源位置。")
            core.require(isinstance(row[2], str) and row[2].strip(), f"{key} 缺少具體結論。")
            expanded[key] = dict(zip(fields, row))
        return expanded

    result["reviews"] = expand_rows(data.get("reviews"), core.STAGES + ("heading",), ("status", "evidence", "note"), evidence_ids)
    card = data.get("scorecard")
    core.require(isinstance(card, dict), "缺少十項評分表。")
    if card.get("method") != "not_applicable":
        result["scorecard"] = {**card, "items": expand_rows(card.get("items"), core.SCORE_DIMENSIONS,
                                                          ("score", "evidence", "note"), evidence_ids)}
    if "book_reviews" in data:
        book_ids = evidence_ids | {s["id"] for s in manifest["sections"]}
        result["book_reviews"] = expand_rows(data["book_reviews"], core.BOOK_STAGES, ("status", "evidence", "note"), book_ids)
    return result


class Workflow:
    """沿用主工具的驗證器，集中處理可重算的檔案與進度欄位。"""

    def __init__(self, core, work, *, full=False, project=None):
        self.c = core
        from verification_cache import VerificationCache
        self.verification = None if full else VerificationCache(core, work)
        self.project = project if project is not None else core.load_project(work, verification=self.verification)
        core.require(project is None or full, "外部專案快照僅供完整驗證使用。")
        self.work, self.manifest, self.terms, self.continuity, _ = self.project
        self.plan_path = self.work / "batch-plan.json"
        plan_raw = self.plan_path.read_bytes()
        self.plan = core.json.loads(plan_raw.decode("utf-8"))
        self.plan_sha = core.digest(plan_raw)
        if self.verification:
            self.verification.input_shas["batch-plan.json"] = self.plan_sha
        self.views = {}
        self.checked = {}
        self.validate_plan()
        self.progress_path = self.work / "batch-progress.json"
        binding = {"source_sha256": self.manifest["source"]["sha256"],
                   "reference_sha256": self.manifest["reference"]["sha256"], "plan_sha256": self.plan_sha}
        if self.progress_path.exists():
            self.progress = core.load_json(self.progress_path)
            core.require(self.progress.get("schema_version") == 1 and
                         all(self.progress.get(k) == v for k, v in binding.items()), "進度與來源或批次計畫指紋不符。")
        else:
            self.progress = {"schema_version": 1, **binding}
        for key in ("chunks", "section_review", "book_review"):
            core.require(isinstance(self.progress.setdefault(key, {}), dict), f"進度 {key} 必須是物件。")

    def validate_plan(self):
        """確認計畫完整覆蓋原稿，避免只憑游標或手填來源指紋通過。"""
        c, plan = self.c, self.plan
        c.require(plan.get("schema_version") == 1 and plan.get("source_sha256") == self.manifest["source"]["sha256"], "批次計畫來源不符。")
        for key, expected in (("source_encoding", self.manifest["source"]["encoding"]), ("heading_pattern", self.manifest["heading_pattern"])):
            c.require(key not in plan or plan[key] == expected, "批次計畫解析設定不符。")
        proof = c.digest(c.canonical([self.verification.index_sha, self.plan_sha])) if self.verification else None
        reused = bool(self.verification and self.verification.get("plan", proof))
        if reused:
            self.views = ChunkViews(self)
            return
        sections = {s["id"]: c.load_section(self.project, s) for s in self.manifest["sections"]}
        covered = []
        for chunk in plan["chunks"]:
            identity = chunk["id"]
            c.require(re.fullmatch(r"c\d{6}", identity) and identity not in self.views, "小段 ID 不合法或重複。")
            section = sections[chunk["section_id"]]
            ids = [b["id"] for b in section["blocks"]]
            if ids:
                first, last = chunk["first_source_id"], chunk["last_source_id"]
                c.require(first in ids and last in ids and ids.index(first) <= ids.index(last), "小段來源範圍不合法。")
                blocks = section["blocks"][ids.index(first):ids.index(last) + 1]
            else:
                c.require(chunk["first_source_id"] is None and chunk["last_source_id"] is None, "空分節範圍不符。")
                c.require(not any(s["id"] == section["id"] for s in self.views.values()), "空分節不能重複分配小段。")
                blocks = []
            if not reused:
                chars = sum(c.content_chars(b["text"]) for b in blocks)
                c.require(not chunk.get("requires_manual_split") and chunk["source_sha256"] == c.digest(c.canonical(blocks))
                          and chunk["block_count"] == len(blocks) and chunk["chars"] == chars
                          and chars <= plan["policy"]["max_chars"], "小段來源指紋不符或尚待人工拆分。")
            self.views[identity] = {**section, "blocks": blocks, "source_sha256": chunk["source_sha256"]}
            covered.extend((section["id"], b["id"]) for b in blocks)
        expected = [(s["id"], b["id"]) for s in sections.values() for b in s["blocks"]]
        c.require(covered == expected and set(s["id"] for s in self.views.values()) == set(sections), "計畫來源覆蓋缺漏、重複或順序錯誤。")
        chunk_ids = [x["id"] for x in plan["chunks"]]
        c.require([identity for b in plan["batches"] for identity in b["chunk_ids"]] == chunk_ids, "工作單位未完整覆蓋小段。")
        for batch in plan["batches"]:
            c.require(batch["chunk_ids"] and all(self.views[x]["id"] == batch["section_id"] for x in batch["chunk_ids"]), "工作單位跨分節或為空。")
        batch_ids = [b["id"] for b in plan["batches"]]
        c.require(len(batch_ids) == len(set(batch_ids)) and
                  [x for d in plan["delivery_batches"] for x in d["work_unit_ids"]] == batch_ids, "交付批次未完整覆蓋工作單位。")
        owners = {}
        batches = {b["id"]: b for b in plan["batches"]}
        for delivery in plan["delivery_batches"]:
            for identity in delivery["work_unit_ids"]:
                section_id = batches[identity]["section_id"]
                c.require(owners.setdefault(section_id, delivery["id"]) == delivery["id"], "同一原分節不能跨交付批次。")
        if self.verification:
            self.verification.remember("plan", proof)
        self.views = ChunkViews(self)

    def path(self, folder, identity, suffix):
        pattern = r"c\d{6}" if folder == "chunks" else r"s\d{4,}"
        self.c.require(re.fullmatch(pattern, identity), "產物 ID 不合法。")
        path = (self.work / folder / (identity + suffix)).resolve()
        protected = {Path(self.manifest[k]["path"]).resolve() for k in ("source", "reference")}
        self.c.require(path.is_relative_to(self.work) and path not in protected, "產物路徑不得指向輸入或工作目錄外。")
        return path

    def section_chunks(self, identity):
        return [x["id"] for x in self.plan["chunks"] if x["section_id"] == identity]

    def context(self, chunk_id):
        section = self.views[chunk_id]
        previous_section = None
        if section["ordinal"]:
            prior_id = self.manifest["sections"][section["ordinal"] - 1]["id"]
            previous_section = next((r for r in self.continuity if r["section_id"] == prior_id), None)
            self.c.require(previous_section is not None, "缺少前一分節敘事狀態。")
        ids = self.section_chunks(section["id"])
        index = ids.index(chunk_id)
        previous_chunk = None
        if index:
            prior_id = ids[index - 1]
            row = self.progress["chunks"].get(prior_id, {})
            self.c.require(row.get("status") in ("self_checked", "merged") and row.get("state_after"), "前一小段尚未完成核對。")
            previous_chunk = {"chunk_id": prior_id, "state_after": row["state_after"]}
        return {"previous_section": previous_section, "previous_chunk": previous_chunk}

    def checked_chunks(self, ids):
        c, result = self.c, []
        for identity in ids:
            text_path, map_path = self.path("chunks", identity, ".txt"), self.path("chunks", identity, ".map.json")
            text, _, sha = c.read_text(text_path, "utf-8")
            mapping, state = c.load_json(map_path), self.progress["chunks"].get(identity, {})
            c.require(state.get("status") in ("self_checked", "merged") and state.get("translation_sha256") == sha
                      and state.get("mapping_sha256") == c.digest(map_path.read_bytes())
                      and state.get("source_sha256") == self.views[identity]["source_sha256"], "小段尚未核對或產物指紋已改變。")
            before = self.context(identity)
            stamp = c.digest(c.canonical([sha, mapping, state, before, self.progress.get("style_profile", {})]))
            if self.checked.get(identity) == stamp:
                result.append((identity, text, mapping))
                continue
            receipt = c.check_section(self.project, self.views[identity], artifact=(text, mapping), chunk=True)
            c.require(mapping.get("chunk_id") == identity and mapping.get("term_dependencies") == receipt["term_dependencies"]
                      and mapping.get("context_before") == before
                      and mapping.get("context_before_sha256") == c.digest(c.canonical(before))
                      and mapping.get("context_after_sha256") == c.digest(c.canonical(state.get("state_after"))), "小段專名或敘事依據已失效，需重新核對。")
            self.checked[identity] = stamp
            result.append((identity, text, mapping))
        return result

    def reading_records(self):
        """只沿用仍綁定有效整節收據的閱讀紀錄，不替未讀範圍補聲明。"""
        c, valid, receipts = self.c, {}, {}
        batches = {b["id"]: b for b in self.plan["batches"]}
        rows = self.progress["book_review"].get("completed_batches", [])
        c.require(isinstance(rows, list), "全書閱讀紀錄必須是清單。")
        seen = set()
        for row in rows:
            identity = row["batch_id"]
            c.require(identity not in seen and identity in batches, "全書閱讀紀錄有重複或未知工作單位。")
            seen.add(identity)
            batch = batches[identity]
            section_id = batch["section_id"]
            if section_id not in receipts:
                section = c.section_by_id(self.manifest, section_id)
                try:
                    receipt = (self.verification.checked_section(self.project, section, self.progress.get("style_profile", {}))
                               if self.verification else c.check_section(self.project, section))
                    receipts[section_id] = receipt if section.get("receipt") == receipt else None
                except (c.PipelineError, OSError, ValueError, KeyError, TypeError):
                    receipts[section_id] = None
            receipt = receipts[section_id]
            if (receipt and row.get("section_id") == section_id and row.get("chunk_ids") == batch["chunk_ids"]
                    and row.get("translation_sha256") == receipt["translation_sha256"]
                    and row.get("mapping_sha256") == receipt["mapping_sha256"]
                    and row.get("receipt_sha256") == c.digest(c.canonical(receipt))
                    and c.passed_reviews(row.get("reviews"), c.BOOK_STAGES)):
                valid[identity] = row
        return valid

    def write_progress(self, valid=None):
        valid = self.reading_records() if valid is None else valid
        pending = [b for b in self.plan["batches"] if b["id"] not in valid]
        self.progress["book_review"] = {"completed_batches": [valid[b["id"]] for b in self.plan["batches"] if b["id"] in valid],
                                        "next_batch": pending[0]["id"] if pending else None}
        delivery = next((d for d in self.plan["delivery_batches"] if any(x not in valid for x in d["work_unit_ids"])), None)
        candidates = [b for b in pending if delivery and b["id"] in delivery["work_unit_ids"]]
        untranslated = [b for b in candidates if any(self.progress["chunks"].get(x, {}).get("status") not in ("self_checked", "merged") for x in b["chunk_ids"])]
        active = (untranslated or candidates or [None])[0]
        self.progress.update(active_delivery_batch=delivery["id"] if delivery else None,
                             active_batch=active["id"] if active else None,
                             phase="translating" if pending else "finalizing")
        self.c.atomic_json(self.progress_path, self.progress)

    def invalidate_section(self, identity):
        section = self.c.section_by_id(self.manifest, identity)
        if section.pop("receipt", None) is not None:
            self.c.atomic_json(self.work / "manifest.json", self.manifest)
        if identity in self.progress["section_review"]:
            self.progress["section_review"][identity].update(reviewed_chunk_ids=[], next_chunk=next(iter(self.section_chunks(identity)), None))
        rows = self.progress["book_review"].get("completed_batches", [])
        self.progress["book_review"]["completed_batches"] = [r for r in rows if r.get("section_id") != identity]
        if isinstance(self.progress.get("final_output"), dict):
            self.progress["final_output"]["sha256"] = None

    def render_units(self, units, expected):
        """依呼叫者提供的語意分組排版，只計算行號，不猜測句子對應。"""
        c, lines, alignments, covered, allowances = self.c, [], [], [], []
        c.require(isinstance(units, list), "units 必須是清單。")
        positions = {identity: index for index, identity in enumerate(expected)}
        for unit in units:
            c.require(isinstance(unit, dict) and ("source_ids" in unit) != ("source_range" in unit), "語意單位須擇一使用 source_ids 或 source_range。")
            if "source_range" in unit:
                span = unit["source_range"]
                c.require(isinstance(span, list) and len(span) == 2 and all(isinstance(x, str) and x in positions for x in span)
                          and positions[span[0]] <= positions[span[1]], "source_range 必須是當前小段內順序正確的兩個來源 ID。")
                ids = expected[positions[span[0]]:positions[span[1]] + 1]
            else:
                ids = unit["source_ids"]
            c.require(isinstance(ids, list) and ids and all(isinstance(x, str) for x in ids), "語意單位缺少來源 IDs。")
            covered.extend(ids)
            if unit.get("disposition") == "excluded":
                c.require("text" not in unit and unit.get("reason") in c.EXCLUSIONS and unit.get("note"), "排除須有明確原因與實際依據。")
                alignments.append({"source_ids": ids, **{k: unit[k] for k in ("disposition", "reason", "note")}})
                continue
            c.require(unit.get("disposition", "translated") == "translated" and isinstance(unit.get("text"), str) and unit["text"].strip(), "翻譯單位需有完整文字。")
            text = unit["text"].replace("\r\n", "\n").replace("\r", "\n").strip("\n")
            if lines:
                lines.append("")
            start = len(lines) + 1
            lines.extend(text.split("\n"))
            alignments.append({"source_ids": ids, "disposition": "translated", "target_lines": [start, len(lines)]})
            for allowance in unit.get("language_allowances", []):
                c.require(isinstance(allowance.get("text"), str) and allowance["text"] and allowance.get("reason"), "用語例外缺少文字或依據。")
                matches = [i for i in range(start, len(lines) + 1) if allowance["text"] in lines[i - 1]]
                c.require(matches, "用語例外未出現在該翻譯單位。")
                allowances.extend({"text": allowance["text"], "reason": allowance["reason"], "source_ids": ids, "target_lines": [i, i]} for i in matches)
        c.require(covered == expected, "語意單位來源 IDs 缺漏、重複或順序不符。")
        return "\n".join(lines) + ("\n" if lines else ""), alignments, allowances

    def save_chunk(self, args, *, data=None, persist_input=False, defer_progress=False, validate_only=False):
        c, identity = self.c, args.chunk
        c.require(identity in self.views, "計畫中沒有此小段。")
        section = self.views[identity]
        data = input_data(c, args.input) if data is None else data
        import section_context
        bundle_terms = []
        if "context_state" in data or section_context.cache_path(c, self.work, section["id"], self.manifest).exists():
            bundle = section_context.require_current_bundle(c, self.work, section["id"], data.get("context_state"), flow=self)
            bundle_terms = [t["id"] for t in bundle["terminology"]]
        siblings = self.section_chunks(section["id"])
        self.checked_chunks(siblings[:siblings.index(identity)])
        text, alignments, allowances = self.render_units(data["units"], [b["id"] for b in section["blocks"]])
        state = data.get("state_after")
        c.require(isinstance(state, dict) and all(isinstance(state.get(k), str) and state[k].strip() for k in ("viewpoint", "time_place"))
                  and all(isinstance(state.get(k), list) for k in ("speakers", "known_facts", "open_threads")), "需提供實際的小段結束敘事狀態。")
        before = self.context(identity)
        body = (section["source_heading"] or "") + "\n" + "\n".join(b["text"] for b in section["blocks"])
        detected = [t["id"] for t in self.terms if t.get("scope", {}).get("from", 0) <= section["ordinal"] <= t.get("scope", {}).get("through", len(self.manifest["sections"])) and any(form in body for form in t["ko"])]
        c.require(isinstance(data.get("term_ids", []), list) and all(isinstance(x, str) for x in data.get("term_ids", [])), "term_ids 必須是字串清單。")
        mapping = {"schema_version": 1, "chunk_id": identity, "section_id": section["id"], "source_sha256": section["source_sha256"],
                   "translation_sha256": c.digest(text), "alignments": alignments, "term_ids": sorted(set(detected + bundle_terms + data.get("term_ids", []))),
                   "reviews": data.get("reviews"), "context_before": before,
                   "style_profile_sha256": c.digest(c.canonical(self.progress.get("style_profile", {}))),
                   "context_before_sha256": c.digest(c.canonical(before)), "context_after_sha256": c.digest(c.canonical(state))}
        if "context_state" in data:
            mapping["context_state"] = data["context_state"]
        if "parallel_review" in data:
            mapping["parallel_review"] = data["parallel_review"]
        if allowances:
            mapping["language_allowances"] = allowances
        if "allowed_hangul" in data:
            mapping["allowed_hangul"] = data["allowed_hangul"]
        receipt = c.check_section(self.project, section, artifact=(text, mapping), chunk=True)
        mapping["term_dependencies"] = receipt["term_dependencies"]
        text_path, map_path = self.path("chunks", identity, ".txt"), self.path("chunks", identity, ".map.json")
        outputs = [(text_path, text), (map_path, c.json_text(mapping))]
        input_path = self.path("chunks", identity, ".input.json")
        if persist_input and input_path.exists():
            c.require(c.load_json(input_path) == data or args.replace, "輸入草稿已有不同內容；確認後使用 --replace。")
        changed = any(p.exists() and p.read_bytes() != value.encode("utf-8") for p, value in outputs)
        if changed:
            old = self.progress["chunks"].get(identity, {})
            c.require(args.replace and all((p.exists() and (p.read_bytes() == value.encode("utf-8") or c.digest(p.read_bytes()) == old.get(key)))
                                          or (not p.exists() and old.get("status") == "awaiting_recheck" and key in old and old[key] is None)
                                          for (p, value), key in zip(outputs, ("translation_sha256", "mapping_sha256"))),
                      "小段已有不同內容；確認未被外部修改後使用 --replace 修訂。")
            if not validate_only:
                self.invalidate_section(section["id"])
        if validate_only:
            return {"validated_chunk": identity, "artifacts": {
                "translation_sha256": c.digest(text), "mapping_sha256": c.digest(c.json_text(mapping)),
                "input_sha256": c.digest(c.json_text(data))}}
        text_path.parent.mkdir(parents=True, exist_ok=True)
        if persist_input:
            c.atomic_json(input_path, data)
        for path, value in outputs:
            c.atomic_text(path, value)
        status = "merged" if not changed and self.progress["chunks"].get(identity, {}).get("status") == "merged" else "self_checked"
        self.progress["chunks"][identity] = {"status": status, "source_sha256": section["source_sha256"],
                                            "translation_sha256": c.digest(text_path.read_bytes()), "mapping_sha256": c.digest(map_path.read_bytes()), "state_after": state}
        if not defer_progress:
            self.write_progress()
        return {"saved": identity, "status": status, "translation": str(text_path), "mapping": str(map_path)}

    def merge_section(self, args, *, defer_progress=False):
        c, identity = self.c, args.section
        section = c.section_by_id(self.manifest, identity)
        chunks = self.checked_chunks(self.section_chunks(identity))
        c.require(chunks, "本節沒有計畫小段。")
        title = args.title
        c.require((isinstance(title, str) and title.strip() and "\n" not in title and "\r" not in title) or
                  (title is None and section["heading"]["kind"] == "preamble" and all(not text for _, text, _ in chunks)), "需提供實際分節標題；只有全數排除的前置文字可省略。")
        lines, alignments, allowances, hangul, terms, inputs = ([title, ""] if title else []), [], [], [], set(), []
        for chunk_id, text, mapping in chunks:
            offset = len(lines)
            for entry in mapping["alignments"]:
                row = deepcopy(entry)
                if "target_lines" in row:
                    row["target_lines"] = [n + offset for n in row["target_lines"]]
                alignments.append(row)
            for entry in mapping.get("language_allowances", []):
                allowances.append({**entry, "target_lines": [n + offset for n in entry["target_lines"]]})
            hangul.extend(mapping.get("allowed_hangul", []))
            terms.update(mapping["term_ids"])
            lines.extend(text.splitlines())
            if text:
                lines.append("")
            inputs.append({"chunk_id": chunk_id, **{k: self.progress["chunks"][chunk_id][k] for k in ("translation_sha256", "mapping_sha256")}})
        text = "\n".join(lines).rstrip("\n") + ("\n" if title or any(t for _, t, _ in chunks) else "")
        mapping = {"schema_version": 1, "section_id": identity, "source_sha256": section["source_sha256"], "translation_sha256": c.digest(text),
                   "title_zh": title, "alignments": alignments, "term_ids": sorted(terms), "reviews": {}, "scorecard": {},
                   "style_profile_sha256": c.digest(c.canonical(self.progress.get("style_profile", {}))),
                   "assembly": {"plan_sha256": self.plan_sha, "chunks": inputs, "translation_sha256": c.digest(text)}}
        if allowances:
            mapping["language_allowances"] = allowances
        if hangul:
            mapping["allowed_hangul"] = hangul
        text_path, map_path = self.path("sections", identity, ".txt"), self.path("sections", identity, ".map.json")
        outputs = [(text_path, text), (map_path, c.json_text(mapping))]
        if any(p.exists() and p.read_bytes() != value.encode("utf-8") for p, value in outputs):
            old = c.load_json(map_path) if map_path.exists() else {}
            known = self.progress["section_review"].get(identity, {})
            c.require(args.replace and "assembly" in old and all(p.exists() and
                      (p.read_bytes() == value.encode("utf-8") or c.digest(p.read_bytes()) == known.get(key))
                      for (p, value), key in zip(outputs, ("translation_sha256", "mapping_sha256"))),
                      "分節已有不同內容或人工修訂；不得用小段稿覆蓋。工具產物重建需 --replace。")
        self.invalidate_section(identity)
        for path, value in outputs:
            c.atomic_text(path, value)
        self.progress["section_review"][identity] = {"translation_sha256": c.digest(text_path.read_bytes()), "mapping_sha256": c.digest(map_path.read_bytes()),
                                                      "reviewed_chunk_ids": [], "next_chunk": chunks[0][0]}
        if not defer_progress:
            self.write_progress()
        return {"merged": identity, "status": "awaiting_section_review", **self.read_section(identity)}

    def submit_chunk(self, args):
        """同一鎖內保存作者輸入及產物；後續合併失敗時回報可恢復的保存點。"""
        data = input_data(self.c, args.input)
        self.c.require(args.chunk in self.views, "計畫中沒有此小段。")
        identity = self.views[args.chunk]["id"]
        result = self.save_chunk(args, data=data, persist_input=True, defer_progress=True)
        result["input"] = str(self.path("chunks", args.chunk, ".input.json"))
        siblings = self.section_chunks(identity)
        pending = next((x for x in siblings if self.progress["chunks"].get(x, {}).get("status") not in ("self_checked", "merged")), None)
        result["next"] = {"action": "translate_chunk", "section_id": identity, "chunk_id": pending} if pending else {"action": "merge_section", "section_id": identity}
        if pending is None and args.merge:
            try:
                # 相同輸入重試不覆蓋已合併或已人工修訂的分節。
                known = self.progress["section_review"].get(identity, {})
                current = [(self.path("sections", identity, suffix), key) for suffix, key in ((".txt", "translation_sha256"), (".map.json", "mapping_sha256"))]
                section = self.c.section_by_id(self.manifest, identity)
                reviewed = False
                if known and section.get("receipt") and all(p.exists() and self.c.digest(p.read_bytes()) == known.get(key) for p, key in current):
                    reviewed = self.c.check_section(self.project, section) == section["receipt"]
                if reviewed:
                    result["next"] = {"action": "section_already_reviewed", "section_id": identity}
                else:
                    result["section"] = self.merge_section(SimpleNamespace(section=identity, title=args.title, replace=args.replace), defer_progress=True)
                    result["next"] = {"action": "read_section", "section_id": identity,
                                      "translation": str(self.path("sections", identity, ".txt"))}
            except (self.c.PipelineError, OSError, ValueError, KeyError, TypeError) as exc:
                result["next"] = {"action": "resolve_merge", "section_id": identity, "error": str(exc)}
                if getattr(exc, "details", None):
                    result["next"]["details"] = exc.details
        self.write_progress()
        return result

    def review_state(self, identity):
        c = self.c
        c.section_by_id(self.manifest, identity)
        paths = {"translation": self.path("sections", identity, ".txt"), "mapping": self.path("sections", identity, ".map.json"),
                 **{name: self.work / name for name in ("terminology.jsonl", "continuity.jsonl", "reviews.jsonl")},
                 "language_rules": Path(c.__file__).resolve().parent.parent / "references/zh-tw-review-rules.json"}
        state = {"source": self.manifest["source"]["sha256"], "reference": self.manifest["reference"]["sha256"], "plan": self.plan_sha,
                 "style_profile": c.digest(c.canonical(self.progress.get("style_profile", {}))),
                 **{name: c.digest(path.read_bytes()) for name, path in paths.items()}}
        state["chunks"] = [{"id": chunk_id, "progress": self.progress["chunks"].get(chunk_id),
                            **{suffix: c.digest(path.read_bytes()) if path.exists() else None
                               for suffix in (".txt", ".map.json") for path in [self.path("chunks", chunk_id, suffix)]}}
                           for chunk_id in self.section_chunks(identity)]
        return {"section_id": identity, "reviewed_chunk_ids": self.section_chunks(identity), "state": c.digest(c.canonical(state))}

    def read_section(self, identity):
        """回傳整份待讀譯稿及綁定版本；輸出完成不表示模型已完成閱讀。"""
        before = self.review_state(identity)
        path = self.path("sections", identity, ".txt")
        text, _, sha = self.c.read_text(path, "utf-8")
        self.c.require(self.review_state(identity) == before, "讀稿期間版本已改變，請重新取得完整譯稿。")
        return {**before, "reading": {"path": str(path), "text": text, "translation_sha256": sha,
                                     "line_count": len(text.splitlines()), "complete": True}}

    def next_step(self, *, valid=None, extra_terms=()):
        """從最早的驗收或連讀缺口接續，不把目前節號加一當作完成證據。"""
        c = self.c
        valid = self.reading_records() if valid is None else valid
        checked = c.validate_project(self.project, verification=self.verification, stop_at_first=True,
                                     style=self.progress.get("style_profile", {}))
        missing_reading = next((b["section_id"] for b in self.plan["batches"] if b["id"] not in valid), None)
        candidates = [x for x in (checked["next"], missing_reading) if x is not None]
        if not candidates:
            return {"action": "final_review", "section_id": None,
                    "note": "分節及連讀紀錄齊備；仍需全書十項評分、完整驗證、組裝及讀回。"}
        identity = min(candidates, key=lambda x: c.section_by_id(self.manifest, x)["ordinal"])
        import section_context
        context = section_context.prepare_with_flow(c, self, identity, extra_terms)
        result = {"action": "resume_section", "section_id": identity, "context": context,
                  "validation_scope": "verified_prefix", "errors": checked["errors"],
                  "book_reading_pending": identity == missing_reading}
        if all(self.path("sections", identity, suffix).is_file() for suffix in (".txt", ".map.json")):
            result["section"] = self.read_section(identity)
        return result

    def review_section(self, args):
        c, identity = self.c, args.section
        raw_data = input_data(c, args.input)
        data = expand_review(c, raw_data, c.load_section(self.project, identity), self.manifest)
        snapshot = self.review_state(identity)
        c.require(data.get("state") == snapshot["state"], "複核版本已改變；請核對目前譯稿與依據後重新取得 review-state。")
        c.require(data.get("reviewed_chunk_ids") == snapshot["reviewed_chunk_ids"], "分節複核尚未完整覆蓋所有小段。")
        valid = self.reading_records()
        if "book_reviews" in data:
            c.require(c.passed_reviews(data["book_reviews"], c.BOOK_STAGES), "共用閱讀需要實際的四項全書複核結果。")
            before = [b for b in self.plan["batches"] if self.c.section_by_id(self.manifest, b["section_id"])["ordinal"] < self.c.section_by_id(self.manifest, identity)["ordinal"]]
            c.require(all(b["id"] in valid for b in before), "前序全書閱讀有缺口，不能把跳讀登記為連續閱讀。")
        text_path, map_path = self.path("sections", identity, ".txt"), self.path("sections", identity, ".map.json")
        text, _, _ = c.read_text(text_path, "utf-8")
        mapping = c.load_json(map_path)
        if "assembly" in mapping:
            chunks = self.checked_chunks(snapshot["reviewed_chunk_ids"])
            expected = [{"chunk_id": chunk_id, **{k: self.progress["chunks"][chunk_id][k] for k in ("translation_sha256", "mapping_sha256")}} for chunk_id, _, _ in chunks]
            c.require(mapping["assembly"].get("plan_sha256") == self.plan_sha and mapping["assembly"].get("chunks") == expected,
                      "小段已修訂但分節尚未重新合併，不能驗收舊分節。")
        mapping.update(reviews=data.get("reviews"), scorecard=data.get("scorecard"))
        section = c.section_by_id(self.manifest, identity)
        receipt = c.check_section(self.project, section, artifact=(text, mapping))
        # 檔案與 stdin、標準與精簡輸入都保存到同一正式位置，避免依賴散落的提交檔。
        review_path = self.path("sections", identity, ".review.json")
        c.atomic_json(review_path, data)
        c.atomic_json(map_path, mapping)
        section["receipt"] = receipt
        c.atomic_json(self.work / "manifest.json", self.manifest)
        self.progress["section_review"][identity] = {"translation_sha256": receipt["translation_sha256"], "mapping_sha256": receipt["mapping_sha256"],
                                                      "reviewed_chunk_ids": snapshot["reviewed_chunk_ids"], "next_chunk": None}
        for chunk_id in snapshot["reviewed_chunk_ids"]:
            if "assembly" in mapping and chunk_id in self.progress["chunks"]:
                self.progress["chunks"][chunk_id]["status"] = "merged"
        valid = {key: row for key, row in valid.items() if row["section_id"] != identity}
        if "book_reviews" in data:
            for batch in self.plan["batches"]:
                if batch["section_id"] == identity:
                    valid[batch["id"]] = {"batch_id": batch["id"], "chunk_ids": batch["chunk_ids"], "section_id": identity,
                                          "translation_sha256": receipt["translation_sha256"], "mapping_sha256": receipt["mapping_sha256"],
                                          "receipt_sha256": c.digest(c.canonical(receipt)), "reviews": data["book_reviews"]}
        self.write_progress(valid)
        result = {"recorded": identity, "input": str(review_path),
                  "average_score": receipt["score"]["average"], "shared_book_reading": "book_reviews" in data,
                  "next_book_batch": self.progress["book_review"]["next_batch"]}
        if getattr(args, "prepare_next", False):
            try:
                stamp = self.verification.section_state(self.project, section, self.progress.get("style_profile", {}), receipt=receipt)
                self.verification.remember("section:" + identity, stamp, receipt)
                result["next"] = self.next_step(valid=valid, extra_terms=getattr(args, "term_id", ()))
            except (c.PipelineError, OSError, ValueError, KeyError, TypeError) as exc:
                result["next"] = {"action": "resolve_next", "error": str(exc),
                                  "note": "本節複核已保存；修正後用 next --prepare 恢復，不重送舊閱讀聲明。"}
                if getattr(exc, "details", None):
                    result["next"]["details"] = exc.details
        return result


def verify_book_reading(core, work):
    """有批次計畫時，完整且仍有效的逐批閱讀才可支援全書聲明。"""
    flow = Workflow(core, work, full=True)
    valid = flow.reading_records()
    core.require(set(valid) == {b["id"] for b in flow.plan["batches"]}, "全書閱讀紀錄缺漏或已失效，請補齊實際複核。")
    return core.digest(core.canonical([valid[b["id"]] for b in flow.plan["batches"]]))


def run(core, args):
    work = Path(args.work).resolve()
    if args.command == "review-state":
        return Workflow(core, work).review_state(args.section)
    with core.writer_lock(work):
        from parallel_workflow import require_main
        require_main(core, work, getattr(args, "actor", None))
        flow = Workflow(core, work)
        result = {"save-chunk": flow.save_chunk, "submit-chunk": flow.submit_chunk, "merge-section": flow.merge_section, "review-section": flow.review_section}[args.command](args)
        flow.verification.flush(flow.manifest)
        return result
