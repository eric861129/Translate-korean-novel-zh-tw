#!/usr/bin/env python3
"""韓文小說字數盤點、批次規劃、逐段覆蓋與驗收組裝；不執行模型翻譯。"""

from __future__ import annotations

import argparse
import codecs
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import unicodedata
from statistics import median
from contextlib import contextmanager
from decimal import Decimal


class PipelineError(ValueError):
    """可向使用者直接說明的資料或驗收錯誤。"""

    def __init__(self, message, *, details=None):
        super().__init__(message)
        self.details = details


HEADER = re.compile(r"(?P<chapter>\d+)\s*화\s*(?P<title>.*?)(?:\s+\((?P<part>\d+|完)\))?")
SPECIAL = re.compile(r"(?:서장|프롤로그|에필로그|종장|외전|후기|작가의 말)(?:\s*\(\d+\))?")
DECORATION = re.compile(r"[─━=_*\-]{8,}")
HANGUL = re.compile(r"[\u1100-\u11ff\u3130-\u318f\uac00-\ud7af]")
PLACEHOLDER = re.compile(r"\[(?:TODO|待翻譯|略|稍後補譯)[^\]]*\]", re.I)
STAGES = ("fidelity", "referents", "fluency", "locale", "post_polish")
BOOK_STAGES = ("structure", "terminology", "continuity", "reading")
EXCLUSIONS = {"advertisement", "duplicate_heading", "layout"}
SCORE_DIMENSIONS = {
    "fidelity": "韓文原意還原",
    "naming_consistency": "人名／稱謂一致性",
    "relationships": "人物關係",
    "plot_continuity": "劇情連貫性",
    "characterization": "人物性格",
    "chinese_naturalness": "中文自然度",
    "taiwan_usage": "台灣中文感",
    "literary_style": "小說文學感",
    "dialogue": "對話自然度",
    "emotion": "情緒描寫",
}
MINIMUM_AVERAGE = Decimal("85")


def digest(value: bytes | str) -> str:
    return hashlib.sha256(value.encode("utf-8") if isinstance(value, str) else value).hexdigest()


def file_digest(path):
    """串流核對完整母檔，不為一般操作解碼全書。"""
    hasher = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


class Project(tuple):
    """保留既有 tuple 介面，來源儲存器只存在記憶體，不寫入 manifest。"""

    def __new__(cls, values, *, source_store=None):
        obj = super().__new__(cls, values)
        obj.source_store = source_store
        return obj


def load_section(project, section):
    """只有正文使用者需要解碼分節；目錄與收據可直接使用精簡索引。"""
    if isinstance(section, str):
        section = section_by_id(project[1], section)
    store = getattr(project, "source_store", None)
    return store.section(section["id"]) if store and "blocks" not in section else section


def canonical(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def require(condition, message):
    if not condition:
        raise PipelineError(message)


def read_text(path: Path, encoding="auto"):
    raw = path.read_bytes()
    if encoding == "auto":
        if raw.startswith((codecs.BOM_UTF32_LE, codecs.BOM_UTF32_BE)):
            encoding = "utf-32"
        elif raw.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
            encoding = "utf-16"
        elif raw.startswith(codecs.BOM_UTF8):
            encoding = "utf-8-sig"
        else:
            encoding = "utf-8"
    try:
        text = raw.decode(encoding, errors="strict")
    except (UnicodeError, LookupError) as exc:
        raise PipelineError(f"無法以 {encoding} 讀取 {path.name}；請指定已確認的編碼。") from exc
    require("\x00" not in text and "\ufffd" not in text, f"{path.name} 含 NUL 或替代字元，請確認原稿與編碼。")
    return text, encoding, digest(raw)


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def load_rows(path: Path, *, fingerprints=None):
    raw = path.read_bytes()
    rows = [json.loads(line) for line in raw.decode("utf-8").splitlines() if line.strip()]
    require(all(isinstance(row, dict) for row in rows), f"{path.name} 每行必須是 JSON 物件。")
    if fingerprints is not None:
        fingerprints[path.name] = digest(raw)
    return rows


def json_text(value):
    return json.dumps(value, ensure_ascii=False, indent=2) + "\n"


def atomic_text(path: Path, payload: str):
    """原子替換單一明確的工作檔案，避免留下半份內容。"""
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="\n", dir=path.parent, prefix=".state-", suffix=".tmp", delete=False) as stream:
        temp = Path(stream.name)
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.replace(temp, path)
    finally:
        if temp.exists():
            temp.unlink()


def atomic_json(path: Path, value):
    atomic_text(path, json_text(value))


@contextmanager
def writer_lock(work: Path):
    lock = work / ".writer.lock"
    try:
        with lock.open("x", encoding="utf-8") as stream:
            stream.write(str(os.getpid()))
    except FileExistsError as exc:
        raise PipelineError("工作目錄已鎖定；確認原程序結束後，才可移除這個明確的 lock 檔。") from exc
    try:
        yield
    finally:
        lock.unlink()


def clean_heading(line):
    line = line.strip().lstrip("<").rstrip(">").strip()
    return re.sub(r"\s*\(오류 수정\)\s*$", "", line).strip()


def parse_heading(line, pattern=None):
    cleaned = clean_heading(line)
    if re.search(r"\s+끝$", cleaned):
        return None
    match = re.fullmatch(pattern, cleaned) if pattern else HEADER.fullmatch(cleaned)
    if match:
        values = match.groupdict()
        return {"kind": values.get("kind") or "chapter", "chapter": values.get("chapter"), "title": values.get("title") or cleaned, "part": values.get("part")}
    if SPECIAL.fullmatch(cleaned):
        return {"kind": "special", "chapter": None, "title": cleaned, "part": None}
    return None


def parse_source(text, pattern=None):
    """每個非空原文行為內容區塊；只有明確版面與同節結束標記另列。"""
    lines = text.splitlines()
    sections = []
    current = None
    for number, raw in enumerate(lines, 1):
        line = raw.strip()
        ending = parse_heading(re.sub(r"\s+끝\s*$", "", line), pattern) if re.search(r"\s+끝\s*$", line) else None
        if current and ending and ending == current["heading"]:
            current["format_lines"].append({"line": number, "text": raw, "reason": "matching_end_marker"})
            continue
        heading = parse_heading(line, pattern)
        if heading:
            current = {"id": f"s{len(sections):04d}", "ordinal": len(sections), "heading": heading, "source_heading": raw, "start_line": number, "blocks": [], "format_lines": []}
            sections.append(current)
            continue
        if not line:
            continue
        if current is None:
            current = {"id": "s0000", "ordinal": 0, "heading": {"kind": "preamble", "chapter": None, "title": "來源前置文字", "part": None}, "source_heading": None, "start_line": number, "blocks": [], "format_lines": []}
            sections.append(current)
        if DECORATION.fullmatch(line):
            current["format_lines"].append({"line": number, "text": raw, "reason": "layout_separator"})
        else:
            current["blocks"].append({"id": f"{current['id']}:p{len(current['blocks']) + 1:04d}", "line": number, "text": raw})
    require(sections and any(s["heading"]["kind"] != "preamble" for s in sections), "找不到原稿章節；請確認檔案或指定 --heading-pattern。")
    for index, section in enumerate(sections):
        end = sections[index + 1]["start_line"] - 1 if index + 1 < len(sections) else len(lines)
        section["end_line"] = end
        section["source_sha256"] = digest("\n".join(lines[section["start_line"] - 1:end]))
    return sections


def text_metrics(text):
    """以 NFC 統一韓文字形後計數，不計換行；不改寫原稿與其指紋。"""
    normalized = unicodedata.normalize("NFC", text)
    return {
        "chars_with_spaces": sum(len(line) for line in normalized.splitlines()),
        "chars_without_whitespace": sum(not char.isspace() for char in normalized),
        "hangul_characters": len(HANGUL.findall(normalized)),
        "eojeol": len(normalized.split()),
    }


def content_chars(text):
    """批次驗證只需同口徑字元數，不重算韓文字數與空白詞數。"""
    return sum(map(len, unicodedata.normalize("NFC", text).splitlines()))


def length_distribution(values):
    lengths = sorted(values)
    if not lengths:
        return None
    return {"count": len(lengths), "min": lengths[0], "median": median(lengths),
            "mean": round(sum(lengths) / len(lengths), 2),
            "p90": lengths[(9 * len(lengths) + 9) // 10 - 1], "max": lengths[-1],
            "over_6000": sum(n > 6000 for n in lengths), "over_12000": sum(n > 12000 for n in lengths)}


def source_inventory(text, sections):
    """分開全檔、待處理內容與編號分節的統計，避免前置資料扭曲章長。"""
    metrics = {s["id"]: text_metrics("\n".join(b["text"] for b in s["blocks"])) for s in sections}
    ranked = sorted(sections, key=lambda s: metrics[s["id"]]["chars_with_spaces"], reverse=True)
    warnings = [{"section_id": s["id"], "note": "非編號分節篇幅異常長；先核對標題與內容邊界，不自動判為廣告或完整一章。"}
                for s in sections if s["heading"]["chapter"] is None
                and metrics[s["id"]]["chars_with_spaces"] > (6000 if s["heading"]["kind"] == "preamble" else 12000)]
    return {"counting_basis": "NFC Unicode 字元；含空白及標點、不含換行。eojeol 是空白分隔單位，不是 token。",
            "file_metrics": text_metrics(text),
            "body_metrics": {key: sum(m[key] for m in metrics.values()) for key in text_metrics("")},
            "preamble_body_chars": sum(metrics[s["id"]]["chars_with_spaces"] for s in sections if s["heading"]["kind"] == "preamble"),
            "numbered_section_distribution": length_distribution(metrics[s["id"]]["chars_with_spaces"] for s in sections if s["heading"]["chapter"] is not None),
            "all_section_distribution": length_distribution(m["chars_with_spaces"] for m in metrics.values()),
            "largest_sections": [{"section_id": s["id"], "source_heading": s["source_heading"],
                                  "kind": s["heading"]["kind"], "chars": metrics[s["id"]]["chars_with_spaces"]} for s in ranked[:5]],
            "structure_warnings": warnings, "sections": metrics}


def chunk_ranges(section, target_chars, max_chars):
    """保留內容區塊，在目標附近優先選場景或空行界線；超長單行只標記。"""
    blocks = section["blocks"]
    sizes = [content_chars(b["text"]) for b in blocks]
    prefix = [0]
    for size in sizes:
        prefix.append(prefix[-1] + size)
    if not blocks:
        return [(0, 0, 0, "section_end")]
    ranges, start = [], 0
    while start < len(blocks):
        if sizes[start] > max_chars:
            end, reason = start + 1, "oversized_block"
        elif prefix[-1] - prefix[start] <= max_chars:
            end, reason = len(blocks), "section_end"
        else:
            candidates = []
            for end in range(start + 1, len(blocks) + 1):
                count = prefix[end] - prefix[start]
                if count > max_chars:
                    break
                next_text = blocks[end]["text"].strip() if end < len(blocks) else ""
                is_scene = bool(re.fullmatch(r"(?:\*\s*){3,}|(?:[◇◆◈○●※]\s*)+", next_text))
                is_paragraph = end < len(blocks) and blocks[end]["line"] - blocks[end - 1]["line"] > 1
                reason = "scene" if is_scene else "paragraph" if is_paragraph else "block"
                candidates.append((end, count, reason))
            near = [c for c in candidates if c[1] >= target_chars * 0.75
                    and prefix[-1] - prefix[c[0]] >= target_chars * 0.5]
            choices = near or candidates
            priority = {"scene": 0, "paragraph": 1, "block": 2}
            end, _, reason = min(choices, key=lambda c: (priority[c[2]], abs(c[1] - target_chars), c[0]))
        ranges.append((start, end, prefix[end] - prefix[start], reason))
        start = end
    return ranges


def group_delivery_batches(work_units, target_count):
    """按原文字元把連續工作單位分成約十個交付範圍，只在完整分節邊界切分。"""
    require(type(target_count) is int and target_count > 0, "交付批次目標數必須是正整數。")
    if not work_units:
        return []
    prefix = [0]
    for unit in work_units:
        prefix.append(prefix[-1] + unit["chars"])
    section_ends = {index for index in range(1, len(work_units))
                    if work_units[index - 1]["section_id"] != work_units[index]["section_id"]}
    count = min(target_count, len(section_ends) + 1)
    weights = [0.5] + [1] * (count - 1) if count > 1 else [1]
    cuts = [0]
    for index in range(1, count):
        future_boundaries = count - index - 1
        choices = [cut for cut in section_ends if cut > cuts[-1]
                   and sum(later > cut for later in section_ends) >= future_boundaries]
        target = prefix[-1] * sum(weights[:index]) / sum(weights)
        cuts.append(min(choices, key=lambda cut: (abs(prefix[cut] - target), cut)))
    cuts.append(len(work_units))
    result = []
    for start, end in zip(cuts, cuts[1:]):
        group = work_units[start:end]
        result.append({"id": f"d{len(result) + 1:04d}",
                       "work_unit_ids": [unit["id"] for unit in group],
                       "first_section_id": group[0]["section_id"],
                       "last_section_id": group[-1]["section_id"],
                       "section_count": len({unit["section_id"] for unit in group}),
                       "chars": prefix[end] - prefix[start],
                       "blocked": any(unit["blocked"] for unit in group)})
    return result


def build_batch_plan(sections, source_sha256, target_chars=4000, max_chars=6000, batch_chars=12000, max_chunks=3, delivery_target_count=10):
    """建立十個左右交付批次，保留小段與工作單位作校對及中斷恢復。"""
    require(all(type(n) is int and n > 0 for n in (target_chars, max_chars, batch_chars, max_chunks))
            and target_chars <= max_chars <= batch_chars, "字數需為正整數且 target <= max <= batch；max-chunks 需為正整數。")
    chunks, batches = [], []
    pilot_pending = True

    def append_batch(group, kind):
        batches.append({"id": f"b{len(batches) + 1:05d}", "kind": kind,
                        "section_id": group[0]["section_id"], "chunk_ids": [c["id"] for c in group],
                        "chars": sum(c["chars"] for c in group),
                        "blocked": any(c["requires_manual_split"] for c in group)})

    for section in sections:
        group = []
        kind = "preamble_review" if section["heading"]["kind"] == "preamble" else "translation"
        for start, end, count, reason in chunk_ranges(section, target_chars, max_chars):
            blocks = section["blocks"][start:end]
            chunk = {"id": f"c{len(chunks) + 1:06d}", "section_id": section["id"],
                     "first_source_id": blocks[0]["id"] if blocks else None,
                     "last_source_id": blocks[-1]["id"] if blocks else None,
                     "start_line": blocks[0]["line"] if blocks else section["start_line"],
                     "end_line": blocks[-1]["line"] if blocks else section["start_line"],
                     "block_count": len(blocks), "chars": count,
                     "source_sha256": digest(canonical(blocks)), "cut_after": reason,
                     "requires_manual_split": count > max_chars}
            chunks.append(chunk)
            if pilot_pending and kind == "translation" and blocks:
                append_batch([chunk], "pilot")
                pilot_pending = False
                continue
            if group and (len(group) >= max_chunks or sum(c["chars"] for c in group) + count > batch_chars
                          or chunk["requires_manual_split"] or group[-1]["requires_manual_split"]):
                append_batch(group, kind)
                group = []
            group.append(chunk)
        if group:
            append_batch(group, kind)
    delivery_batches = group_delivery_batches(batches, delivery_target_count)
    return {"schema_version": 1, "source_sha256": source_sha256,
            "policy": {"unit": "NFC chars_with_spaces excluding line breaks; body blocks only",
                       "target_chars": target_chars, "max_chars": max_chars, "batch_chars": batch_chars,
                       "max_chunks": max_chunks, "max_sections_per_batch": 1, "pilot_chunks": 1,
                       "delivery_target_count": delivery_target_count, "first_delivery_weight": 0.5},
            "summary": {"delivery_batches": len(delivery_batches), "sections": len(sections),
                        "chunks": len(chunks), "batches": len(batches),
                        "body_chars": sum(c["chars"] for c in chunks),
                        "oversized_chunks": sum(c["requires_manual_split"] for c in chunks)},
            "note": "delivery_batches 為全書交付範圍；batches 為可中斷接續的內部工作單位。切點須核對，尚未翻譯或驗收。",
            "chunks": chunks, "batches": batches, "delivery_batches": delivery_batches}


def plan_inputs(args):
    source = Path(args.source).resolve()
    text, encoding, sha = read_text(source, args.source_encoding)
    sections = parse_source(text, args.heading_pattern)
    plan = build_batch_plan(sections, sha, args.target_chars, args.max_chars, args.batch_chars, args.max_chunks, args.delivery_batches)
    plan.update(source_path=str(source), source_encoding=encoding, heading_pattern=args.heading_pattern)
    if args.output:
        output = Path(args.output).resolve()
        require(not output.exists() and output != source, "計畫輸出檔已存在或是原稿；不覆寫，請指定新的明確檔名。")
        with output.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(plan, ensure_ascii=False, indent=2) + "\n")
        require(load_json(output) == plan, "批次計畫讀回不一致。")
        return {"output": str(output), "sha256": digest(output.read_bytes()), "summary": plan["summary"]}
    return plan


def show_section(section, first=None, last=None):
    """以既有區塊 ID 限定閱讀範圍，保留原章節索引與原文指紋。"""
    if first is None and last is None:
        return section
    ids = [b["id"] for b in section["blocks"]]
    require(first in ids and last in ids and ids.index(first) <= ids.index(last), "需指定同節內順序正確的 from-block 與 through-block。")
    return {**section, "blocks": section["blocks"][ids.index(first):ids.index(last) + 1],
            "format_lines": [row for row in section["format_lines"]
                             if section["blocks"][ids.index(first)]["line"] <= row["line"] <= section["blocks"][ids.index(last)]["line"]],
            "partial": True, "note": "這是分節的局部閱讀範圍；record 仍要求整節完整覆蓋。"}


def inspect_inputs(args):
    text, encoding, sha = read_text(Path(args.source), args.source_encoding)
    sections = parse_source(text, args.heading_pattern)
    keys = [canonical(s["heading"]) for s in sections]
    result = {"source_encoding": encoding, "source_sha256": sha, "characters": len(text), "lines": len(text.splitlines()), "sections_including_preamble": len(sections), "numbered_sections": sum(s["heading"]["chapter"] is not None for s in sections), "has_preamble": sections[0]["heading"]["kind"] == "preamble", "repeated_source_headings": len(keys) - len(set(keys)), "first": sections[0]["source_heading"], "last": sections[-1]["source_heading"], "note": "解析結果仍須核對原稿，未判定官方全集完整性或異常編號正誤。"}
    inventory = source_inventory(text, sections)
    result["inventory"] = {k: v for k, v in inventory.items() if k != "sections"}
    if args.reference:
        _, ref_encoding, ref_sha = read_text(Path(args.reference), args.reference_encoding)
        result.update(reference_encoding=ref_encoding, reference_sha256=ref_sha)
    if args.details:
        result["sections"] = [{**{k: s[k] for k in ("id", "heading", "source_heading", "start_line", "end_line")},
                               "metrics": inventory["sections"][s["id"]], "block_count": len(s["blocks"])} for s in sections]
    return result


def init_project(args):
    source, reference, work = Path(args.source).resolve(), Path(args.reference).resolve(), Path(args.work).resolve()
    require(source != reference, "韓文原稿與人工版必須是不同檔案。")
    require(not work.exists(), "工作目錄已存在；請使用 next／validate 續作，或指定新的工作目錄。")
    text, source_encoding, source_sha = read_text(source, args.source_encoding)
    _, reference_encoding, reference_sha = read_text(reference, args.reference_encoding)
    sections = parse_source(text, args.heading_pattern)
    manifest = {"schema_version": 1, "title": args.title, "source": {"path": str(source), "encoding": source_encoding, "sha256": source_sha}, "reference": {"path": str(reference), "encoding": reference_encoding, "sha256": reference_sha}, "heading_pattern": args.heading_pattern, "sections": sections}
    work.mkdir(parents=True)
    (work / "sections").mkdir()
    if getattr(args, "source_layout", "packs") == "packs":
        from source_store import build, SourceStore
        manifest, payloads = build(sys.modules[__name__], manifest)
        SourceStore(sys.modules[__name__], work, manifest).publish(payloads)
    atomic_json(work / "manifest.json", manifest)
    for name in ("terminology.jsonl", "continuity.jsonl", "reviews.jsonl"):
        (work / name).touch(exist_ok=False)
    return {"work": str(work), "sections": len(sections), "next": sections[0]["id"], "schema_version": manifest["schema_version"],
            "source_packs": len(manifest.get("source_storage", {}).get("packs", []))}


def load_project(work, *, verification=None):
    work = Path(work).resolve()
    manifest = load_json(work / "manifest.json")
    require(manifest.get("schema_version") in (1, 2), "不支援的 manifest 版本。")
    for label in ("source", "reference"):
        item = manifest[label]
        current_sha = file_digest(Path(item["path"]))
        if current_sha != item["sha256"]:
            raise PipelineError(f"{label} 檔案已改變；原驗收失效，請以新工作目錄重新索引並核對可沿用內容。",
                                details={"requires_recheck": True, "changes": [{"kind": label, "path": item["path"],
                                         "scope": "whole_input", "before": item["sha256"], "after": current_sha}]})
        if verification:
            verification.input_shas[label] = current_sha
    index_sha = None
    if verification:
        from verification_cache import index_fingerprint
        index_sha = index_fingerprint(sys.modules[__name__], manifest)
        verification.index_sha = index_sha
    store = None
    if manifest["schema_version"] == 2:
        from source_store import SourceStore
        store = SourceStore(sys.modules[__name__], work, manifest)
    if not verification or not verification.get("source_index", index_sha):
        if store:
            store.verify_origin()
            store.verify_packs()
        else:
            actual_text, _, _ = read_text(Path(manifest["source"]["path"]), manifest["source"]["encoding"])
            actual_sections = parse_source(actual_text, manifest["heading_pattern"])
            indexed = [{k: v for k, v in s.items() if k != "receipt"} for s in manifest["sections"]]
            require(actual_sections == indexed, "manifest 的原稿索引與輸入不符，不能省略、增添或重排來源分節。")
        if verification:
            verification.remember("source_index", index_sha)
    actual_sections = manifest["sections"]
    fingerprints = verification.input_shas if verification else None
    terms = load_rows(work / "terminology.jsonl", fingerprints=fingerprints)
    continuity = load_rows(work / "continuity.jsonl", fingerprints=fingerprints)
    issues = load_rows(work / "reviews.jsonl", fingerprints=fingerprints)
    for name, rows, key in (("專名", terms, "id"), ("敘事狀態", continuity, "section_id"), ("問題", issues, "id")):
        ids = [r.get(key) for r in rows]
        require(all(isinstance(i, str) and i for i in ids) and len(ids) == len(set(ids)), f"{name}識別碼缺漏或重複。")
    section_ids = {s["id"] for s in actual_sections}
    for term in terms:
        require(isinstance(term.get("ko"), list) and term["ko"] and all(isinstance(k, str) and k for k in term["ko"]), f"{term['id']} 缺少韓文詞形。")
        require(isinstance(term.get("zh"), str) and term["zh"] and term.get("basis") and term.get("evidence"), f"{term['id']} 缺少譯名或證據。")
        require(term.get("decision") in ("adopted", "pending"), f"{term['id']} 的 decision 無效。")
        if term.get("category") in ("plant", "animal", "creature") or "origin" in term:
            require(term.get("origin") in ("real_world", "author_created", "uncertain"), f"{term['id']} 需標示物種來源判定，不可預設套用現實物種。")
        scope = term.get("scope", {})
        require(isinstance(scope.get("from", 0), int) and isinstance(scope.get("through", len(actual_sections)), int) and 0 <= scope.get("from", 0) <= scope.get("through", len(actual_sections)), f"{term['id']} 的 scope 無效。")
        require(isinstance(term.get("forbidden_zh", []), list) and all(isinstance(t, str) and t for t in term.get("forbidden_zh", [])), f"{term['id']} 的禁用譯名無效。")
    require(all(c["section_id"] in section_ids and c.get("summary") and isinstance(c.get("facts"), list) for c in continuity), "敘事狀態需使用存在的分節、摘要及 facts 清單。")
    require(all(r.get("status") in ("open", "resolved") and isinstance(r.get("section_ids"), list) and set(r["section_ids"]).issubset(section_ids) and r.get("note") for r in issues), "問題紀錄的狀態、分節或說明無效。")
    return Project((work, manifest, terms, continuity, issues), source_store=store)


def section_by_id(manifest, identity):
    section = next((s for s in manifest["sections"] if s["id"] == identity), None)
    require(section is not None, f"不存在的分節：{identity}")
    return section


def artifact_path(work, identity, suffix):
    path = (work / "sections" / f"{identity}{suffix}").resolve()
    require(path.is_relative_to(work), "分節檔案不得指向工作目錄外。")
    return path


def passed_reviews(reviews, stages):
    return isinstance(reviews, dict) and all(isinstance(reviews.get(key), dict) and reviews[key].get("status") == "passed" and isinstance(reviews[key].get("note"), str) and reviews[key]["note"].strip() for key in stages)


def check_scorecard(card, valid_evidence, excluded_preamble=False):
    """驗證十項評分與算術門檻；分數本身仍是有依據的代理自評。"""
    require(isinstance(card, dict), "缺少十項評分表。")
    if excluded_preamble:
        require(card.get("method") == "not_applicable" and card.get("reason"), "排除的前置非故事資料需說明評分不適用，不得填入虛構高分。")
        return {"method": "not_applicable", "average": None}
    require(card.get("method") == "agent_self_assessment", "評分必須標示為代理自評，不能冒稱獨立或人工評分。")
    items = card.get("items")
    require(isinstance(items, dict) and set(items) == set(SCORE_DIMENSIONS), "十項評分必須完整且各佔相同權重。")
    values = []
    for key, label in SCORE_DIMENSIONS.items():
        item = items[key]
        require(isinstance(item, dict) and type(item.get("score")) in (int, float), f"{label} 缺少有效數值分數。")
        value = Decimal(str(item["score"]))
        require(value.is_finite() and 0 <= value <= 100, f"{label} 分數必須介於 0 至 100。")
        require(isinstance(item.get("note"), str) and item["note"].strip(), f"{label} 缺少評分依據。")
        evidence = item.get("evidence")
        require(isinstance(evidence, list) and evidence and all(isinstance(e, str) and e in valid_evidence for e in evidence), f"{label} 缺少有效原文區塊或分節證據。")
        values.append(value)
    average = sum(values) / Decimal(len(SCORE_DIMENSIONS))
    require(average >= MINIMUM_AVERAGE, f"十項平均 {average} 分，未達 85 分，請修訂後重新評分；不先四捨五入。")
    return {"method": "agent_self_assessment", "average": float(average), "minimum_average": int(MINIMUM_AVERAGE)}


def check_taiwan_text(text, mapping, source_ids, rules=None):
    """抓取已列出的簡體字與地域用語，語境仍由代理核對。"""
    if rules is None:
        rules = load_json(Path(__file__).resolve().parent.parent / "references" / "zh-tw-review-rules.json")
    lines = text.splitlines()
    for allowance in mapping.get("language_allowances", []):
        span = allowance.get("target_lines")
        evidence = allowance.get("source_ids")
        require(isinstance(span, list) and len(span) == 2 and all(type(n) is int for n in span) and 1 <= span[0] <= span[1] <= len(lines), "用語保留例外需有精確譯文行號。")
        require(isinstance(evidence, list) and evidence and all(isinstance(e, str) and e in source_ids for e in evidence), "用語保留例外需有原文證據。")
        require(isinstance(allowance.get("text"), str) and allowance["text"] and allowance.get("reason"), "用語保留例外需有精確文字與理由。")
        fragment = "\n".join(lines[span[0] - 1:span[1]])
        require(allowance["text"] in fragment and "\n" not in allowance["text"], "用語保留例外未出現在指定範圍，或跨越多行。")
        for i in range(span[0] - 1, span[1]):
            lines[i] = lines[i].replace(allowance["text"], "")
    remaining = "\n".join(lines)
    unambiguous = set(rules["simplified_only"]) - set(rules["ambiguous_characters_not_automatically_blocked"])
    simplified = sorted(set(remaining) & unambiguous)
    regional = [word for word in rules["mainland_terms"] if word in remaining]
    require(not simplified, "發現需處理的簡體字：" + "、".join(simplified))
    require(not regional, "發現需核對並調整的中國慣用語：" + "、".join(regional))


def available_terms(project, section):
    """適用範圍只需目錄，不讀取韓文正文。"""
    _, manifest, terms, _, _ = project
    return {t["id"]: t for t in terms if t.get("scope", {}).get("from", 0) <= section["ordinal"] <= t.get("scope", {}).get("through", len(manifest["sections"]))}


def term_context(project, section):
    """以相同規則找出專名適用範圍與新命中詞，供驗收及快取失效判斷。"""
    section = load_section(project, section)
    body = (section["source_heading"] or "") + "\n" + "\n".join(b["text"] for b in section["blocks"])
    available = available_terms(project, section)
    detected = {key for key, term in available.items() if any(form in body for form in term["ko"])}
    return available, detected


def check_section(project, section, *, artifact=None, chunk=False):
    """共用內容檢查；小段不冒用整節標題或敘事收據。"""
    work, manifest, terms, continuity, issues = project
    identity = section["id"]
    if artifact is None:
        text_path = artifact_path(work, identity, ".txt")
        map_path = artifact_path(work, identity, ".map.json")
        require(text_path.is_file() and map_path.is_file(), "缺少分節譯文或逐段對應檔。")
        text, _, text_sha = read_text(text_path, "utf-8")
        mapping = load_json(map_path)
        mapping_sha = digest(map_path.read_bytes())
    else:
        text, mapping = artifact
        text_sha, mapping_sha = digest(text), digest(json_text(mapping))
        require("\x00" not in text and "\ufffd" not in text, "譯文含 NUL 或替代字元。")
    section = load_section(project, section)
    require(mapping.get("schema_version") == 1 and mapping.get("section_id") == identity, "對應檔版本或分節 ID 不符。")
    require(mapping.get("source_sha256") == section["source_sha256"] and mapping.get("translation_sha256") == text_sha, "原文或譯文已變更，請重新校對並更新對應檔指紋。")
    require(passed_reviews(mapping.get("reviews"), STAGES if chunk else STAGES + ("heading",)), "尚未記錄標題、語意、主詞指代、流暢度、台灣用語及潤色後回查的實際結果。")
    lines = text.splitlines()
    title = mapping.get("title_zh")
    nonempty = [i + 1 for i, line in enumerate(lines) if line.strip()]
    if chunk:
        body = set(nonempty)
    elif title is None:
        require(section["heading"]["kind"] == "preamble" and not nonempty, "只有全數排除的來源前置文字可不輸出標題與正文。")
        body = set()
    else:
        require(isinstance(title, str) and title.strip() and nonempty and lines[nonempty[0] - 1] == title, "譯文第一個非空行必須等於已核對的 title_zh。")
        heading = section["heading"]
        if heading["chapter"] is not None:
            prefix = f"第{int(heading['chapter'])}章 "
            suffix = f"（{heading['part']}）" if heading["part"] else ""
            require(title.startswith(prefix) and (not suffix or title.endswith(suffix)) and len(title) > len(prefix + suffix), "中文章號、分節號或標題不符合原稿。")
        body = set(nonempty[1:])
        require(not any(re.match(r"^第\d+章 .+", lines[i - 1]) or lines[i - 1] == title for i in body), "正文內出現額外章節標頭，請核對重複或串節。")
    expected = {block["id"] for block in section["blocks"]}
    covered = []
    target_covered = []
    entries = mapping.get("alignments")
    require(isinstance(entries, list), "alignments 必須是清單。")
    for item in entries:
        ids = item.get("source_ids")
        require(isinstance(ids, list) and ids and all(isinstance(i, str) and i in expected for i in ids), "逐段對應含未知或空白來源 ID。")
        covered.extend(ids)
        if item.get("disposition") == "excluded":
            require(item.get("reason") in EXCLUSIONS and item.get("note") and "target_lines" not in item, "排除內容必須是可證明的非故事資料，並填寫原因與說明。")
        else:
            require(item.get("disposition") == "translated", "未知的逐段處理方式。")
            span = item.get("target_lines")
            require(isinstance(span, list) and len(span) == 2 and all(type(n) is int for n in span) and 1 <= span[0] <= span[1] <= len(lines), "譯文範圍需為有效的 1-based 起訖行號。")
            occupied = {i for i in range(span[0], span[1] + 1) if lines[i - 1].strip()}
            require(occupied and occupied.issubset(body), "對應不得包含標題或只有空白行。")
            target_covered.extend(sorted(occupied))
    require(len(covered) == len(set(covered)) and set(covered) == expected, "原文區塊缺漏或重複對應。")
    require(len(target_covered) == len(set(target_covered)) and set(target_covered) == body, "譯文有未對應內容或重複範圍；合併翻譯請放在同一個 mapping entry。")
    excluded_preamble = (section["heading"]["kind"] == "preamble" and not body)
    score = None if chunk else check_scorecard(mapping.get("scorecard"), expected | {identity}, excluded_preamble=excluded_preamble)
    if "style_profile_sha256" in mapping:
        progress = load_json(work / "batch-progress.json") if (work / "batch-progress.json").exists() else {}
        require(mapping["style_profile_sha256"] == digest(canonical(progress.get("style_profile", {}))), "文體設定已變更，請重新核對受影響譯稿。")
    rules_raw = (Path(__file__).resolve().parent.parent / "references" / "zh-tw-review-rules.json").read_bytes()
    check_taiwan_text(text, mapping, expected, json.loads(rules_raw.decode("utf-8")))
    remaining = text
    for allowance in mapping.get("allowed_hangul", []):
        require(allowance.get("text") and allowance.get("reason"), "保留韓文字樣需提供精確文字及理由。")
        remaining = remaining.replace(allowance["text"], "")
    require(not HANGUL.search(remaining), "譯文含未解釋的韓文殘留。")
    require(not PLACEHOLDER.search(text), "譯文含待處理的占位文字。")
    available, detected = term_context(project, section)
    declared = mapping.get("term_ids")
    require(isinstance(declared, list) and all(isinstance(t, str) for t in declared), "需明列本節 term_ids，無專名時填空清單。")
    require(len(declared) == len(set(declared)) and set(declared).issubset(available) and detected.issubset(declared), "本節專名依賴缺漏、重複或超出適用範圍。")
    dependencies = {key: digest(canonical(available[key])) for key in sorted(declared)}
    for key in declared:
        term = available[key]
        require(term["decision"] == "adopted", f"{key} 的譯名尚未決定。")
        require(not any(bad in text for bad in term.get("forbidden_zh", [])), f"{key} 出現本節禁用的譯名。")
    ordinal = {s["id"]: s["ordinal"] for s in manifest["sections"]}
    relevant_context = sorted((c for c in continuity if ordinal[c["section_id"]] <= section["ordinal"]), key=lambda c: ordinal[c["section_id"]])
    require(chunk or any(c["section_id"] == identity for c in relevant_context), "缺少本節敘事狀態；全數排除的前置文字也需說明。")
    require(not any(r["status"] == "open" and (not r["section_ids"] or identity in r["section_ids"]) for r in issues), "本節或全書仍有未解決的校對問題。")
    return {"translation_sha256": text_sha, "mapping_sha256": mapping_sha, "source_sha256": section["source_sha256"], "term_dependencies": dependencies, "continuity_sha256": digest(canonical(relevant_context)), **({"score": score} if not chunk else {}), "language_rules_sha256": digest(rules_raw)}


def validate_project(project, *, verification=None, stop_at_first=False, style=None):
    _, manifest, _, _, _ = project
    verified, errors, titles = [], [], {}
    for section in manifest["sections"]:
        try:
            receipt = (verification.checked_section(project, section, style or {})
                       if verification else check_section(project, section))
            require(section.get("receipt") == receipt, "尚未 record，或上次驗收後譯文、對應、專名或敘事狀態已改變。")
            mapping = load_json(artifact_path(project[0], section["id"], ".map.json"))
            heading = section["heading"]
            if heading["chapter"] is not None:
                key = (heading["chapter"], heading["title"])
                title = mapping["title_zh"].removeprefix(f"第{int(heading['chapter'])}章 ")
                if heading["part"]:
                    title = title.removesuffix(f"（{heading['part']}）")
                require(key not in titles or titles[key] == title, "同一原稿章名在不同分節中譯名不一致。")
                titles[key] = title
            verified.append(section["id"])
        except (PipelineError, OSError, ValueError, KeyError, TypeError) as exc:
            errors.append({"section_id": section["id"], "error": str(exc)})
            if stop_at_first:
                break
    return {"total": len(manifest["sections"]), "verified": len(verified), "next": errors[0]["section_id"] if errors else None, "errors": errors, "semantic_review": "代理聲明，工具只檢查紀錄、指紋與機械條件，不獨立證明譯文正確。"}


def record_section(args):
    work = Path(args.work).resolve()
    with writer_lock(work):
        project = load_project(work)
        section = section_by_id(project[1], args.section)
        section["receipt"] = check_section(project, section)
        atomic_json(work / "manifest.json", project[1])
    return {"recorded": args.section, "kind": "agent_attested_and_mechanically_checked", "average_score": section["receipt"]["score"]["average"]}


def book_state(project):
    work, manifest, _, _, issues = project
    report = validate_project(project)
    require(not report["errors"], f"仍有 {len(report['errors'])} 個分節尚未有效驗收。")
    require(not any(r["status"] == "open" for r in issues), "全書仍有未解決的校對問題。")
    reading = {}
    if (work / "batch-plan.json").exists():
        import chunk_workflow
        reading["batch_reading_sha256"] = chunk_workflow.verify_book_reading(sys.modules[__name__], work)
    return {"manifest_sha256": digest((work / "manifest.json").read_bytes()), "terms_sha256": digest((work / "terminology.jsonl").read_bytes()), "continuity_sha256": digest((work / "continuity.jsonl").read_bytes()), "reviews_sha256": digest((work / "reviews.jsonl").read_bytes()), "receipts_sha256": digest(canonical([s["receipt"] for s in manifest["sections"]])), **reading}


def attest_book(args):
    work = Path(args.work).resolve()
    with writer_lock(work):
        project = load_project(work)
        state = book_state(project)
        review_path = work / "book-review.json"
        review = load_json(review_path)
        require(passed_reviews(review.get("reviews"), BOOK_STAGES), "尚未完成章節、專名、敘事連貫及全書閱讀複核。")
        require(review.get("state") == state, "全書複核的 state 不符；請核對目前內容後更新。")
        score = check_scorecard(review.get("scorecard"), {s["id"] for s in project[1]["sections"]})
        atomic_json(work / "book-receipt.json", {"state": state, "review_sha256": digest(review_path.read_bytes()), "average_score": score["average"]})
    return {"book_review_recorded": True, "kind": "agent_attestation", "average_score": score["average"]}


def assemble_book(args):
    work = Path(args.work).resolve()
    with writer_lock(work):
        project = load_project(work)
        state = book_state(project)
        receipt = load_json(work / "book-receipt.json")
        review = load_json(work / "book-review.json")
        score = check_scorecard(review.get("scorecard"), {s["id"] for s in project[1]["sections"]})
        require(receipt == {"state": state, "review_sha256": digest((work / "book-review.json").read_bytes()), "average_score": score["average"]}, "全書複核尚未記錄，或複核後資料已變更。")
        output = Path(args.output).resolve()
        inputs = {Path(project[1][key]["path"]).resolve() for key in ("source", "reference")}
        require(output not in inputs and not output.is_relative_to(work), "交付檔不得覆蓋輸入，或放進工作狀態目錄。")
        require(not output.exists(), "交付檔已存在；請指定新的明確檔名，工具不覆寫既有版本。")
        pieces = [artifact_path(work, s["id"], ".txt").read_text(encoding="utf-8").strip() for s in project[1]["sections"]]
        text = "\n\n\n".join(piece for piece in pieces if piece) + "\n"
        require(text.strip(), "沒有可交付的正文。")
        # 組裝前再查一次，避免編輯中的工作檔混入輸出。
        require(book_state(load_project(work)) == state, "組裝期間工作資料改變，請重試驗收。")
        with output.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(text)
        require(output.read_bytes() == text.encode("utf-8"), "組裝讀回不一致，請檢查交付檔。")
    return {"output": str(output), "sha256": digest(output.read_bytes()), "sections": len(pieces), "nonempty_sections": sum(bool(p) for p in pieces), "average_score": score["average"], "scope": "全部提供原稿，依代理校對聲明及機械檢查組裝"}


def cli(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    fingerprint = sub.add_parser("fingerprint")
    fingerprint.add_argument("--path", required=True)
    for name in ("inspect", "init", "plan"):
        p = sub.add_parser(name)
        p.add_argument("--source", required=True)
        if name != "plan":
            p.add_argument("--reference", required=name == "init")
        p.add_argument("--source-encoding", default="auto")
        if name != "plan":
            p.add_argument("--reference-encoding", default="auto")
        p.add_argument("--heading-pattern", help="fullmatch 韓文標頭 regex，可用 chapter/title/part/kind 命名群組")
        if name == "init":
            p.add_argument("--work", required=True)
            p.add_argument("--title", required=True)
            p.add_argument("--source-layout", choices=("packs", "inline"), default="packs", help="預設約 2 MiB 來源包；inline 僅供舊格式相容")
        elif name == "inspect":
            p.add_argument("--details", action="store_true")
        else:
            p.add_argument("--target-chars", type=int, default=4000, help="每翻譯小段的目標字元數，含空白、不含換行")
            p.add_argument("--max-chars", type=int, default=6000, help="小段上限；超長原文單行會標記阻塞")
            p.add_argument("--batch-chars", type=int, default=12000, help="單次工作批次的字元上限")
            p.add_argument("--max-chunks", type=int, default=3, help="單次批次最多的小段數")
            p.add_argument("--delivery-batches", type=int, default=10, help="全書交付批次目標數；內部工作單位不計入")
            p.add_argument("--output", help="省略時唯讀預覽；指定時只建立尚不存在的計畫 JSON")
    for name in ("show", "next", "validate", "record", "book-state", "attest-book", "assemble", "save-chunk", "submit-chunk", "merge-section", "review-state", "review-section", "prepare-section", "term-details", "reference-search", "reference-extract", "migrate-source", "restore-inline", "rebuild-source"):
        p = sub.add_parser(name)
        p.add_argument("--work", required=True)
        if name in ("migrate-source", "restore-inline"):
            p.add_argument("--apply", action="store_true", help="省略時只預覽；指定後才切換索引格式")
            p.add_argument("--checkpoint", action="store_true", help="確認翻譯任務已保存且停止寫入後使用")
        if name in ("show", "record", "merge-section", "review-state", "review-section", "prepare-section", "term-details"):
            p.add_argument("--section", required=True)
        if name in ("save-chunk", "submit-chunk"):
            p.add_argument("--chunk", required=True)
        if name in ("save-chunk", "submit-chunk", "review-section"):
            p.add_argument("--input", required=True, help="含真實翻譯、校對或複核聲明的 UTF-8 JSON；- 代表標準輸入")
        if name in ("save-chunk", "submit-chunk", "merge-section"):
            p.add_argument("--replace", action="store_true", help="明確重建仍符合原產物指紋的檔案；不覆蓋外部修訂")
        if name in ("merge-section", "submit-chunk"):
            p.add_argument("--title", help="已核對的分節標題；全數排除的前置文字可省略")
        if name == "submit-chunk":
            p.add_argument("--merge", action="store_true", help="本節小段齊備時接續合併，仍需實際完整閱讀與評分")
        if name == "prepare-section":
            p.add_argument("--term-id", action="append", default=[], help="補入隱含人物或專名，可重複指定")
            p.add_argument("--term-detail", choices=("compact", "full"), default="compact", help="預設精簡既用已核定詞條；full 顯示全部完整證據")
        if name == "reference-search":
            p.add_argument("--query", action="append", required=True, help="批次字面查詢，可重複指定；不執行正規表示式")
            p.add_argument("--limit", type=int, default=20, help="每個查詢最多回傳命中行數")
            p.add_argument("--after-line", type=int, default=0, help="從此行之後繼續搜尋")
            p.add_argument("--before", type=int, default=2, help="命中位置前方上下文行數")
            p.add_argument("--after", type=int, default=2, help="命中位置後方上下文行數")
        if name == "reference-extract":
            p.add_argument("--range", action="append", required=True, help="1-based 含端點行號，例如 12:25，可重複指定")
            p.add_argument("--reference-state", required=True, help="reference-search 回傳的實際位置版本")
        if name == "term-details":
            p.add_argument("--term-id", action="append", required=True, help="取得本節指定詞條的完整依據，可重複指定")
            p.add_argument("--context-state", required=True, help="實際閱讀資料包的 context_state，拒絕混用版本")
        if name in ("next", "review-section"):
            p.add_argument("--prepare" if name == "next" else "--prepare-next", action="store_true",
                           help="直接取得最早缺口的分節資料；不代表已閱讀或翻譯")
            p.add_argument("--term-id", action="append", default=[], help="補入接續分節的隱含專名，可重複指定")
        if name in ("show", "prepare-section"):
            p.add_argument("--from-block", help="局部閱讀的第一個完整原文區塊 ID")
            p.add_argument("--through-block", help="局部閱讀的最後一個完整原文區塊 ID")
        if name == "assemble":
            p.add_argument("--output", required=True)
    import parallel_workflow
    parallel = sub.add_parser("parallel", help="舊平行草稿交接與恢復；不再開放新派工，日常使用單代理")
    parallel.add_argument("--work", required=True)
    parallel.add_argument("--actor", required=True)
    parallel.add_argument("--action", choices=parallel_workflow.ACTIONS, required=True)
    parallel.add_argument("--input", default="-", help="固定 JSON 輸入；- 代表標準輸入")
    for name in ("record", "save-chunk", "submit-chunk", "merge-section", "review-section", "attest-book", "assemble", "migrate-source", "restore-inline", "rebuild-source"):
        sub.choices[name].add_argument("--actor", help="僅舊平行工作尚未交接時指定原主代理；單代理不用填")
    args = parser.parse_args(argv)
    try:
        if args.command in ("record", "save-chunk", "submit-chunk", "merge-section", "review-section", "attest-book", "assemble", "migrate-source", "restore-inline", "rebuild-source"):
            parallel_workflow.require_main(sys.modules[__name__], args.work, args.actor)
        if args.command == "parallel":
            result = parallel_workflow.run(sys.modules[__name__], args)
        elif args.command == "fingerprint":
            result = {"path": str(Path(args.path).resolve()), "sha256": digest(Path(args.path).read_bytes())}
        elif args.command == "inspect":
            result = inspect_inputs(args)
        elif args.command == "init":
            result = init_project(args)
        elif args.command == "plan":
            result = plan_inputs(args)
        elif args.command in ("migrate-source", "restore-inline", "rebuild-source"):
            import source_store
            result = source_store.rebuild(sys.modules[__name__], args) if args.command == "rebuild-source" else source_store.migrate(sys.modules[__name__], args)
        elif args.command == "record":
            result = record_section(args)
        elif args.command == "attest-book":
            result = attest_book(args)
        elif args.command == "assemble":
            result = assemble_book(args)
        elif args.command == "prepare-section":
            import section_context
            result = section_context.prepare_section(sys.modules[__name__], args)
        elif args.command in ("reference-search", "reference-extract"):
            import reference_store
            result = reference_store.run(sys.modules[__name__], args)
        elif args.command == "term-details":
            import section_context
            result = section_context.term_details(sys.modules[__name__], args)
        elif args.command == "next":
            import section_context
            result = section_context.next_section(sys.modules[__name__], args)
        elif args.command in ("save-chunk", "submit-chunk", "merge-section", "review-state", "review-section"):
            import chunk_workflow
            result = chunk_workflow.run(sys.modules[__name__], args)
        else:
            verification = None
            if args.command == "show":
                from verification_cache import VerificationCache
                verification = VerificationCache(sys.modules[__name__], args.work)
            project = load_project(args.work, verification=verification)
            if args.command == "show":
                result = show_section(load_section(project, args.section), args.from_block, args.through_block)
            elif args.command == "book-state":
                result = book_state(project)
            else:
                result = validate_project(project)
                if args.command == "next":
                    result["errors"] = result["errors"][:1]
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 1 if args.command == "validate" and result["errors"] else 0
    except (PipelineError, OSError, ValueError, KeyError, TypeError) as exc:
        print(json.dumps({"error": str(exc), **({"details": exc.details} if getattr(exc, "details", None) else {})}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(cli())
