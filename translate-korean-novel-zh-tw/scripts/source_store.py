"""約 2 MiB 的來源包與精簡索引；實體合併、按分節讀取，原稿保持唯一來源。"""

from collections import OrderedDict
from copy import deepcopy
from pathlib import Path
import re


POLICY = {"target_bytes": 2 * 1024 * 1024, "max_bytes": 4 * 1024 * 1024, "max_sections": 100}
FORMAT = "section-jsonl-v1"
EXTRA_FIELDS = {"source_record", "block_count", "body_chars", "first_source_id", "last_source_id"}


def source_record(section):
    return {k: v for k, v in section.items() if k != "receipt"}


def metadata(section):
    return {k: v for k, v in section.items() if k not in EXTRA_FIELDS | {"receipt"}}


def build(core, manifest, policy=None):
    """依原順序組包；UTF-8 位元組偏移不混用字元位置，整節不截斷。"""
    policy = dict(POLICY if policy is None else policy)
    core.require(set(policy) == set(POLICY) and all(type(n) is int and n > 0 for n in policy.values())
                 and policy["target_bytes"] <= policy["max_bytes"], "來源包政策無效。")
    generation = core.digest(core.canonical({"format": FORMAT, "source": manifest["source"]["sha256"],
                             "encoding": manifest["source"]["encoding"], "heading_pattern": manifest["heading_pattern"], "policy": policy}))
    result = {**manifest, "schema_version": 2, "sections": []}
    payloads, packs, buffer, ids = {}, [], bytearray(), []

    def finish():
        if not ids:
            return
        name = f"part-{len(packs) + 1:04d}.jsonl"
        raw = bytes(buffer)
        payloads[name] = raw
        packs.append({"name": name, "sha256": core.digest(raw), "bytes": len(raw),
                      "first_section": ids[0], "last_section": ids[-1], "sections": len(ids),
                      "oversized_section": len(ids) == 1 and len(raw) > policy["max_bytes"]})
        buffer.clear()
        ids.clear()

    for section in manifest["sections"]:
        record = source_record(section)
        raw = (core.canonical(record) + "\n").encode("utf-8")
        if ids and (len(buffer) + len(raw) > policy["max_bytes"] or len(ids) >= policy["max_sections"]):
            finish()
        blocks = record["blocks"]
        entry = {k: v for k, v in section.items() if k not in ("blocks", "format_lines")}
        entry.update(block_count=len(blocks), body_chars=sum(core.content_chars(b["text"]) for b in blocks),
                     first_source_id=blocks[0]["id"] if blocks else None, last_source_id=blocks[-1]["id"] if blocks else None,
                     source_record={"pack": f"part-{len(packs) + 1:04d}.jsonl", "offset": len(buffer),
                                    "length": len(raw), "sha256": core.digest(raw)})
        result["sections"].append(entry)
        buffer.extend(raw)
        ids.append(section["id"])
        if len(buffer) >= policy["target_bytes"]:
            finish()
    finish()
    result["source_storage"] = {"format": FORMAT, "generation": generation, "policy": policy, "packs": packs}
    return result, payloads


class SourceStore:
    """一次操作最多保留兩個來源包、四個解碼分節，避免日常載入整本正文。"""

    def __init__(self, core, work, manifest, *, payloads=None):
        self.c, self.work, self.manifest = core, Path(work).resolve(), manifest
        storage = manifest["source_storage"]
        core.require(storage.get("format") == FORMAT and re.fullmatch(r"[0-9a-f]{64}", storage.get("generation", "")), "來源包格式或版本識別碼無效。")
        self.root = (self.work / "context" / "source" / storage["generation"]).resolve()
        core.require(self.root.is_relative_to(self.work), "來源包目錄不得指向工作目錄外。")
        self.packs = {p["name"]: p for p in storage["packs"]}
        core.require(len(self.packs) == len(storage["packs"]) and self.packs, "來源包清單空白或重複。")
        self.sections = {s["id"]: s for s in manifest["sections"]}
        core.require(len(self.sections) == len(manifest["sections"]), "來源分節 ID 重複。")
        self._packs, self._sections = OrderedDict(), OrderedDict()
        self.payloads = payloads
        self.decoded_sections = set()
        self.pack_reads = 0

    def path(self, name):
        self.c.require(name in self.packs and re.fullmatch(r"part-\d{4,}\.jsonl", name), "來源包檔名不合法。")
        path = (self.root / name).resolve()
        protected = {Path(self.manifest[k]["path"]).resolve() for k in ("source", "reference")}
        self.c.require(path.is_relative_to(self.root) and path not in protected, "來源包不得覆蓋輸入或指向目錄外。")
        return path

    def pack(self, name):
        if name not in self._packs:
            path = self.path(name)
            try:
                raw = self.payloads[name] if self.payloads is not None else path.read_bytes()
            except FileNotFoundError as exc:
                raise self.c.PipelineError("來源包缺失；請執行 rebuild-source 從原稿重建。") from exc
            expected = self.packs[name]
            self.c.require(len(raw) == expected["bytes"] and self.c.digest(raw) == expected["sha256"],
                           "來源包指紋不符；請執行 rebuild-source 從原稿重建，不修改索引迎合快取。")
            self._packs[name] = raw
            self.pack_reads += 1
            if len(self._packs) > 2:
                self._packs.popitem(last=False)
        self._packs.move_to_end(name)
        return self._packs[name]

    def section(self, identity):
        c = self.c
        c.require(identity in self.sections, "不存在的來源分節。")
        entry = self.sections[identity]
        if identity not in self._sections:
            ref = entry["source_record"]
            raw = self.pack(ref["pack"])
            start, length = ref["offset"], ref["length"]
            c.require(type(start) is int and type(length) is int and start >= 0 and length > 0 and start + length <= len(raw), "來源分節位元組範圍無效。")
            fragment = raw[start:start + length]
            c.require(fragment.endswith(b"\n") and c.digest(fragment) == ref["sha256"], "來源分節指紋或邊界不符。")
            record = c.json.loads(fragment.decode("utf-8"))
            c.require({k: v for k, v in record.items() if k not in ("blocks", "format_lines")} == metadata(entry), "來源分節與精簡索引不符。")
            c.require(len(record["blocks"]) == entry["block_count"], "來源區塊數不符。")
            self._sections[identity] = record
            self.decoded_sections.add(identity)
            if len(self._sections) > 4:
                self._sections.popitem(last=False)
        self._sections.move_to_end(identity)
        record = self._sections[identity]
        return {**record, **({"receipt": entry["receipt"]} if "receipt" in entry else {})}

    def verify_origin(self):
        """直接解析母檔，核對全部目錄、偏移與包指紋；不信任先前快取。"""
        c, m = self.c, self.manifest
        text, _, sha = c.read_text(Path(m["source"]["path"]), m["source"]["encoding"])
        c.require(sha == m["source"]["sha256"], "source 檔案已改變，請重新索引。")
        original = {**m, "sections": c.parse_source(text, m["heading_pattern"])}
        expected, payloads = build(c, original, m["source_storage"]["policy"])
        c.require(expected["source_storage"] == m["source_storage"] and expected["sections"] ==
                  [source_record(s) for s in m["sections"]], "manifest 的原稿索引與輸入不符，不能省略、增添或重排來源分節。")
        return payloads

    def verify_packs(self):
        for name in self.packs:
            self.pack(name)

    def publish(self, payloads):
        """只重建明確列出的衍生包；每檔原子寫入，不刪除任何目錄。"""
        for name, raw in payloads.items():
            path = self.path(name)
            if path.exists() and path.read_bytes() == raw:
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            self.c.atomic_text(path, raw.decode("utf-8"))
        self._packs.clear()
        self._sections.clear()
        self.payloads = None
        self.verify_packs()


def mutable_state(core, work):
    """遷移比對所有非衍生工作檔，避免鎖外編輯造成新舊進度混合。"""
    rows = []
    for path in sorted(work.rglob("*")):
        rel = path.relative_to(work)
        if not path.is_file() or rel.parts[0] in ("context", ".migration") or path.name == ".writer.lock" or path.name.startswith(".state-"):
            continue
        rows.append((rel.as_posix(), core.file_digest(path)))
    return core.digest(core.canonical(rows))


def ensure_not_delivered(core, work):
    progress = core.load_json(work / "batch-progress.json") if (work / "batch-progress.json").exists() else {}
    output = progress.get("final_output") or {}
    core.require(not (work / "book-receipt.json").exists() and progress.get("phase") != "delivered" and not output.get("sha256")
                 and not (output.get("path") and Path(output["path"]).exists()), "已有全書收據或交付檔，保留既有格式；需另行安排重新驗收，不直接遷移。")


def migrate(core, args):
    """預覽或在保存點切換；只改 manifest，譯稿、計畫與進度保持原始位元組。"""
    work = Path(args.work).resolve()

    def operation():
        ensure_not_delivered(core, work)
        before_stamp = mutable_state(core, work)
        project = core.load_project(work)
        manifest = project[1]
        target_schema = 1 if args.command == "restore-inline" else 2
        if manifest["schema_version"] == target_schema:
            return {"status": "already_current", "schema_version": target_schema}
        before = core.validate_project(project)
        from chunk_workflow import Workflow
        flow = Workflow(core, work, full=True) if (work / "batch-plan.json").exists() else None
        before_reading = flow.reading_records() if flow else {}
        if target_schema == 2:
            candidate, payloads = build(core, manifest)
            store = SourceStore(core, work, candidate, payloads=payloads)
        else:
            candidate = {k: deepcopy(v) for k, v in manifest.items() if k != "source_storage"}
            candidate.update(schema_version=1, sections=[project.source_store.section(s["id"]) for s in manifest["sections"]])
            store, payloads = None, None
        replacement = core.Project((work, candidate, *project[2:]), source_store=store)
        after = core.validate_project(replacement)
        core.require(before == after, "格式轉換改變驗收結果，未切換。")
        if flow:
            checked_flow = Workflow(core, work, full=True, project=replacement)
            core.require(checked_flow.reading_records() == before_reading, "格式轉換改變閱讀紀錄，未切換。")
        result = {"status": "preview", "from_schema": manifest["schema_version"], "to_schema": target_schema,
                  "sections": len(candidate["sections"]), "packs": len(candidate.get("source_storage", {}).get("packs", [])),
                  "manifest_bytes_before": (work / "manifest.json").stat().st_size,
                  "manifest_bytes_after": len(core.json_text(candidate).encode("utf-8")),
                  "verified_sections": before["verified"], "pending_or_invalid_sections": len(before["errors"]),
                  "next_section": before["next"], "valid_reading_batches": len(before_reading)}
        if not args.apply:
            return result
        core.require(getattr(args, "checkpoint", False), "請在翻譯任務停止寫入的保存點使用 --checkpoint；寫入鎖不代表其他編輯器已停止。")
        backup = (work / ".migration" / f"manifest-v{manifest['schema_version']}.json").resolve()
        core.require(backup.is_relative_to(work) and backup not in {Path(manifest[k]["path"]).resolve() for k in ("source", "reference")}, "遷移備份路徑不合法。")
        raw_manifest = (work / "manifest.json").read_bytes()
        core.require(not backup.exists() or backup.read_bytes() == raw_manifest, "已存在不同的遷移備份；保留它，先確認前次遷移狀態。")
        backup.parent.mkdir(parents=True, exist_ok=True)
        if not backup.exists():
            with backup.open("xb") as stream:
                stream.write(raw_manifest)
        if store:
            store.publish(payloads)
        for label in ("source", "reference"):
            core.require(core.file_digest(Path(manifest[label]["path"])) == manifest[label]["sha256"], "遷移期間輸入改變，未切換。")
        core.require(mutable_state(core, work) == before_stamp, "遷移期間工作檔改變，未切換。")
        core.atomic_json(work / "manifest.json", candidate)
        core.require(core.load_json(work / "manifest.json") == candidate, "遷移索引讀回不符。")
        result.update(status="migrated", backup=str(backup))
        return result

    if args.apply:
        with core.writer_lock(work):
            return operation()
    return operation()


def rebuild(core, args):
    work = Path(args.work).resolve()
    with core.writer_lock(work):
        manifest = core.load_json(work / "manifest.json")
        core.require(manifest.get("schema_version") == 2, "只有來源包格式需要 rebuild-source。")
        for label in ("source", "reference"):
            core.require(core.file_digest(Path(manifest[label]["path"])) == manifest[label]["sha256"], "輸入檔案已改變，不能沿用舊索引重建。")
        store = SourceStore(core, work, manifest)
        payloads = store.verify_origin()
        store.publish(payloads)
        return {"rebuilt": len(payloads), "manifest_unchanged": True}
