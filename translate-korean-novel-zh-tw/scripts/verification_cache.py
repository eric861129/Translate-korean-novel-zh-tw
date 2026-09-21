"""以內容指紋重用機械檢查；不產生閱讀聲明，交付驗證不使用此快取。"""

from pathlib import Path


def rule_fingerprints(core):
    skill = Path(core.__file__).resolve().parent.parent
    paths = [skill / "SKILL.md", *sorted((skill / "references").glob("*")),
             *sorted(p for p in (skill / "scripts").glob("*.py") if not p.name.startswith("test_"))]
    return {p.relative_to(skill).as_posix(): core.digest(p.read_bytes()) for p in paths if p.is_file()}


def index_fingerprint(core, manifest):
    """收據不影響來源索引；只建立淺層視圖，避免複製全書內容。"""
    index = {**manifest, "sections": [{k: v for k, v in s.items() if k != "receipt"} for s in manifest["sections"]]}
    return core.digest(core.canonical(index))


class VerificationCache:
    """可丟棄的正向驗證結果；損毀、缺檔或工具變更一律重新檢查。"""

    def __init__(self, core, work):
        self.c, self.work = core, Path(work).resolve()
        self.path = (self.work / "context" / "verification-cache.json").resolve()
        core.require(self.path.is_relative_to(self.work), "驗證快取不得指向工作目錄外。")
        self.rules = rule_fingerprints(core)
        self.input_shas = {}
        self.entries, self.dirty = {}, False
        try:
            packet = core.load_json(self.path)
            payload = packet["payload"]
            if (packet.get("sha256") == core.digest(core.canonical(payload)) and payload.get("schema_version") == 1
                    and payload.get("rules") == self.rules and isinstance(payload.get("entries"), dict)):
                self.entries = payload["entries"]
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            pass

    def get(self, key, state):
        row = self.entries.get(key)
        return row.get("value") if isinstance(row, dict) and row.get("state") == state else None

    def remember(self, key, state, value=True):
        row = {"state": state, "value": value}
        if self.entries.get(key) != row:
            self.entries[key], self.dirty = row, True

    def flush(self, manifest):
        if not self.dirty:
            return
        self.c.require(self.path not in {Path(manifest[k]["path"]).resolve() for k in ("source", "reference")}, "驗證快取不得覆蓋輸入檔。")
        payload = {"schema_version": 1, "rules": self.rules, "entries": self.entries}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.c.atomic_json(self.path, {"payload": payload, "sha256": self.c.digest(self.c.canonical(payload))})
        self.dirty = False

    def section_state(self, project, section, style, *, receipt=None):
        """逐次讀取產物位元組，包含新命中專名、隱含依賴及前序敘事。"""
        c, (work, manifest, terms, continuity, issues) = self.c, project
        raw_text = c.artifact_path(work, section["id"], ".txt").read_bytes()
        raw_map = c.artifact_path(work, section["id"], ".map.json").read_bytes()
        if receipt is not None:
            c.require(c.digest(raw_text) == receipt["translation_sha256"] and c.digest(raw_map) == receipt["mapping_sha256"],
                      "驗收後產物已改變，不能沿用本次檢查結果。")
        mapping = c.json.loads(raw_map.decode("utf-8"))
        available = c.available_terms(project, section)
        source_stamp = section.get("source_record", {}).get("sha256") or c.digest(c.canonical({k: v for k, v in section.items() if k != "receipt"}))
        match_stamp = c.digest(c.canonical([source_stamp, {key: term["ko"] for key, term in available.items()}]))
        detected = self.get("term-matches:" + section["id"], match_stamp)
        if detected is None:
            _, matches = c.term_context(project, section)
            detected = sorted(matches)
            self.remember("term-matches:" + section["id"], match_stamp, detected)
        declared = mapping.get("term_ids", [])
        c.require(isinstance(declared, list) and all(isinstance(t, str) for t in declared), "專名依賴格式不符。")
        selected = set(declared) | set(detected)
        ordinal = {s["id"]: s["ordinal"] for s in manifest["sections"]}
        state = {"section": section, "translation": c.digest(raw_text), "mapping": c.digest(raw_map),
                 "terms": {key: available.get(key) for key in sorted(selected)},
                 "continuity": [r for r in continuity if ordinal[r["section_id"]] <= section["ordinal"]],
                 "issues": [r for r in issues if not r["section_ids"] or section["id"] in r["section_ids"]],
                 "style": style}
        return c.digest(c.canonical(state))

    def checked_section(self, project, section, style):
        """只重用當前內容及依據均相同的有效收據；不以游標代替檢查。"""
        c = self.c
        stamp = self.section_state(project, section, style)
        key = "section:" + section["id"]
        receipt = self.get(key, stamp)
        if not receipt:
            receipt = c.check_section(project, section)
        c.require(section.get("receipt") == receipt, "尚未 record，或上次驗收後譯文、對應、專名或敘事狀態已改變。")
        self.remember(key, stamp, receipt)
        return receipt
