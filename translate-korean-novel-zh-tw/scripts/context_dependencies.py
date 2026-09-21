"""按本節實際閱讀資料建立依賴與差異，不變更既有翻譯驗收收據。"""

from pathlib import Path
from types import SimpleNamespace


def select_terms(core, flow, section, extra_terms):
    available = core.available_terms(flow.project, section)
    missing = sorted(set(extra_terms) - set(available))
    if missing:
        raise core.PipelineError("補入的專名 ID 不存在或超出本節適用範圍。", details={"requires_recheck": True,
                                 "changes": [{"kind": "term_scope", "term_id": key, "source_ids": [section["id"]]} for key in missing]})
    body = (section["source_heading"] or "") + "\n" + "\n".join(b["text"] for b in section["blocks"])
    selected = set(extra_terms) | {key for key, row in available.items() if any(form in body for form in row["ko"])}
    while True:
        entities = {available[key]["entity_id"] for key in selected if available[key].get("entity_id")}
        forms = {form for key in selected for form in available[key]["ko"]}
        expanded = selected | {key for key, row in available.items() if row.get("entity_id") in entities or forms.intersection(row["ko"])}
        if expanded == selected:
            return [available[key] for key in sorted(selected)]
        selected = expanded


def current_flow(core, work):
    """操作結束前重讀依據；不為二次確認再解析整本原稿或重建整份工作流程。"""
    work = Path(work).resolve()
    manifest = core.load_json(work / "manifest.json")
    terms = core.load_rows(work / "terminology.jsonl")
    continuity = core.load_rows(work / "continuity.jsonl")
    issues = core.load_rows(work / "reviews.jsonl")
    store = None
    if manifest["schema_version"] == 2:
        from source_store import SourceStore
        store = SourceStore(core, work, manifest)
    project = core.Project((work, manifest, terms, continuity, issues), source_store=store)
    progress_path = work / "batch-progress.json"
    return SimpleNamespace(work=work, manifest=manifest, terms=terms, continuity=continuity, project=project,
                           progress=core.load_json(progress_path) if progress_path.exists() else {}, verification=None,
                           plan_sha=core.digest((work / "batch-plan.json").read_bytes()))


def rules(core, flow):
    from verification_cache import rule_fingerprints
    result = dict(flow.verification.rules if flow.verification else rule_fingerprints(core))
    # 維護與操作導覽不定義譯文語義；其他既有及未知參考檔採保守綁定。
    for path in ("references/maintenance.md", "references/resume-and-delivery.md"):
        result.pop(path, None)
    return result


def snapshot(core, work, identity, extra_terms, *, flow=None):
    flow = flow if flow else current_flow(core, work)
    manifest = flow.manifest
    core.require(manifest.get("schema_version") in (1, 2), "不支援的 manifest 版本。")
    inputs = {}
    for key in ("source", "reference"):
        item = manifest[key]
        inputs[key] = flow.verification.input_shas[key] if flow.verification else core.file_digest(Path(item["path"]))
        if inputs[key] != item["sha256"]:
            raise core.PipelineError(f"{key} 檔案已改變；資料包失效，請重新核對。", details={"requires_recheck": True,
                                     "changes": [{"kind": key, "path": item["path"], "scope": "whole_input"}]})
    inputs["batch-plan.json"] = flow.plan_sha
    if flow.progress:
        core.require(flow.progress.get("schema_version") == 1 and flow.progress.get("source_sha256") == inputs["source"]
                     and flow.progress.get("reference_sha256") == inputs["reference"] and flow.progress.get("plan_sha256") == flow.plan_sha,
                     "進度與來源或批次計畫指紋不符。")
    section = core.load_section(flow.project, identity)
    terms = select_terms(core, flow, section, extra_terms)
    previous = None
    if section["ordinal"]:
        prior_id = manifest["sections"][section["ordinal"] - 1]["id"]
        previous = next((row for row in flow.continuity if row["section_id"] == prior_id), None)
        if previous is None:
            raise core.PipelineError("缺少前一分節敘事狀態，需先補齊已核對的摘要。", details={"requires_recheck": True,
                                     "changes": [{"kind": "continuity", "path": "continuity.jsonl", "section_id": prior_id,
                                                  "source_ids": [identity], "change": "missing"}]})
    style = flow.progress.get("style_profile", {})
    core.require(isinstance(style, dict), "style_profile 必須是物件。")
    issues = [row for row in flow.project[4] if not row["section_ids"] or identity in row["section_ids"]]
    from verification_cache import index_fingerprint
    return {"schema_version": 2, "work": str(Path(work).resolve()), "section_id": identity, "extra_term_ids": sorted(set(extra_terms)),
            "inputs": inputs, "index_sha256": flow.verification.index_sha if flow.verification else index_fingerprint(core, manifest),
            "style_profile_sha256": core.digest(core.canonical(style)),
            "terms": {row["id"]: core.digest(core.canonical(row)) for row in terms},
            "continuity": {previous["section_id"]: core.digest(core.canonical(previous))} if previous else {},
            "issues": {row["id"]: core.digest(core.canonical(row)) for row in sorted(issues, key=lambda r: r["id"])},
            "rules": rules(core, flow)}


def change_report(core, old_packet, expected, *, project):
    """差異定位僅導引回查，從不補上語意通過或更新既有聲明。"""
    old = old_packet.get("dependencies", {}) if isinstance(old_packet, dict) else {}
    identity = expected["section_id"]
    changes = []
    if not isinstance(old, dict) or not old:
        return {"requires_recheck": False, "previous_state": None, "changes": []}
    if old.get("schema_version") != expected["schema_version"]:
        changes.append({"kind": "dependency_policy", "source_ids": [identity], "note": "依賴格式已更新，重新核對本節資料；不重設翻譯進度。"})
        return {"requires_recheck": True, "previous_state": old_packet.get("state"), "changes": changes}
    section = core.load_section(project, identity)
    old_payload = old_packet.get("payload")
    old_rows = old_payload.get("terminology") if isinstance(old_payload, dict) else None
    old_terms = {row["id"]: row for row in (old_rows if isinstance(old_rows, list) else []) if isinstance(row, dict) and isinstance(row.get("id"), str)}
    new_terms = {row["id"]: row for row in project[2]}
    for group in ("terms", "continuity", "issues", "rules", "inputs"):
        before, after = old.get(group, {}), expected[group]
        before = before if isinstance(before, dict) else {}
        for key in sorted(set(before) | set(after)):
            if before.get(key) == after.get(key):
                continue
            row = {"kind": group, "id": key, "change": "added" if key not in before else "removed" if key not in after else "modified"}
            if group == "terms":
                left, right = old_terms.get(key, {}), new_terms.get(key, {})
                forms = {form for term in (left, right) for form in (term.get("ko") if isinstance(term.get("ko"), list) else []) if isinstance(form, str) and form}
                row["source_ids"] = [b["id"] for b in section["blocks"] if any(form in b["text"] for form in forms)] or [identity]
                row["fields"] = [field for field in sorted(set(left) | set(right)) if left.get(field) != right.get(field)]
                row["term_id"] = key
            elif group == "rules":
                row.update(path=key, source_ids=[identity])
            elif group == "continuity":
                row.update(path="continuity.jsonl", section_id=key, source_ids=[identity])
            elif group == "issues":
                row.update(path="reviews.jsonl", issue_id=key, source_ids=[identity])
            else:
                row.update(source_ids=[identity], scope="input_or_plan")
            changes.append(row)
    for key in ("index_sha256", "style_profile_sha256", "extra_term_ids"):
        if old.get(key) != expected.get(key):
            changes.append({"kind": key, "source_ids": [identity]})
    return {"requires_recheck": bool(changes), "previous_state": old_packet.get("state"), "changes": changes}
