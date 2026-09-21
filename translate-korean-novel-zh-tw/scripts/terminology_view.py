"""詞條的閱讀視圖；完整詞庫與驗收依賴不受呈現長度影響。"""

from copy import deepcopy
import re


# 舊詞庫的自由文字也可能藏有使用限制；命中時保守保留完整證據，不推斷詞義。
CAUTION = re.compile(
    r"多義|歧義|衝突|同名|混淆|未決|待確認|待查|待核|不確定|語境|"
    r"注意|慎用|勿|禁止|不可|不得|除非|僅限|只限|只在|不宜|取決|"
    r"然而|但是|但|而非|不是|不等於|並非|區分|分辨|特定|場合|兩種|多種|"
    r"conflict|ambiguit|polysem|unresolved|uncertain|caution|context.dependent", re.I)
RISK_FIELDS = re.compile(r"attention|conflict|sense|ambigu|warning|caution|pending", re.I)
PRIORITY_FIELDS = ("id", "ko", "zh", "entity_id", "category", "decision", "scope", "definition", "voice_note", "forbidden_zh", "origin", "basis")
REASONS = {
    "new_or_changed": "前序分節收據未記錄相同版本，保留首次使用或變更後的完整依據。",
    "pending": "譯名尚未核定，不能視為可直接採用。",
    "shared_form": "同一韓文詞形有多份適用記錄，需核對本節身分或詞義。",
    "same_translation": "不同實體使用相同中文名稱，需辨別人物或事物。",
    "recorded_attention": "詞條明列需注意的用法、衝突或多義資料。",
    "text_caution": "既有文字含限制或歧義提示，完整保留供語境判讀。",
    "open_issue": "本節或全書仍有未解問題，保守保留本節詞條完整證據。",
}


def reading_view(core, flow, payload, detail="compact"):
    """只省略可按版本取回的 evidence；其他欄位、提醒與原文全部保留。"""
    core.require(detail in ("compact", "full"), "詞條閱讀模式必須為 compact 或 full。")
    terms = payload["terminology"]
    ordinal = core.section_by_id(flow.manifest, payload["section"]["id"])["ordinal"]
    prior_versions = set()
    for section in flow.manifest["sections"]:
        if section["ordinal"] < ordinal:
            # 此紀錄只識別歷史使用版本，不替代本次或歷史分節的有效驗收。
            prior_versions.update(section.get("receipt", {}).get("term_dependencies", {}).items())
    shared_forms, translations = {}, {}
    for term in terms:
        for form in term["ko"]:
            shared_forms.setdefault(form, set()).add(term["id"])
        translations.setdefault(term["zh"], []).append(term)
    attention, shown, voices = [], [], []
    has_open_issue = any(row["status"] == "open" for row in payload["issues"])
    for term in terms:
        fingerprint = core.digest(core.canonical(term))
        reasons, related = [], set()
        if (term["id"], fingerprint) not in prior_versions:
            reasons.append("new_or_changed")
        if term["decision"] != "adopted":
            reasons.append("pending")
        for form in term["ko"]:
            peers = shared_forms[form] - {term["id"]}
            if peers:
                related.update(peers)
        if related:
            reasons.append("shared_form")
        collisions = {row["id"] for row in translations[term["zh"]]
                      if row["id"] != term["id"] and row.get("entity_id", row["id"]) != term.get("entity_id", term["id"])}
        if collisions:
            reasons.append("same_translation")
            related.update(collisions)
        if any(RISK_FIELDS.search(key) and value for key, value in term.items()):
            reasons.append("recorded_attention")
        if CAUTION.search(core.canonical(term)):
            reasons.append("text_caution")
        if has_open_issue:
            reasons.append("open_issue")
        if reasons:
            attention.append({"term_id": term["id"], "reasons": reasons, "related_term_ids": sorted(related)})
        full = detail == "full" or bool(reasons)
        # context_state 已綁定本節使用的完整詞條，顯示端不重複輸出長指紋。
        reference = term["id"]
        if full:
            shown.append(deepcopy(term))
        else:
            row = {key: deepcopy(term[key]) for key in PRIORITY_FIELDS if key in term}
            row.update({key: deepcopy(value) for key, value in term.items() if key not in row and key != "evidence"})
            row.setdefault("scope", {"from": 0, "through": len(flow.manifest["sections"]) - 1})
            row["evidence_ref"] = reference
            shown.append(row)
        if term.get("voice_note"):
            voices.append({"term_id": term["id"], "voice_note": term["voice_note"],
                           **({"evidence": deepcopy(term["evidence"])} if full else {"evidence_ref": reference})})
    payload["terminology"] = shown
    payload["character_voices"] = voices
    payload["terminology_view"] = detail
    payload["term_attention"] = attention
    used_reasons = {reason for row in attention for reason in row["reasons"]}
    payload["term_attention_legend"] = {key: value for key, value in REASONS.items() if key in used_reasons}
    payload["note"] += " 精簡視圖只折疊既用已核定詞條的 evidence；完整依據用 term-details 配合本次 context_state 取得。新詞、衝突與多義提示不等於完整語義偵測，未登錄的新詞仍須實際讀原文辨認並登錄。"
    return payload
