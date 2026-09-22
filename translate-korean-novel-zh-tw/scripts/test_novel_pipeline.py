"""合成短文測試工具不變條件；不代表模型翻譯品質。測試資料留在獨立暫存目錄。"""

import argparse
from contextlib import redirect_stdout, redirect_stderr
from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
import unittest

import novel_pipeline as pipeline


SOURCE = """서장

나는 문을 열었다.
아무도 없었다.

<1화 약속 (1)>
목진이 말했다.
“내일 돌아오겠다.”
< 1화 약속 (1) > 끝

<1화 약속 (2)>
목진이 돌아왔다.
"""
TRANSLATIONS = {
    "나는 문을 열었다.": "我推開了門。",
    "아무도 없었다.": "裡面一個人也沒有。",
    "목진이 말했다.": "木真說道：",
    "“내일 돌아오겠다.”": "「我明天會回來。」",
    "목진이 돌아왔다.": "木真回來了。",
}


def write_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_rows(path, data):
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in data), encoding="utf-8")


def synthetic_reviews(stages):
    return {stage: {"status": "passed", "note": "合成測試模擬聲明，只驗證程式機制。"} for stage in stages}


def synthetic_scorecard(evidence, score=90):
    return {"method": "agent_self_assessment", "items": {key: {"score": score, "note": "合成測試的模擬評分，不代表實際翻譯品質。", "evidence": evidence} for key in pipeline.SCORE_DIMENSIONS}}


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.run_root = Path(tempfile.mkdtemp(prefix="novel-skill-tests-"))

    def setUp(self):
        self.root = self.run_root / self._testMethodName
        self.root.mkdir()
        self.source = self.root / "韓文原稿.txt"
        self.reference = self.root / "人工版.txt"
        self.source.write_text(SOURCE, encoding="utf-16")
        self.reference.write_text("木真說：明天回來。", encoding="utf-8")
        self.work = self.root / "translation-work"
        pipeline.init_project(argparse.Namespace(source=str(self.source), reference=str(self.reference), work=str(self.work), title="合成小說", source_encoding="auto", reference_encoding="auto", heading_pattern=None, source_layout="inline"))
        self.terms = [{"id": "name-001", "ko": ["목진"], "zh": "木真", "category": "person", "decision": "adopted", "basis": "human_reference", "evidence": [{"source": "reference", "line": 1}], "forbidden_zh": ["穆真"]}]
        write_rows(self.work / "terminology.jsonl", self.terms)
        manifest = pipeline.load_json(self.work / "manifest.json")
        self.contexts = [{"section_id": s["id"], "summary": "合成測試的敘事狀態。", "facts": []} for s in manifest["sections"]]
        write_rows(self.work / "continuity.jsonl", self.contexts)
        for section in manifest["sections"]:
            heading = section["heading"]
            title = "序章" if heading["chapter"] is None else f"第{heading['chapter']}章 約定（{heading['part']}）"
            lines, alignment = [title, ""], []
            for block in section["blocks"]:
                line_number = len(lines) + 1
                lines.extend([TRANSLATIONS[block["text"]], ""])
                alignment.append({"source_ids": [block["id"]], "disposition": "translated", "target_lines": [line_number, line_number]})
            path = self.work / "sections" / f"{section['id']}.txt"
            path.write_text("\n".join(lines), encoding="utf-8")
            mapping = {"schema_version": 1, "section_id": section["id"], "source_sha256": section["source_sha256"], "translation_sha256": pipeline.digest(path.read_bytes()), "title_zh": title, "alignments": alignment, "term_ids": [] if section["ordinal"] == 0 else ["name-001"], "reviews": synthetic_reviews(pipeline.STAGES + ("heading",)), "scorecard": synthetic_scorecard([section["id"]])}
            write_json(self.work / "sections" / f"{section['id']}.map.json", mapping)

    def record(self, identity):
        return pipeline.record_section(argparse.Namespace(work=str(self.work), section=identity))

    def record_all(self):
        for identity in ("s0000", "s0001", "s0002"):
            self.record(identity)

    def report(self):
        return pipeline.validate_project(pipeline.load_project(self.work))

    def map_path(self, identity="s0001"):
        return self.work / "sections" / f"{identity}.map.json"

    def change_map(self, change, identity="s0001"):
        mapping = pipeline.load_json(self.map_path(identity))
        change(mapping)
        write_json(self.map_path(identity), mapping)

    def update_translation(self, text, identity="s0001"):
        path = self.work / "sections" / f"{identity}.txt"
        path.write_text(text, encoding="utf-8")
        self.change_map(lambda m: m.update(translation_sha256=pipeline.digest(path.read_bytes())), identity)

    def attest(self):
        state = pipeline.book_state(pipeline.load_project(self.work))
        write_json(self.work / "book-review.json", {"state": state, "reviews": synthetic_reviews(pipeline.BOOK_STAGES), "scorecard": synthetic_scorecard(["s0000", "s0001", "s0002"])})
        pipeline.attest_book(argparse.Namespace(work=str(self.work)))

    def test_encoding_detection_and_explicit_cp949(self):
        self.assertEqual(pipeline.read_text(self.source)[0].splitlines(), SOURCE.splitlines())
        other = self.root / "cp949.txt"
        other.write_bytes("서장\n안녕".encode("cp949"))
        with self.assertRaises(pipeline.PipelineError):
            pipeline.read_text(other)
        self.assertIn("안녕", pipeline.read_text(other, "cp949")[0])

    def test_preamble_irregular_numbering_and_mismatched_end(self):
        text = "廣告\n서장\n시작\n<74화 문 (4)>\n내용\n<75화 문 (5)>\n끝난다\n<74화 문 (4)> 끝\n"
        sections = pipeline.parse_source(text)
        self.assertEqual([s["heading"]["chapter"] for s in sections], [None, None, "74", "75"])
        self.assertEqual(sections[0]["heading"]["kind"], "preamble")
        self.assertEqual(sections[-1]["blocks"][-1]["text"], "<74화 문 (4)> 끝")

    def test_matching_end_marker_is_tracked_without_duplicate_section(self):
        sections = pipeline.parse_source(SOURCE)
        self.assertEqual(len(sections), 3)
        self.assertEqual(sections[1]["format_lines"][0]["reason"], "matching_end_marker")

    def test_missing_source_block_fails(self):
        self.change_map(lambda m: m["alignments"].pop())
        with self.assertRaisesRegex(pipeline.PipelineError, "原文區塊缺漏"):
            self.record("s0001")

    def test_duplicate_source_coverage_fails(self):
        self.change_map(lambda m: m["alignments"].append(deepcopy(m["alignments"][0])))
        with self.assertRaisesRegex(pipeline.PipelineError, "原文區塊缺漏或重複"):
            self.record("s0001")

    def test_unmapped_added_sentence_fails(self):
        text = (self.work / "sections/s0001.txt").read_text(encoding="utf-8")
        self.update_translation(text + "\n這是自行增加的內容。\n")
        with self.assertRaisesRegex(pipeline.PipelineError, "譯文有未對應"):
            self.record("s0001")

    def test_many_to_one_mapping_is_allowed(self):
        self.update_translation("第1章 約定（1）\n\n木真說：「我明天會回來。」\n")
        self.change_map(lambda m: m.update(alignments=[{"source_ids": ["s0001:p0001", "s0001:p0002"], "disposition": "translated", "target_lines": [3, 3]}]))
        self.record("s0001")

    def test_source_index_tampering_fails(self):
        manifest = pipeline.load_json(self.work / "manifest.json")
        manifest["sections"].pop()
        write_json(self.work / "manifest.json", manifest)
        with self.assertRaisesRegex(pipeline.PipelineError, "索引與輸入不符"):
            pipeline.load_project(self.work)

    def test_source_change_invalidates_project(self):
        self.source.write_text(SOURCE + "變更", encoding="utf-16")
        with self.assertRaisesRegex(pipeline.PipelineError, "source 檔案已改變"):
            pipeline.load_project(self.work)

    def test_reference_change_invalidates_project(self):
        self.reference.write_text("另一個名字", encoding="utf-8")
        with self.assertRaisesRegex(pipeline.PipelineError, "reference 檔案已改變"):
            pipeline.load_project(self.work)

    def test_draft_exists_but_resume_starts_at_first_unverified(self):
        self.record("s0002")
        self.assertEqual(self.report()["next"], "s0000")

    def test_review_attestation_is_required(self):
        self.change_map(lambda m: m["reviews"].pop("post_polish"))
        with self.assertRaisesRegex(pipeline.PipelineError, "尚未記錄"):
            self.record("s0001")

    def test_pronoun_and_taiwan_usage_reviews_are_required(self):
        original = pipeline.load_json(self.map_path())
        for stage in ("referents", "locale"):
            with self.subTest(stage=stage):
                mapping = deepcopy(original)
                mapping["reviews"].pop(stage)
                write_json(self.map_path(), mapping)
                with self.assertRaisesRegex(pipeline.PipelineError, "主詞指代.*台灣用語"):
                    self.record("s0001")

    def test_exactly_85_is_accepted_and_below_is_rejected_without_rounding(self):
        self.change_map(lambda m: m.update(scorecard=synthetic_scorecard(["s0001"], 85)))
        self.assertEqual(self.record("s0001")["average_score"], 85)
        self.change_map(lambda m: m.update(scorecard=synthetic_scorecard(["s0001"], 84.99)))
        with self.assertRaisesRegex(pipeline.PipelineError, "未達 85"):
            self.record("s0001")

    def test_threshold_is_average_not_an_invented_per_item_floor(self):
        card = synthetic_scorecard(["s0001"], 90)
        card["items"]["literary_style"]["score"] = 50
        self.assertEqual(pipeline.check_scorecard(card, {"s0001"})["average"], 86)

    def test_scorecard_requires_all_ten_items_and_valid_evidence(self):
        card = synthetic_scorecard(["s0001"])
        card["items"].pop("emotion")
        with self.assertRaisesRegex(pipeline.PipelineError, "十項評分必須完整"):
            pipeline.check_scorecard(card, {"s0001"})
        card = synthetic_scorecard(["s9999"])
        with self.assertRaisesRegex(pipeline.PipelineError, "有效原文區塊"):
            pipeline.check_scorecard(card, {"s0001"})

    def test_non_finite_boolean_and_out_of_range_scores_fail(self):
        for value in (float("nan"), float("inf"), True, 101, -1):
            with self.subTest(value=value):
                card = synthetic_scorecard(["s0001"])
                card["items"]["fidelity"]["score"] = value
                with self.assertRaises(pipeline.PipelineError):
                    pipeline.check_scorecard(card, {"s0001"})

    def test_excluded_preamble_does_not_receive_invented_literary_scores(self):
        score = pipeline.check_scorecard({"method": "not_applicable", "reason": "合成測試前置廣告。"}, {"s0000"}, excluded_preamble=True)
        self.assertIsNone(score["average"])
        with self.assertRaises(pipeline.PipelineError):
            pipeline.check_scorecard({"method": "not_applicable", "reason": "不想評分。"}, {"s0001"})

    def test_simplified_characters_block_record_even_with_high_scores(self):
        text = (self.work / "sections/s0001.txt").read_text(encoding="utf-8").replace("說道", "说道")
        self.update_translation(text)
        with self.assertRaisesRegex(pipeline.PipelineError, "簡體字"):
            self.record("s0001")

    def test_mainland_usage_requires_contextual_revision(self):
        text = (self.work / "sections/s0001.txt").read_text(encoding="utf-8").replace("說道", "傳來信息")
        self.update_translation(text)
        with self.assertRaisesRegex(pipeline.PipelineError, "中國慣用語"):
            self.record("s0001")

    def test_valid_traditional_ambiguous_characters_are_not_blocked(self):
        pipeline.check_taiwan_text("王后在几案旁大喊，拔出刀刃，走向山崖。古人云，尸位素餐。范先生住在村里。", {}, set())

    def test_language_allowance_only_applies_to_its_line_range(self):
        mapping = {"language_allowances": [{"text": "信息", "target_lines": [1, 1], "source_ids": ["s0001:p0001"], "reason": "合成測試：精確原名例外。"}]}
        with self.assertRaisesRegex(pipeline.PipelineError, "中國慣用語"):
            pipeline.check_taiwan_text("原名信息\n一般信息", mapping, {"s0001:p0001"})
        pipeline.check_taiwan_text("原名信息\n一般消息", mapping, {"s0001:p0001"})

    def test_wrong_chapter_number_fails(self):
        text = (self.work / "sections/s0001.txt").read_text(encoding="utf-8").replace("第1章", "第2章")
        self.update_translation(text)
        self.change_map(lambda m: m.update(title_zh="第2章 約定（1）"))
        with self.assertRaisesRegex(pipeline.PipelineError, "中文章號"):
            self.record("s0001")

    def test_modified_mapping_requires_record_again(self):
        self.record_all()
        self.change_map(lambda m: m["reviews"]["fluency"].update(note="合成測試更新紀錄。"))
        self.assertEqual(self.report()["next"], "s0001")

    def test_used_term_change_only_invalidates_dependent_sections(self):
        self.record_all()
        self.terms[0]["zh"] = "木貞"
        write_rows(self.work / "terminology.jsonl", self.terms)
        result = self.report()
        self.assertEqual(result["verified"], 1)
        self.assertEqual(result["next"], "s0001")

    def test_unrelated_term_does_not_invalidate_sections(self):
        self.record_all()
        term = deepcopy(self.terms[0])
        term.update(id="other", ko=["청산"], zh="青山")
        write_rows(self.work / "terminology.jsonl", self.terms + [term])
        self.assertEqual(self.report()["verified"], 3)

    def test_new_matching_term_requires_declared_dependency(self):
        self.record_all()
        term = deepcopy(self.terms[0])
        term.update(id="door", ko=["문"], zh="門")
        write_rows(self.work / "terminology.jsonl", self.terms + [term])
        self.assertEqual(self.report()["next"], "s0000")

    def test_pending_term_prevents_record(self):
        self.terms[0]["decision"] = "pending"
        write_rows(self.work / "terminology.jsonl", self.terms)
        with self.assertRaisesRegex(pipeline.PipelineError, "譯名尚未決定"):
            self.record("s0001")

    def test_fictional_species_accepts_descriptive_chinese_without_real_taxon(self):
        invented = {"id": "fiction-001", "ko": ["은빛날개늑대"], "zh": "銀翼狼", "category": "creature", "origin": "author_created", "decision": "adopted", "basis": "source_description", "evidence": [{"note": "合成測試：原稿描述銀色翅膀的狼。"}]}
        write_rows(self.work / "terminology.jsonl", self.terms + [invented])
        self.assertEqual(pipeline.load_project(self.work)[2][-1]["zh"], "銀翼狼")

    def test_species_requires_origin_classification(self):
        self.terms[0]["category"] = "plant"
        write_rows(self.work / "terminology.jsonl", self.terms)
        with self.assertRaisesRegex(pipeline.PipelineError, "物種來源判定"):
            pipeline.load_project(self.work)

    def test_uncertain_origin_can_have_an_adopted_name(self):
        self.terms[0].update(category="plant", origin="uncertain")
        write_rows(self.work / "terminology.jsonl", self.terms)
        self.record("s0001")

    def test_continuity_change_invalidates_current_and_later(self):
        self.record_all()
        self.contexts[1]["summary"] = "合成測試更新人物已知資訊。"
        write_rows(self.work / "continuity.jsonl", self.contexts)
        result = self.report()
        self.assertEqual(result["verified"], 1)
        self.assertEqual(result["next"], "s0001")

    def test_open_issue_prevents_record(self):
        write_rows(self.work / "reviews.jsonl", [{"id": "i1", "section_ids": ["s0001"], "status": "open", "note": "合成測試尚未確認主詞。"}])
        with self.assertRaisesRegex(pipeline.PipelineError, "未解決"):
            self.record("s0001")

    def test_fluent_but_wrong_meaning_is_not_detected_by_mechanics(self):
        # 刻意錯譯否定，證明模擬的 passed 收據不能充當語意判讀器。
        text = (self.work / "sections/s0000.txt").read_text(encoding="utf-8").replace("我推開了門。", "我沒有推開門。")
        self.update_translation(text, "s0000")
        self.record("s0000")
        self.assertEqual(self.report()["verified"], 1)

    def test_complete_assembly_and_input_protection(self):
        self.record_all()
        with self.assertRaises(FileNotFoundError):
            pipeline.assemble_book(argparse.Namespace(work=str(self.work), output=str(self.root / "完整版.txt")))
        self.attest()
        before = self.source.read_bytes()
        with self.assertRaises(pipeline.PipelineError):
            pipeline.assemble_book(argparse.Namespace(work=str(self.work), output=str(self.source)))
        self.assertEqual(self.source.read_bytes(), before)
        output = self.root / "完整版.txt"
        result = pipeline.assemble_book(argparse.Namespace(work=str(self.work), output=str(output)))
        self.assertEqual(result["sections"], 3)
        content = output.read_text(encoding="utf-8")
        self.assertLess(content.index("序章"), content.index("第1章 約定（1）"))
        self.assertLess(content.index("第1章 約定（1）"), content.index("第1章 約定（2）"))
        with self.assertRaisesRegex(pipeline.PipelineError, "交付檔已存在"):
            pipeline.assemble_book(argparse.Namespace(work=str(self.work), output=str(output)))

    def test_book_state_change_requires_new_book_attestation(self):
        self.record_all()
        self.attest()
        review = pipeline.load_json(self.work / "book-review.json")
        review["reviews"]["reading"]["note"] = "合成測試更新全書複核。"
        write_json(self.work / "book-review.json", review)
        with self.assertRaisesRegex(pipeline.PipelineError, "全書複核尚未記錄"):
            pipeline.assemble_book(argparse.Namespace(work=str(self.work), output=str(self.root / "完整版.txt")))

    def test_assembly_spaces_every_content_line_without_changing_reviewed_sections(self):
        sections = {
            "s0000": "序章\r\n我推開了門。\r\n裡面一個人也沒有。\r\n",
            "s0001": "\n第1章 約定（1）\n\n\n木真說道：\n \t\n\n「我明天會回來。」\n\n\n",
            "s0002": "第1章 約定（2）\n木真回來了。",
        }
        for identity, content in sections.items():
            path = self.work / "sections" / f"{identity}.txt"
            path.write_bytes(content.encode("utf-8"))
            mapping = pipeline.load_json(self.map_path(identity))
            body_lines = [i for i, line in enumerate(content.splitlines(), 1) if line.strip()][1:]
            for alignment, line in zip(mapping["alignments"], body_lines):
                alignment["target_lines"] = [line, line]
            mapping["translation_sha256"] = pipeline.digest(path.read_bytes())
            write_json(self.map_path(identity), mapping)
        self.record_all()
        self.attest()
        before = {path: path.read_bytes() for path in (self.work / "sections").iterdir() if path.is_file()}
        output = self.root / "完整版.txt"
        result = pipeline.assemble_book(argparse.Namespace(work=str(self.work), output=str(output)))
        expected = ("━━━━━━━━━━━━━━━━\n\n序章\n\n━━━━━━━━━━━━━━━━\n\n我推開了門。\n\n裡面一個人也沒有。\n\n"
                    "━━━━━━━━━━━━━━━━\n\n第1章 約定（1）\n\n━━━━━━━━━━━━━━━━\n\n木真說道：\n\n「我明天會回來。」\n\n"
                    "第1章 約定（2）\n\n木真回來了。\n")
        self.assertEqual(output.read_bytes(), expected.encode("utf-8"))
        self.assertEqual(result["sha256"], pipeline.digest(output.read_bytes()))
        self.assertEqual({path: path.read_bytes() for path in before}, before)
        self.assertEqual(self.report()["errors"], [])

    def test_whole_book_average_must_also_reach_85(self):
        self.record_all()
        state = pipeline.book_state(pipeline.load_project(self.work))
        write_json(self.work / "book-review.json", {"state": state, "reviews": synthetic_reviews(pipeline.BOOK_STAGES), "scorecard": synthetic_scorecard(["s0000", "s0001", "s0002"], 84)})
        with self.assertRaisesRegex(pipeline.PipelineError, "未達 85"):
            pipeline.attest_book(argparse.Namespace(work=str(self.work)))
        self.assertFalse((self.work / "book-receipt.json").exists())


class FinalTextLayoutTests(unittest.TestCase):
    def test_chapter_frames_follow_manifest_and_preserve_same_chapter_parts(self):
        sections = [
            {"id": "s0000", "heading": {"kind": "preamble", "chapter": None}},
            {"id": "s0001", "heading": {"kind": "special", "chapter": None}},
            {"id": "s0002", "heading": {"kind": "chapter", "chapter": "01", "part": "1"}},
            {"id": "s0003", "heading": {"kind": "chapter", "chapter": "1", "part": "2"}},
            {"id": "s0004", "heading": {"kind": "preamble", "chapter": None}},
            {"id": "s0005", "heading": {"kind": "chapter", "chapter": "1", "part": "3"}},
            {"id": "s0006", "heading": {"kind": "chapter", "chapter": "2", "part": None}},
            {"id": "s0007", "heading": {"kind": "special", "chapter": None}},
        ]
        pieces = [" \n", "序章\n序文。", "第1章 門（1）\r\n「第9章還沒開始。」",
                  "第1章 門（2）\n＊＊＊\n第二段。", "\n\t", "第1章 門（3）\n第三段。",
                  "第2章 路\n下一章。", "後記\n後記正文。"]
        expected = ("━━━━━━━━━━━━━━━━\n\n序章\n\n━━━━━━━━━━━━━━━━\n\n序文。\n\n"
                    "━━━━━━━━━━━━━━━━\n\n第1章 門（1）\n\n━━━━━━━━━━━━━━━━\n\n「第9章還沒開始。」\n\n"
                    "第1章 門（2）\n\n第二段。\n\n第1章 門（3）\n\n第三段。\n\n"
                    "━━━━━━━━━━━━━━━━\n\n第2章 路\n\n━━━━━━━━━━━━━━━━\n\n下一章。\n\n"
                    "━━━━━━━━━━━━━━━━\n\n後記\n\n━━━━━━━━━━━━━━━━\n\n後記正文。\n")
        self.assertEqual(pipeline.render_final_book(pieces, sections), expected)

    def test_final_book_removes_standalone_scene_stars_without_replacing_them(self):
        pieces = ["第1章 門\n第一段。\n★ ★ ★\n＊＊＊\n* * *\n☆　☆　☆\n……\n「紙上寫著＊＊＊。」\n第二段。"]
        sections = [{"id": "s0001", "heading": {"chapter": "1"}}]
        expected = ("━━━━━━━━━━━━━━━━\n\n第1章 門\n\n━━━━━━━━━━━━━━━━\n\n"
                    "第一段。\n\n……\n\n「紙上寫著＊＊＊。」\n\n第二段。\n")
        self.assertEqual(pipeline.render_final_book(pieces, sections), expected)

    def test_chapter_rendering_rejects_missing_section_metadata(self):
        for pieces, sections in [(["序章\n正文。"], []), ([], [{"id": "s0000"}])]:
            with self.subTest(pieces=pieces), self.assertRaisesRegex(pipeline.PipelineError, "分節數量"):
                pipeline.render_final_book(pieces, sections)

    def test_spacing_preserves_content_and_is_idempotent(self):
        cases = [
            (["標題\n第一行\n第二行"], "標題\n\n第一行\n\n第二行\n"),
            (["\n甲\n\n\n\t \n乙\n\n"], "甲\n\n乙\n"),
            (["甲\r\n乙\r丙"], "甲\n\n乙\n\n丙\n"),
            (["甲\n\n", "", " \t\n", "\n乙"], "甲\n\n乙\n"),
            (["　詩行甲  \n詩行乙\n＊＊＊\n「對話。」"], "　詩行甲  \n\n詩行乙\n\n＊＊＊\n\n「對話。」\n"),
            (["甲\u2028乙"], "甲\u2028乙\n"),
            (["\r\n \t\n", ""], ""),
            ([], ""),
        ]
        for pieces, expected in cases:
            with self.subTest(pieces=pieces):
                actual = pipeline.render_final_text(pieces)
                self.assertEqual(actual, expected)
                self.assertEqual(pipeline.render_final_text([actual]), actual)


class BatchPlanningTests(unittest.TestCase):
    """驗證字數口徑、原文覆蓋與批次界線，不模擬真實翻譯品質。"""

    def test_counts_normalized_korean_without_counting_newlines(self):
        metrics = pipeline.text_metrics("가 나.\r\n가!")
        self.assertEqual(metrics, {"chars_with_spaces": 6, "chars_without_whitespace": 5,
                                   "hangul_characters": 3, "eojeol": 3})

    def test_large_special_section_is_visible_in_inventory(self):
        source = "1화 문\n가나다\n작가의 말\n" + "가" * 13000
        inventory = pipeline.source_inventory(source, pipeline.parse_source(source))
        self.assertEqual(inventory["numbered_section_distribution"]["max"], 3)
        self.assertEqual(inventory["all_section_distribution"]["max"], 13000)
        self.assertEqual(inventory["largest_sections"][0]["section_id"], "s0001")
        self.assertEqual(inventory["structure_warnings"][0]["section_id"], "s0001")

    def test_chunk_coverage_batch_limits_and_first_pilot(self):
        sections = pipeline.parse_source("1화 문\n가나다라\n마바사아\n자차카타\n2화 길\n파하나\n")
        before = deepcopy(sections)
        plan = pipeline.build_batch_plan(sections, "source-hash", 4, 6, 10, 2)
        self.assertEqual([c["chars"] for c in plan["chunks"]], [4, 4, 4, 3])
        self.assertEqual([b["chars"] for b in plan["batches"]], [4, 8, 3])
        self.assertEqual(plan["batches"][0]["kind"], "pilot")
        self.assertEqual([c["first_source_id"] for c in plan["chunks"]],
                         ["s0000:p0001", "s0000:p0002", "s0000:p0003", "s0001:p0001"])
        self.assertEqual(sections, before)
        self.assertEqual(plan, pipeline.build_batch_plan(sections, "source-hash", 4, 6, 10, 2))

    def test_long_book_has_ten_delivery_batches_without_losing_work_units(self):
        source = "\n".join(f"{number}화 문\n가나다라마바" for number in range(1, 13))
        sections = pipeline.parse_source(source)
        plan = pipeline.build_batch_plan(sections, "hash", 4, 6, 10, 2, 3)
        delivery = plan["delivery_batches"]
        self.assertEqual(len(delivery), 3)
        self.assertEqual(plan["summary"]["delivery_batches"], 3)
        self.assertEqual([unit for group in delivery for unit in group["work_unit_ids"]],
                         [unit["id"] for unit in plan["batches"]])
        self.assertEqual([group["chars"] for group in delivery], [12, 30, 30])
        self.assertEqual([group["id"] for group in delivery], ["d0001", "d0002", "d0003"])
        self.assertEqual([group["section_count"] for group in delivery], [2, 5, 5])

    def test_few_work_units_produce_fewer_nonempty_delivery_batches(self):
        sections = pipeline.parse_source("1화 문\n가나다\n2화 길\n라마바\n")
        plan = pipeline.build_batch_plan(sections, "hash", 4, 6, 10, 2, 10)
        self.assertEqual(len(plan["delivery_batches"]), 2)
        self.assertTrue(all(group["work_unit_ids"] for group in plan["delivery_batches"]))

    def test_long_sections_are_not_split_between_delivery_batches(self):
        units = [{"id": f"b{index:05d}", "section_id": "s0000" if index <= 5 else "s0001",
                  "chars": 4000, "blocked": False} for index in range(1, 11)]
        delivery = pipeline.group_delivery_batches(units, 10)
        self.assertEqual(len(delivery), 2)
        self.assertEqual([group["work_unit_ids"] for group in delivery],
                         [[unit["id"] for unit in units[:5]], [unit["id"] for unit in units[5:]]])

    def test_blocked_work_unit_blocks_its_delivery_batch(self):
        sections = pipeline.parse_source("1화 문\n가나다라마바사아자\n")
        plan = pipeline.build_batch_plan(sections, "hash", 4, 6, 10, 2, 10)
        self.assertEqual(len(plan["delivery_batches"]), 1)
        self.assertTrue(plan["delivery_batches"][0]["blocked"])

    def test_nonpositive_delivery_target_is_rejected(self):
        sections = pipeline.parse_source("1화 문\n가나다\n")
        with self.assertRaises(pipeline.PipelineError):
            pipeline.build_batch_plan(sections, "hash", 4, 6, 10, 2, 0)

    def test_scene_boundary_takes_priority_near_target(self):
        sections = pipeline.parse_source("1화 문\n가나다\n라마바사\n* * *\n아자차\n카타파\n하나둘\n셋넷오\n")
        plan = pipeline.build_batch_plan(sections, "hash", 8, 12, 24, 3)
        self.assertEqual(plan["chunks"][0]["last_source_id"], "s0000:p0002")
        self.assertEqual(plan["chunks"][1]["first_source_id"], "s0000:p0003")
        self.assertEqual(plan["chunks"][0]["cut_after"], "scene")

    def test_oversized_line_is_flagged_without_silent_truncation(self):
        sections = pipeline.parse_source("1화 문\n가나다라마바사아자\n차카\n")
        plan = pipeline.build_batch_plan(sections, "hash", 4, 6, 10, 2)
        self.assertEqual(plan["chunks"][0]["chars"], 9)
        self.assertTrue(plan["chunks"][0]["requires_manual_split"])
        self.assertTrue(plan["batches"][0]["blocked"])
        self.assertEqual(plan["summary"]["body_chars"], 11)
        self.assertEqual(plan["summary"]["oversized_chunks"], 1)

    def test_preamble_does_not_consume_story_pilot(self):
        sections = pipeline.parse_source("來源\n1화 문\n가나다라\n마바사아\n자차카타\n")
        plan = pipeline.build_batch_plan(sections, "hash", 4, 6, 10, 2)
        self.assertEqual([b["kind"] for b in plan["batches"]], ["preamble_review", "pilot", "translation"])

    def test_unbalanced_sections_stay_complete_and_ordered(self):
        sections = pipeline.parse_source("서장\n\n1화 문\n" + "가나다\n" * 17 + "2화 길\n아\n")
        plan = pipeline.build_batch_plan(sections, "hash", 8, 12, 20, 2)
        covered = []
        for chunk in plan["chunks"]:
            section = next(s for s in sections if s["id"] == chunk["section_id"])
            blocks = section["blocks"]
            if blocks:
                ids = [b["id"] for b in blocks]
                covered.extend(ids[ids.index(chunk["first_source_id"]):ids.index(chunk["last_source_id"]) + 1])
            self.assertLessEqual(chunk["chars"], 12)
        self.assertEqual(covered, [b["id"] for s in sections for b in s["blocks"]])
        self.assertEqual(len(covered), len(set(covered)))
        self.assertEqual(plan["chunks"][0]["block_count"], 0)
        for batch in plan["batches"]:
            self.assertLessEqual(batch["chars"], 20)
            self.assertLessEqual(len(batch["chunk_ids"]), 2)

    def test_invalid_budgets_are_rejected(self):
        sections = pipeline.parse_source("서장\n가")
        for values in ((0, 6, 10, 2), (7, 6, 10, 2), (4, 6, 5, 2), (4, 6, 10, 0)):
            with self.subTest(values=values), self.assertRaises(pipeline.PipelineError):
                pipeline.build_batch_plan(sections, "hash", *values)

    def test_plan_cli_is_read_only_and_refuses_overwrite(self):
        root = Path(tempfile.mkdtemp(prefix="novel-batch-cli-"))
        source = root / "原稿.txt"
        source.write_text("1화 문\n가 나.\n", encoding="utf-16")
        before = source.read_bytes()
        args = ["plan", "--source", str(source)]
        output = io.StringIO()
        with redirect_stdout(output):
            status = pipeline.cli(args)
        self.assertEqual(status, 0)
        self.assertEqual(json.loads(output.getvalue())["summary"]["body_chars"], 4)
        self.assertEqual(json.loads(output.getvalue())["summary"]["delivery_batches"], 1)
        self.assertEqual(list(root.iterdir()), [source])
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            self.assertEqual(pipeline.cli(args + ["--output", str(source)]), 2)
        self.assertEqual(source.read_bytes(), before)
        plan_path = root / "batch-plan.json"
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            self.assertEqual(pipeline.cli(args + ["--output", str(plan_path)]), 0)
            self.assertEqual(pipeline.cli(args + ["--output", str(plan_path)]), 2)

    def test_show_range_uses_existing_ids_without_changing_section(self):
        section = pipeline.parse_source(SOURCE)[0]
        shown = pipeline.show_section(section, "s0000:p0002", "s0000:p0002")
        self.assertEqual([b["text"] for b in shown["blocks"]], ["아무도 없었다."])
        self.assertEqual(len(section["blocks"]), 2)
        self.assertTrue(shown["partial"])
        with self.assertRaises(pipeline.PipelineError):
            pipeline.show_section(section, "s0000:p0002", "s0000:p0001")


if __name__ == "__main__":
    unittest.main(verbosity=2)
