"""以合成短文驗證自動產物與共用閱讀，不執行真實小說翻譯。"""

import argparse
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import chunk_workflow
import section_context
import novel_pipeline as core
from test_novel_pipeline import SOURCE, TRANSLATIONS, synthetic_reviews, synthetic_scorecard, write_json, write_rows


class WorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.run_root = Path(tempfile.mkdtemp(prefix="novel-workflow-tests-"))

    def setUp(self):
        self.root = self.run_root / self._testMethodName
        self.root.mkdir()
        self.source, self.reference = self.root / "韓文.txt", self.root / "人工版.txt"
        self.source.write_text(SOURCE, encoding="utf-8")
        self.reference.write_text("木真說明天回來。", encoding="utf-8")
        self.work = self.root / "translation-work"
        core.init_project(argparse.Namespace(source=self.source, reference=self.reference, work=self.work, title="合成測試", source_encoding="auto", reference_encoding="auto", heading_pattern=None, source_layout="inline"))
        self.manifest = core.load_json(self.work / "manifest.json")
        self.plan = core.build_batch_plan(self.manifest["sections"], self.manifest["source"]["sha256"], target_chars=10, max_chars=14, batch_chars=28)
        write_json(self.work / "batch-plan.json", self.plan)
        self.terms = [{"id": "person-001", "ko": ["목진"], "zh": "木真", "decision": "adopted", "basis": "human_reference", "evidence": ["合成測試人工版"], "forbidden_zh": ["穆真"]}]
        write_rows(self.work / "terminology.jsonl", self.terms)
        self.contexts = [{"section_id": s["id"], "summary": "合成測試敘事狀態。", "facts": []} for s in self.manifest["sections"]]
        write_rows(self.work / "continuity.jsonl", self.contexts)

    def flow(self):
        return chunk_workflow.Workflow(core, self.work)

    def invoke(self, command, **kwargs):
        return chunk_workflow.run(core, argparse.Namespace(command=command, work=self.work, replace=False, **kwargs))

    def payload(self, chunk):
        section = self.flow().views[chunk]
        return {"units": [{"source_ids": [b["id"]], "text": TRANSLATIONS[b["text"]]} for b in section["blocks"]],
                "reviews": synthetic_reviews(core.STAGES),
                "state_after": {"viewpoint": "合成敘事視角", "time_place": "合成場景", "speakers": [], "known_facts": [], "open_threads": []}}

    def save(self, chunk, data=None, replace=False):
        path = self.root / (chunk + ".input.json")
        write_json(path, self.payload(chunk) if data is None else data)
        return chunk_workflow.run(core, argparse.Namespace(command="save-chunk", work=self.work, chunk=chunk, input=path, replace=replace))

    def save_section(self, identity):
        for chunk in self.flow().section_chunks(identity):
            self.save(chunk)

    def merge(self, identity="s0000", replace=False):
        section = core.section_by_id(self.manifest, identity)
        heading = section["heading"]
        title = "序章" if not heading["chapter"] else f"第{heading['chapter']}章 約定（{heading['part']}）"
        return chunk_workflow.run(core, argparse.Namespace(command="merge-section", work=self.work, section=identity, title=title, replace=replace))

    def review_payload(self, identity):
        state = self.invoke("review-state", section=identity)
        return {**state, "reviews": synthetic_reviews(core.STAGES + ("heading",)), "scorecard": synthetic_scorecard([identity]), "book_reviews": synthetic_reviews(core.BOOK_STAGES)}

    def review(self, identity="s0000", data=None):
        path = self.root / (identity + ".review.json")
        write_json(path, self.review_payload(identity) if data is None else data)
        return self.invoke("review-section", section=identity, input=path)

    def complete_section(self, identity):
        self.save_section(identity)
        self.merge(identity)
        return self.review(identity)

    def test_save_calculates_mapping_fingerprints_and_progress(self):
        chunk = self.plan["chunks"][0]["id"]
        self.save(chunk)
        flow = self.flow()
        text = (self.work / "chunks" / (chunk + ".txt")).read_text(encoding="utf-8")
        mapping = core.load_json(self.work / "chunks" / (chunk + ".map.json"))
        self.assertEqual(mapping["translation_sha256"], core.digest(text))
        self.assertEqual(flow.progress["chunks"][chunk]["status"], "self_checked")
        self.assertEqual(len(flow.checked_chunks([chunk])), 1)
        self.assertEqual(flow.progress["book_review"]["completed_batches"], [])

    def test_multiline_many_to_one_mapping_is_computed_after_spacing(self):
        flow = self.flow()
        units = [{"source_ids": ["a", "b"], "text": "「我會回來。」\n\n他推開了門。"}, {"source_ids": ["c"], "text": "「好。」"}]
        text, mapping, _ = flow.render_units(units, ["a", "b", "c"])
        self.assertEqual(text, "「我會回來。」\n\n他推開了門。\n\n「好。」\n")
        self.assertEqual([m["target_lines"] for m in mapping], [[1, 3], [5, 5]])

    def test_missing_duplicate_and_reordered_sources_are_rejected(self):
        flow = self.flow()
        for ids in (["a"], ["a", "a"], ["b", "a"]):
            with self.subTest(ids=ids), self.assertRaisesRegex(core.PipelineError, "來源 IDs"):
                flow.render_units([{"source_ids": ids, "text": "合成譯文。"}], ["a", "b"])

    def test_language_allowances_get_exact_lines(self):
        _, _, allowances = self.flow().render_units([{"source_ids": ["a"], "text": "引文：信息\n\n結尾。", "language_allowances": [{"text": "信息", "reason": "合成原作引文。"}]}], ["a"])
        self.assertEqual(allowances[0]["target_lines"], [1, 1])

    def test_missing_reviews_never_creates_artifacts(self):
        identity = self.plan["chunks"][0]["id"]
        data = self.payload(identity)
        for value in core.STAGES:
            invalid = deepcopy(data)
            del invalid["reviews"][value]
            with self.subTest(value=value), self.assertRaises(core.PipelineError):
                self.save(identity, invalid)
        self.assertFalse((self.work / "chunks").exists())

    def test_terms_are_detected_without_manual_hashes(self):
        self.save_section("s0001")
        identity = self.flow().section_chunks("s0001")[0]
        mapping = core.load_json(self.work / "chunks" / (identity + ".map.json"))
        self.assertEqual(mapping["term_ids"], ["person-001"])
        self.assertEqual(mapping["term_dependencies"]["person-001"], core.digest(core.canonical(self.terms[0])))

    def creature_name_submission(self, *, wrong_yual_name=False):
        root = self.root / "creature-name-collision"
        root.mkdir()
        source = root / "韓文.txt"
        reference = root / "人工版.txt"
        source.write_text("<1화 이명 테스트 (1)>\n유알이 말했다.\n\n알유가 말했다.\n", encoding="utf-8")
        reference.write_text("幽頞、猰貐。", encoding="utf-8")
        work = root / "translation-work"
        core.init_project(argparse.Namespace(source=source, reference=reference, work=work, title="異名測試",
                                             source_encoding="auto", reference_encoding="auto", heading_pattern=None,
                                             source_layout="inline"))
        manifest = core.load_json(work / "manifest.json")
        plan = core.build_batch_plan(manifest["sections"], manifest["source"]["sha256"],
                                     target_chars=100, max_chars=6000, batch_chars=6000)
        write_json(work / "batch-plan.json", plan)
        terms = [
            {"id": "creature-yual", "ko": ["유알"], "zh": "幽頞", "category": "creature",
             "origin": "author_created", "decision": "adopted", "basis": "human_reference",
             "evidence": ["人工版專名對照"], "forbidden_zh": ["猰貐"]},
            {"id": "creature-aryu", "ko": ["알유"], "zh": "猰貐", "category": "creature",
             "origin": "author_created", "decision": "adopted", "basis": "human_reference",
             "evidence": ["人工版專名對照"], "forbidden_zh": []},
        ]
        write_rows(work / "terminology.jsonl", terms)
        write_rows(work / "continuity.jsonl", [
            {"section_id": section["id"], "summary": "異名碰撞合成測試。", "facts": []}
            for section in manifest["sections"]
        ])
        flow = chunk_workflow.Workflow(core, work)
        chunks = flow.section_chunks(manifest["sections"][0]["id"])
        self.assertEqual(len(chunks), 1)
        chunk = chunks[0]
        units = []
        for block in flow.views[chunk]["blocks"]:
            if "유알" in block["text"]:
                translated = "猰貐說道：" if wrong_yual_name else "幽頞說道："
            else:
                translated = "猰貐說道："
            units.append({"source_ids": [block["id"]], "text": translated})
        data = {"units": units, "reviews": synthetic_reviews(core.STAGES),
                "state_after": {"viewpoint": "全知視角", "time_place": "合成場景", "speakers": [],
                                "known_facts": [], "open_threads": []}}
        input_path = root / "submit.json"
        write_json(input_path, data)
        result = chunk_workflow.run(core, argparse.Namespace(command="submit-chunk", work=work, chunk=chunk,
                                   input=input_path, merge=False, title=None, replace=False))
        return result, work, chunk

    def test_forbidden_name_in_another_entities_mapped_range_is_allowed(self):
        result, work, chunk = self.creature_name_submission()
        self.assertEqual(result["saved"], chunk)
        self.assertTrue((work / "chunks" / (chunk + ".txt")).is_file())

    def test_forbidden_name_in_its_own_mapped_range_is_rejected(self):
        with self.assertRaisesRegex(core.PipelineError, "creature-yual 出現本節禁用的譯名"):
            self.creature_name_submission(wrong_yual_name=True)

    def mokgan_name_submission(self, *, source_form, add_resolution=False, merge=False):
        root = self.root / "mokgan-name-collision"
        root.mkdir()
        source = root / "韓文.txt"
        reference = root / "人工版.txt"
        source.write_text(f"<1화 同音詞區分測試 (1)>\n{source_form}\n", encoding="utf-8")
        reference.write_text("目艮、木簡。", encoding="utf-8")
        work = root / "translation-work"
        core.init_project(argparse.Namespace(source=source, reference=reference, work=work, title="木簡同音測試",
                                             source_encoding="auto", reference_encoding="auto", heading_pattern=None,
                                             source_layout="inline"))
        manifest = core.load_json(work / "manifest.json")
        plan = core.build_batch_plan(manifest["sections"], manifest["source"]["sha256"],
                                     target_chars=100, max_chars=6000, batch_chars=6000)
        write_json(work / "batch-plan.json", plan)
        terms = [
            {"id": "title-mokgan", "ko": ["목간(目艮)", "목간"], "zh": "目艮", "decision": "adopted",
             "basis": "source_hanja", "evidence": ["括號漢字區分同音專名"], "forbidden_zh": ["木簡"]},
            {"id": "object-mokgan", "ko": ["목간(木簡)", "목간"], "zh": "木簡", "decision": "adopted",
             "basis": "source_hanja", "evidence": ["原文括號漢字指木簡實物"], "forbidden_zh": []},
        ]
        write_rows(work / "terminology.jsonl", terms)
        write_rows(work / "continuity.jsonl", [
            {"section_id": section["id"], "summary": "木簡同音詞合成測試。", "facts": []}
            for section in manifest["sections"]
        ])
        flow = chunk_workflow.Workflow(core, work)
        chunk = flow.section_chunks(manifest["sections"][0]["id"])[0]
        units = []
        for block in flow.views[chunk]["blocks"]:
            unit = {"source_ids": [block["id"]], "text": "木簡放在那裡。"}
            if add_resolution:
                unit["term_resolutions"] = [{"term_id": "title-mokgan", "forbidden_zh": "木簡",
                                             "resolves_as": "object-mokgan", "reason": "合成測試中的木製簡牘語境。"}]
            units.append(unit)
        data = {"units": units, "reviews": synthetic_reviews(core.STAGES),
                "state_after": {"viewpoint": "全知視角", "time_place": "合成場景", "speakers": [],
                                "known_facts": [], "open_threads": []}}
        input_path = root / "submit.json"
        write_json(input_path, data)
        result = chunk_workflow.run(core, argparse.Namespace(command="submit-chunk", work=work, chunk=chunk,
                                   input=input_path, merge=merge,
                                   title="第1章 同音詞區分測試（1）" if merge else None, replace=False))
        return result, work, chunk

    def test_parenthetical_hanja_selects_mokgan_object_translation(self):
        result, work, chunk = self.mokgan_name_submission(source_form="목간(木簡)이 놓여 있었다.")
        self.assertEqual(result["saved"], chunk)
        self.assertTrue((work / "chunks" / (chunk + ".txt")).is_file())

    def test_parenthetical_hanja_does_not_allow_title_forbidden_translation(self):
        with self.assertRaisesRegex(core.PipelineError, "title-mokgan 出現本節禁用的譯名"):
            self.mokgan_name_submission(source_form="목간(目艮)이 쓰러졌다.")

    def test_bare_mokgan_object_requires_explicit_source_scoped_resolution(self):
        result, work, chunk = self.mokgan_name_submission(
            source_form="낡은 목간 하나와 서책이 들어있었다.", add_resolution=True)
        self.assertEqual(result["saved"], chunk)
        self.assertTrue((work / "chunks" / (chunk + ".txt")).is_file())

    def test_bare_mokgan_without_resolution_is_rejected(self):
        with self.assertRaisesRegex(core.PipelineError, "title-mokgan 出現本節禁用的譯名"):
            self.mokgan_name_submission(source_form="낡은 목간 하나와 서책이 들어있었다.")

    def test_resolution_cannot_override_explicit_title_hanja(self):
        with self.assertRaisesRegex(core.PipelineError, "消歧依據與原文詞形不符"):
            self.mokgan_name_submission(source_form="목간(目艮)이 쓰러졌다.", add_resolution=True)

    def test_term_resolution_lines_are_rebased_during_section_merge(self):
        result, work, _ = self.mokgan_name_submission(
            source_form="낡은 목간 하나와 서책이 들어있었다.", add_resolution=True, merge=True)
        self.assertIn("section", result)
        section_id = result["section"]["merged"]
        mapping = core.load_json(work / "sections" / (section_id + ".map.json"))
        text = (work / "sections" / (section_id + ".txt")).read_text(encoding="utf-8").splitlines()
        resolution = next(resolution for entry in mapping["alignments"]
                          for resolution in entry.get("term_resolutions", []))
        start, end = resolution["target_lines"]
        self.assertEqual(start, end)
        self.assertIn("木簡", text[start - 1])

    def test_changed_term_dependency_blocks_merge(self):
        self.save_section("s0001")
        self.terms[0]["evidence"].append("補充的合成依據")
        write_rows(self.work / "terminology.jsonl", self.terms)
        with self.assertRaisesRegex(core.PipelineError, "依據已失效"):
            self.merge("s0001")

    def test_changed_previous_context_blocks_merge(self):
        self.save_section("s0001")
        self.contexts[0]["summary"] = "已修改的合成前節狀態。"
        write_rows(self.work / "continuity.jsonl", self.contexts)
        with self.assertRaisesRegex(core.PipelineError, "依據已失效"):
            self.merge("s0001")

    def test_external_chunk_edit_is_preserved(self):
        identity = self.plan["chunks"][0]["id"]
        self.save(identity)
        path = self.work / "chunks" / (identity + ".txt")
        path.write_text("人工調整的譯文。\n", encoding="utf-8")
        with self.assertRaises(core.PipelineError):
            self.save(identity, replace=True)
        self.assertEqual(path.read_text(encoding="utf-8"), "人工調整的譯文。\n")

    def test_save_is_repeatable_and_keeps_user_settings(self):
        identity = self.plan["chunks"][0]["id"]
        progress = self.flow().progress
        progress["style_profile"] = {"narrator_voice": "合成設定"}
        progress["final_output"] = {"path": str(self.root / "唯一完整版.txt"), "sha256": None}
        write_json(self.work / "batch-progress.json", progress)
        self.save(identity)
        self.save(identity)
        fresh = core.load_json(self.work / "batch-progress.json")
        self.assertEqual(fresh["style_profile"], progress["style_profile"])
        self.assertEqual(fresh["final_output"], progress["final_output"])

    def test_merge_offsets_lines_and_leaves_review_pending(self):
        self.save_section("s0000")
        result = self.merge()
        text = (self.work / "sections/s0000.txt").read_text(encoding="utf-8")
        mapping = core.load_json(self.work / "sections/s0000.map.json")
        self.assertEqual(text, "序章\n\n我推開了門。\n\n裡面一個人也沒有。\n")
        self.assertEqual([x["target_lines"] for x in mapping["alignments"]], [[3, 3], [5, 5]])
        self.assertEqual(mapping["reviews"], {})
        self.assertEqual(result["status"], "awaiting_section_review")
        self.assertNotIn("receipt", core.load_json(self.work / "manifest.json")["sections"][0])

    def test_merge_rejects_missing_or_changed_chunk(self):
        ids = self.flow().section_chunks("s0000")
        self.assertGreater(len(ids), 1)
        self.save(ids[0])
        with self.assertRaises((core.PipelineError, OSError)):
            self.merge()

    def test_merge_does_not_overwrite_manual_section_edit(self):
        self.save_section("s0000")
        self.merge()
        path = self.work / "sections/s0000.txt"
        path.write_text("序章\n\n人工修訂的譯稿。\n", encoding="utf-8")
        with self.assertRaisesRegex(core.PipelineError, "人工修訂"):
            self.merge(replace=True)
        self.assertIn("人工修訂", path.read_text(encoding="utf-8"))

    def test_single_review_updates_section_receipt_and_book_coverage(self):
        result = self.complete_section("s0000")
        flow = self.flow()
        self.assertTrue(result["shared_book_reading"])
        self.assertEqual(flow.progress["section_review"]["s0000"]["next_chunk"], None)
        section = core.section_by_id(flow.manifest, "s0000")
        self.assertEqual(section["receipt"], core.check_section(flow.project, section))
        expected = {b["id"] for b in self.plan["batches"] if b["section_id"] == "s0000"}
        self.assertEqual(set(flow.reading_records()), expected)

    def test_shared_reading_requires_explicit_book_checks(self):
        self.save_section("s0000")
        self.merge()
        data = self.review_payload("s0000")
        del data["book_reviews"]
        result = self.review(data=data)
        self.assertFalse(result["shared_book_reading"])
        self.assertEqual(self.flow().reading_records(), {})

    def test_partial_read_and_missing_checks_fail_without_receipt(self):
        self.save_section("s0000")
        self.merge()
        data = self.review_payload("s0000")
        for key in ("reviewed_chunk_ids", "reviews", "book_reviews"):
            invalid = deepcopy(data)
            invalid[key] = [] if key == "reviewed_chunk_ids" else {}
            with self.subTest(key=key), self.assertRaises(core.PipelineError):
                self.review(data=invalid)
        self.assertNotIn("receipt", core.load_json(self.work / "manifest.json")["sections"][0])

    def test_stale_reading_snapshot_is_rejected(self):
        self.save_section("s0000")
        self.merge()
        data = self.review_payload("s0000")
        self.contexts[0]["facts"].append("新增的合成敘事事實")
        write_rows(self.work / "continuity.jsonl", self.contexts)
        with self.assertRaisesRegex(core.PipelineError, "版本已改變"):
            self.review(data=data)

    def test_out_of_order_reading_does_not_claim_continuous_coverage(self):
        self.save_section("s0001")
        self.merge("s0001")
        with self.assertRaisesRegex(core.PipelineError, "前序全書閱讀"):
            self.review("s0001")

    def test_complete_book_still_requires_final_attestation_and_assembly(self):
        for identity in ("s0000", "s0001", "s0002"):
            self.complete_section(identity)
        flow = self.flow()
        self.assertEqual(flow.progress["phase"], "finalizing")
        self.assertFalse((self.work / "book-receipt.json").exists())
        state = core.book_state(core.load_project(self.work))
        write_json(self.work / "book-review.json", {"state": state, "reviews": synthetic_reviews(core.BOOK_STAGES), "scorecard": synthetic_scorecard(["s0000", "s0001", "s0002"])})
        core.attest_book(argparse.Namespace(work=self.work))
        output = self.root / "合成最終版.txt"
        core.assemble_book(argparse.Namespace(work=self.work, output=output))
        text = output.read_text(encoding="utf-8")
        self.assertEqual(text.count("序章"), 1)
        self.assertIn("木真說道：\n\n「我明天會回來。」", text)

    def test_missing_book_reading_blocks_attestation(self):
        for identity in ("s0000", "s0001", "s0002"):
            self.complete_section(identity)
        progress = core.load_json(self.work / "batch-progress.json")
        progress["book_review"]["completed_batches"].pop()
        write_json(self.work / "batch-progress.json", progress)
        with self.assertRaisesRegex(core.PipelineError, "全書閱讀紀錄缺漏"):
            core.book_state(core.load_project(self.work))

    def test_changed_shared_reading_evidence_invalidates_book_receipt(self):
        for identity in ("s0000", "s0001", "s0002"):
            self.complete_section(identity)
        state = core.book_state(core.load_project(self.work))
        self.assertIn("batch_reading_sha256", state)
        write_json(self.work / "book-review.json", {"state": state, "reviews": synthetic_reviews(core.BOOK_STAGES), "scorecard": synthetic_scorecard(["s0000", "s0001", "s0002"])})
        core.attest_book(argparse.Namespace(work=self.work))
        progress = core.load_json(self.work / "batch-progress.json")
        progress["book_review"]["completed_batches"][0]["reviews"]["reading"]["note"] = "已改動的合成閱讀依據。"
        write_json(self.work / "batch-progress.json", progress)
        with self.assertRaisesRegex(core.PipelineError, "資料已變更"):
            core.assemble_book(argparse.Namespace(work=self.work, output=self.root / "不可交付.txt"))
        self.assertFalse((self.root / "不可交付.txt").exists())

    def test_revision_invalidates_prior_section_and_book_reading(self):
        self.complete_section("s0000")
        identity = self.flow().section_chunks("s0000")[-1]
        data = self.payload(identity)
        data["units"][0]["text"] = "裡頭一個人也沒有。"
        self.save(identity, data, replace=True)
        flow = self.flow()
        self.assertNotIn("receipt", core.section_by_id(flow.manifest, "s0000"))
        self.assertEqual(flow.reading_records(), {})
        self.merge(replace=True)
        self.review()

    def test_bad_plan_and_stale_progress_are_rejected(self):
        plan_path = self.work / "batch-plan.json"
        invalid = deepcopy(self.plan)
        invalid["chunks"].pop()
        write_json(plan_path, invalid)
        with self.assertRaises(core.PipelineError):
            self.flow()
        write_json(plan_path, self.plan)
        self.save(self.plan["chunks"][0]["id"])
        progress = core.load_json(self.work / "batch-progress.json")
        progress["plan_sha256"] = "0" * 64
        write_json(self.work / "batch-progress.json", progress)
        with self.assertRaisesRegex(core.PipelineError, "進度與來源"):
            self.flow()

    def test_chunk_revision_requires_remerge_even_with_new_snapshot(self):
        self.save_section("s0000")
        self.merge()
        old_review = self.review_payload("s0000")
        identity = self.flow().section_chunks("s0000")[-1]
        data = self.payload(identity)
        data["units"][0]["text"] = "裡頭一個人也沒有。"
        self.save(identity, data, replace=True)
        with self.assertRaisesRegex(core.PipelineError, "版本已改變"):
            self.review(data=old_review)
        with self.assertRaisesRegex(core.PipelineError, "尚未重新合併"):
            self.review()

    def test_replaying_merged_chunk_keeps_verified_status(self):
        self.complete_section("s0000")
        identity = self.flow().section_chunks("s0000")[0]
        self.save(identity)
        self.assertEqual(self.flow().progress["chunks"][identity]["status"], "merged")

    def test_replacement_save_recovers_from_partial_file_write(self):
        identity = self.plan["chunks"][0]["id"]
        self.save(identity)
        data = self.payload(identity)
        data["units"][0]["text"] = "我打開了門。"
        original = core.atomic_text
        def fail_map(path, value):
            if path.name == identity + ".map.json":
                raise OSError("合成小段寫入中斷")
            return original(path, value)
        with patch.object(core, "atomic_text", side_effect=fail_map), self.assertRaises(OSError):
            self.save(identity, data, replace=True)
        self.save(identity, data, replace=True)
        self.assertIn("我打開了門。", self.flow().checked_chunks([identity])[0][1])

    def test_replacement_merge_recovers_without_reusing_reading(self):
        self.complete_section("s0000")
        identity = self.flow().section_chunks("s0000")[-1]
        data = self.payload(identity)
        data["units"][0]["text"] = "裡頭一個人也沒有。"
        self.save(identity, data, replace=True)
        original = core.atomic_text
        def fail_map(path, value):
            if path.name == "s0000.map.json":
                raise OSError("合成分節寫入中斷")
            return original(path, value)
        with patch.object(core, "atomic_text", side_effect=fail_map), self.assertRaises(OSError):
            self.merge(replace=True)
        self.merge(replace=True)
        self.assertEqual(self.flow().reading_records(), {})
        self.review()

    def test_manual_mapping_edit_is_not_overwritten_on_remerge(self):
        self.save_section("s0000")
        self.merge()
        path = self.work / "sections/s0000.map.json"
        mapping = core.load_json(path)
        mapping["reference_evidence"] = "人工新增的核對紀錄"
        write_json(path, mapping)
        with self.assertRaisesRegex(core.PipelineError, "人工修訂"):
            self.merge(replace=True)
        self.assertEqual(core.load_json(path)["reference_evidence"], mapping["reference_evidence"])

    def test_new_source_bytes_invalidate_old_inputs(self):
        self.source.write_text(SOURCE + "\n새로운 내용\n", encoding="utf-8")
        with self.assertRaisesRegex(core.PipelineError, "檔案已改變"):
            self.save(self.plan["chunks"][0]["id"])

    def test_writer_lock_prevents_concurrent_mutation(self):
        lock = self.work / ".writer.lock"
        lock.write_text("99999", encoding="utf-8")
        with self.assertRaisesRegex(core.PipelineError, "已鎖定"):
            self.save(self.plan["chunks"][0]["id"])
        self.assertEqual(lock.read_text(encoding="utf-8"), "99999")

    def test_excluded_preamble_requires_evidence_and_no_fake_score(self):
        source, reference = self.root / "前置韓文.txt", self.root / "前置人工.txt"
        source.write_text("https://example.invalid/store\n서장\n시작\n", encoding="utf-8")
        reference.write_text("序章", encoding="utf-8")
        work = self.root / "前置工作"
        core.init_project(argparse.Namespace(source=source, reference=reference, work=work, title="前置測試", source_encoding="auto", reference_encoding="auto", heading_pattern=None, source_layout="inline"))
        manifest = core.load_json(work / "manifest.json")
        plan = core.build_batch_plan(manifest["sections"], manifest["source"]["sha256"])
        write_json(work / "batch-plan.json", plan)
        write_rows(work / "continuity.jsonl", [{"section_id": s["id"], "summary": "合成前置狀態", "facts": []} for s in manifest["sections"]])
        payload = {"units": [{"source_ids": [manifest["sections"][0]["blocks"][0]["id"]], "disposition": "excluded", "reason": "advertisement", "note": "合成商店連結，經測試聲明排除。"}],
                   "reviews": synthetic_reviews(core.STAGES), "scorecard": {"method": "not_applicable", "reason": "合成廣告全數排除"},
                   "state_after": {"viewpoint": "前置非故事文字", "time_place": "不適用", "speakers": [], "known_facts": [], "open_threads": []}}
        path = self.root / "preamble-input.json"
        write_json(path, payload)
        chunk_workflow.run(core, argparse.Namespace(command="save-chunk", work=work, chunk=plan["chunks"][0]["id"], input=path, replace=False))
        result = chunk_workflow.run(core, argparse.Namespace(command="merge-section", work=work, section="s0000", title=None, replace=False))
        self.assertEqual((work / "sections/s0000.txt").read_bytes(), b"")
        self.assertEqual(result["status"], "awaiting_section_review")

    def test_cli_success_path_produces_readable_json(self):
        self.save_section("s0000")
        script = Path(core.__file__)
        merged = subprocess.run([sys.executable, "-X", "utf8", str(script), "merge-section", "--work", str(self.work), "--section", "s0000", "--title", "序章"], capture_output=True)
        self.assertEqual(merged.returncode, 0, merged.stderr)
        self.assertIn("state", json.loads(merged.stdout.decode("utf-8")))
        path = self.root / "cli-review.json"
        write_json(path, self.review_payload("s0000"))
        reviewed = subprocess.run([sys.executable, "-X", "utf8", str(script), "review-section", "--work", str(self.work), "--section", "s0000", "--input", str(path)], capture_output=True)
        self.assertEqual(reviewed.returncode, 0, reviewed.stderr)
        self.assertTrue(json.loads(reviewed.stdout.decode("utf-8"))["shared_book_reading"])

    def test_cli_uses_same_error_type_and_returns_structured_failure(self):
        path = self.root / "invalid.json"
        write_json(path, {"units": []})
        script = Path(core.__file__)
        result = subprocess.run([sys.executable, "-X", "utf8", str(script), "save-chunk", "--work", str(self.work), "--chunk", self.plan["chunks"][0]["id"], "--input", str(path)], capture_output=True, check=False)
        self.assertEqual(result.returncode, 2)
        self.assertIn("error", json.loads(result.stderr.decode("utf-8")))

    def test_interrupted_progress_write_cannot_claim_book_reading(self):
        self.save_section("s0000")
        self.merge()
        original = core.atomic_json
        def fail_progress(path, value):
            if path.name == "batch-progress.json":
                raise OSError("合成中斷")
            return original(path, value)
        with patch.object(core, "atomic_json", side_effect=fail_progress), self.assertRaises(OSError):
            self.review()
        self.assertEqual(self.flow().reading_records(), {})
        self.assertFalse((self.work / ".writer.lock").exists())

    def prepare(self, identity="s0000", terms=None, first=None, last=None):
        return section_context.prepare_section(core, argparse.Namespace(work=self.work, section=identity, term_id=terms or [], from_block=first, through_block=last))

    def test_chunk_saves_without_score_but_keeps_actual_checks(self):
        identity = self.plan["chunks"][0]["id"]
        data = self.payload(identity)
        data["reviews"]["referents"]["note"] = "合成核對：主詞為敘事者，已修正指代。"
        self.save(identity, data)
        mapping = core.load_json(self.work / "chunks" / (identity + ".map.json"))
        self.assertNotIn("scorecard", mapping)
        self.assertEqual(mapping["reviews"], data["reviews"])

    def test_legacy_chunk_score_is_optional_and_not_recomputed(self):
        identity = self.plan["chunks"][0]["id"]
        self.save(identity)
        path = self.work / "chunks" / (identity + ".map.json")
        mapping = core.load_json(path)
        mapping["scorecard"] = synthetic_scorecard(["s0000"])
        write_json(path, mapping)
        progress = core.load_json(self.work / "batch-progress.json")
        progress["chunks"][identity]["mapping_sha256"] = core.digest(path.read_bytes())
        write_json(self.work / "batch-progress.json", progress)
        with patch.object(core, "check_scorecard", side_effect=AssertionError("小段不需重評")):
            self.flow().checked_chunks([identity])

    def test_open_issue_blocks_chunk_even_if_old_input_has_high_score(self):
        identity = self.plan["chunks"][0]["id"]
        data = self.payload(identity)
        data["scorecard"] = synthetic_scorecard(["s0000"], 100)
        rows = [{"id": "issue-001", "status": "open", "section_ids": ["s0000"], "note": "合成測試：漏譯尚未修正。"}]
        write_rows(self.work / "reviews.jsonl", rows)
        with self.assertRaisesRegex(core.PipelineError, "未解決"):
            self.save(identity, data)
        rows[0].update(status="resolved", note="合成測試：已補回漏譯並逐段核對。")
        write_rows(self.work / "reviews.jsonl", rows)
        self.save(identity, data)
        self.assertEqual(core.load_rows(self.work / "reviews.jsonl"), rows)

    def test_section_still_needs_complete_score_at_least_85(self):
        self.save_section("s0000")
        self.merge()
        for score in (None, synthetic_scorecard(["s0000"], 84.99)):
            data = self.review_payload("s0000")
            data["scorecard"] = score
            with self.subTest(score=score), self.assertRaises(core.PipelineError):
                self.review(data=data)
        data = self.review_payload("s0000")
        data["scorecard"] = synthetic_scorecard(["s0000"], 85)
        self.assertEqual(self.review(data=data)["average_score"], 85)

    def test_bundle_collects_source_voice_style_and_previous_summary(self):
        self.terms[0]["voice_note"] = "合成依據：稱呼直接，句子簡短。"
        write_rows(self.work / "terminology.jsonl", self.terms)
        progress = self.flow().progress
        progress["style_profile"] = {"narrator_voice": "合成敘述口吻"}
        write_json(self.work / "batch-progress.json", progress)
        bundle = self.prepare("s0001")
        self.assertEqual(bundle["section"]["blocks"], self.manifest["sections"][1]["blocks"])
        self.assertEqual(bundle["terminology"], self.terms)
        self.assertEqual(bundle["character_voices"][0]["voice_note"], self.terms[0]["voice_note"])
        self.assertEqual(bundle["style_profile"], progress["style_profile"])
        self.assertEqual(bundle["previous_section"], self.contexts[0])
        self.assertEqual(bundle["cache"], "rebuilt")
        self.assertEqual(core.load_json(self.work / "batch-progress.json"), progress)

    def test_cache_reuse_skips_source_parsing_and_does_not_claim_reading(self):
        first = self.prepare()
        with patch.object(core, "parse_source", side_effect=AssertionError("快取命中不重做索引")):
            second = self.prepare()
        self.assertEqual(second["cache"], "reused")
        self.assertEqual(first["context_state"], second["context_state"])
        self.assertFalse((self.work / "batch-progress.json").exists())
        self.assertNotIn("receipt", core.load_json(self.work / "manifest.json")["sections"][0])

    def test_paged_bundle_keeps_ids_and_full_bundle_state(self):
        full = self.prepare()
        blocks = full["section"]["blocks"]
        pages = [self.prepare(first=b["id"], last=b["id"]) for b in blocks]
        self.assertEqual([p["section"]["blocks"][0] for p in pages], blocks)
        self.assertTrue(all(p["section"]["partial"] and p["context_state"] == full["context_state"] for p in pages))
        with self.assertRaises(core.PipelineError):
            self.prepare(first=blocks[0]["id"])

    def test_implicit_terms_are_explicit_and_bound_to_saved_chunk(self):
        bundle = self.prepare(terms=["person-001"])
        identity = self.plan["chunks"][0]["id"]
        data = self.payload(identity)
        data["context_state"] = bundle["context_state"]
        self.save(identity, data)
        mapping = core.load_json(self.work / "chunks" / (identity + ".map.json"))
        self.assertEqual(mapping["term_ids"], ["person-001"])
        self.assertEqual(self.prepare(terms=["person-001"])["cache"], "reused")
        with self.assertRaisesRegex(core.PipelineError, "不存在或超出"):
            self.prepare(terms=["unknown-person"])

    def test_stale_bundle_rejected_on_save_and_rebuilt_after_term_change(self):
        bundle = self.prepare("s0001")
        identity = self.flow().section_chunks("s0001")[0]
        data = self.payload(identity)
        data["context_state"] = bundle["context_state"]
        self.terms[0]["voice_note"] = "新核對的合成口吻。"
        write_rows(self.work / "terminology.jsonl", self.terms)
        with self.assertRaisesRegex(core.PipelineError, "資料包已失效"):
            self.save(identity, data)
        current = self.prepare("s0001")
        self.assertEqual(current["cache"], "rebuilt")
        self.assertNotEqual(current["context_state"], bundle["context_state"])
        with self.assertRaisesRegex(core.PipelineError, "資料包已失效"):
            self.save(identity, data)
        data["context_state"] = current["context_state"]
        self.save(identity, data)

    def test_prepared_bundle_cannot_be_silently_omitted_on_save(self):
        self.prepare()
        with self.assertRaisesRegex(core.PipelineError, "context_state"):
            self.save(self.plan["chunks"][0]["id"])

    def test_changed_source_or_reference_never_returns_cached_content(self):
        self.prepare()
        for path in (self.source, self.reference):
            original = path.read_bytes()
            path.write_bytes(original + "變動".encode("utf-8"))
            with self.subTest(path=path), self.assertRaisesRegex(core.PipelineError, "檔案已改變"):
                self.prepare()
            path.write_bytes(original)

    def test_changed_context_style_issues_and_plan_rebuild_bundle(self):
        original = self.prepare("s0001")
        self.contexts[0]["summary"] = "更新的合成前節摘要。"
        write_rows(self.work / "continuity.jsonl", self.contexts)
        changed = self.prepare("s0001")
        self.assertNotEqual(changed["context_state"], original["context_state"])
        progress = self.flow().progress
        progress["style_profile"] = {"dialogue_quotes": "「」"}
        write_json(self.work / "batch-progress.json", progress)
        self.assertEqual(self.prepare("s0001")["cache"], "rebuilt")
        write_rows(self.work / "reviews.jsonl", [{"id": "i1", "status": "resolved", "section_ids": ["s0001"], "note": "已修正的合成問題。"}])
        self.assertEqual(self.prepare("s0001")["cache"], "rebuilt")
        self.plan["note"] = "核對後的合成切分說明。"
        write_json(self.work / "batch-plan.json", self.plan)
        with self.assertRaisesRegex(core.PipelineError, "進度與來源"):
            self.prepare("s0001")
        progress["plan_sha256"] = core.digest((self.work / "batch-plan.json").read_bytes())
        write_json(self.work / "batch-progress.json", progress)
        self.assertEqual(self.prepare("s0001")["cache"], "rebuilt")

    def test_corrupted_cache_is_rebuilt_and_manifest_tampering_is_rejected(self):
        bundle = self.prepare()
        path = Path(bundle["path"])
        packet = core.load_json(path)
        packet["payload"]["section"]["blocks"].pop()
        write_json(path, packet)
        rebuilt = self.prepare()
        self.assertEqual(rebuilt["cache"], "rebuilt")
        self.assertEqual(rebuilt["section"]["blocks"], bundle["section"]["blocks"])
        manifest = core.load_json(self.work / "manifest.json")
        manifest["sections"][0]["blocks"].pop()
        write_json(self.work / "manifest.json", manifest)
        with self.assertRaisesRegex(core.PipelineError, "原稿索引"):
            self.prepare()

    def test_style_change_invalidates_saved_chunk_and_section_receipt(self):
        self.complete_section("s0000")
        progress = core.load_json(self.work / "batch-progress.json")
        progress["style_profile"] = {"narrator_voice": "已改變的敘述口吻。"}
        write_json(self.work / "batch-progress.json", progress)
        with self.assertRaisesRegex(core.PipelineError, "文體設定已變更"):
            self.merge(replace=True)
        self.assertEqual(self.flow().reading_records(), {})
        report = core.validate_project(core.load_project(self.work))
        self.assertIn("文體設定已變更", report["errors"][0]["error"])

    def test_cached_workflow_keeps_section_and_final_book_score_gates(self):
        for identity in ("s0000", "s0001", "s0002"):
            bundle = self.prepare(identity)
            for chunk in self.flow().section_chunks(identity):
                data = self.payload(chunk)
                data["context_state"] = bundle["context_state"]
                self.save(chunk, data)
            self.merge(identity)
            self.review(identity)
        state = core.book_state(core.load_project(self.work))
        path = self.work / "book-review.json"
        data = {"state": state, "reviews": synthetic_reviews(core.BOOK_STAGES), "scorecard": synthetic_scorecard(["s0000"], 84.99)}
        write_json(path, data)
        with self.assertRaisesRegex(core.PipelineError, "未達 85"):
            core.attest_book(argparse.Namespace(work=self.work))
        data["scorecard"] = synthetic_scorecard(["s0000"], 85)
        write_json(path, data)
        core.attest_book(argparse.Namespace(work=self.work))
        self.assertTrue((self.work / "book-receipt.json").exists())
        completed = self.root / "合成快取流程完整版.txt"
        core.assemble_book(argparse.Namespace(work=self.work, output=completed))
        self.assertIn("木真回來了。", completed.read_text(encoding="utf-8"))
        self.reference.write_text("交付前已改變的人工版。", encoding="utf-8")
        output = self.root / "不得交付.txt"
        with self.assertRaisesRegex(core.PipelineError, "檔案已改變"):
            core.assemble_book(argparse.Namespace(work=self.work, output=output))
        self.assertFalse(output.exists())

    def test_prepare_cli_exposes_bundle_and_reports_cache_hit(self):
        command = [sys.executable, "-X", "utf8", str(Path(core.__file__)), "prepare-section", "--work", str(self.work), "--section", "s0000"]
        for expected in ("rebuilt", "reused"):
            result = subprocess.run(command, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout.decode("utf-8"))["cache"], expected)

    def test_rule_revision_invalidates_prepared_bundle(self):
        self.prepare()
        language_rules = Path(core.__file__).resolve().parent.parent / "references/zh-tw-review-rules.json"
        original_read = Path.read_bytes
        def changed_rules(path):
            payload = original_read(path)
            return payload + b"\n" if path == language_rules else payload
        with patch.object(Path, "read_bytes", changed_rules):
            self.assertEqual(self.prepare()["cache"], "rebuilt")

    def test_inputs_changed_during_build_do_not_publish_mixed_cache(self):
        original_build = section_context.build_payload
        def changed_during_build(*args, **kwargs):
            payload = original_build(*args, **kwargs)
            self.contexts[0]["summary"] = "建立途中改動的合成摘要。"
            write_rows(self.work / "continuity.jsonl", self.contexts)
            return payload
        with patch.object(section_context, "build_payload", side_effect=changed_during_build), self.assertRaisesRegex(core.PipelineError, "準備期間依據變動"):
            self.prepare("s0001")
        self.assertFalse((self.work / "context/s0001.json").exists())

    def test_missing_previous_summary_and_pending_term_are_not_invented(self):
        write_rows(self.work / "continuity.jsonl", self.contexts[1:])
        with self.assertRaisesRegex(core.PipelineError, "缺少前一分節"):
            self.prepare("s0001")
        write_rows(self.work / "continuity.jsonl", self.contexts)
        self.terms[0]["decision"] = "pending"
        write_rows(self.work / "terminology.jsonl", self.terms)
        bundle = self.prepare("s0001")
        self.assertEqual(bundle["terminology"][0]["decision"], "pending")
        identity = self.flow().section_chunks("s0001")[0]
        data = self.payload(identity)
        data["context_state"] = bundle["context_state"]
        with self.assertRaisesRegex(core.PipelineError, "尚未決定"):
            self.save(identity, data)


    def prime_verification(self):
        flow = self.flow()
        records = flow.reading_records()
        flow.verification.flush(flow.manifest)
        return records

    def submit(self, chunk, data=None, merge=False, title=None, replace=False):
        path = self.root / "submit.json"
        write_json(path, self.payload(chunk) if data is None else data)
        return chunk_workflow.run(core, argparse.Namespace(command="submit-chunk", work=self.work, chunk=chunk,
                                  input=path, merge=merge, title=title, replace=replace))

    def test_range_and_explicit_ids_produce_identical_artifacts(self):
        flow = self.flow()
        explicit = [{"source_ids": ["a", "b"], "text": "「明天回來。」\n\n他離開了。",
                     "language_allowances": [{"text": "明天", "reason": "合成測試"}]},
                    {"source_ids": ["c"], "disposition": "excluded", "reason": "layout", "note": "已核對版面"}]
        ranged = [{**u, "source_range": [u["source_ids"][0], u["source_ids"][-1]]} for u in explicit]
        for unit in ranged:
            del unit["source_ids"]
        self.assertEqual(flow.render_units(explicit, ["a", "b", "c"]), flow.render_units(ranged, ["a", "b", "c"]))

    def test_invalid_and_overlapping_ranges_are_rejected(self):
        flow = self.flow()
        for span in (["b", "a"], ["a", "outside"], ["a"], "a:b", [1, 2]):
            with self.subTest(span=span), self.assertRaises(core.PipelineError):
                flow.render_units([{"source_range": span, "text": "完整文字。"}], ["a", "b"])
        for units in ([{"source_ids": ["a"], "source_range": ["a", "b"], "text": "文字。"}],
                      [{"source_range": ["a", "b"], "text": "文字。"}, {"source_range": ["b", "b"], "text": "重複。"}],
                      [{"source_range": ["b", "b"], "text": "缺漏。"}]):
            with self.subTest(units=units), self.assertRaises(core.PipelineError):
                flow.render_units(units, ["a", "b"])

    def test_content_chars_matches_inventory_including_decomposed_hangul(self):
        for value in ("한 글\r\n둘", "\u1100\u1161\n\nＡ ", "a\u2028b", ""):
            self.assertEqual(core.content_chars(value), core.text_metrics(value)["chars_with_spaces"])
        with patch.object(core, "text_metrics", side_effect=AssertionError("不得計算無用統計")):
            self.flow()

    def test_warm_source_and_plan_skip_reparse_and_chunk_hash_checks(self):
        flow = self.flow()
        flow.verification.flush(flow.manifest)
        with (patch.object(core, "parse_source", side_effect=AssertionError("不重解析")),
              patch.object(core, "content_chars", side_effect=AssertionError("不重計字"))):
            warm = self.flow()
        self.assertEqual(flow.views, warm.views)

    def test_cache_corruption_falls_back_and_index_tamper_never_passes(self):
        flow = self.flow()
        flow.verification.flush(flow.manifest)
        flow.verification.path.write_text("{bad", encoding="utf-8")
        with patch.object(core, "parse_source", wraps=core.parse_source) as parsing:
            self.flow()
        self.assertEqual(parsing.call_count, 1)
        self.manifest["sections"][0]["blocks"][0]["text"] = "變造原文"
        write_json(self.work / "manifest.json", self.manifest)
        with self.assertRaisesRegex(core.PipelineError, "原稿索引"):
            self.flow()

    def test_warm_plan_change_is_revalidated(self):
        flow = self.flow()
        flow.verification.flush(flow.manifest)
        self.plan["chunks"][0]["chars"] += 1
        write_json(self.work / "batch-plan.json", self.plan)
        with self.assertRaisesRegex(core.PipelineError, "小段來源指紋"):
            self.flow()

    def test_warm_source_and_reference_hashes_ignore_mtime_shortcuts(self):
        import os
        flow = self.flow()
        flow.verification.flush(flow.manifest)
        for path in (self.source, self.reference):
            raw, stat = path.read_bytes(), path.stat()
            path.write_bytes(raw.replace("木".encode(), "林".encode()) if path == self.reference else raw.replace("목".encode(), "문".encode()))
            os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
            with self.subTest(path=path), self.assertRaisesRegex(core.PipelineError, "檔案已改變"):
                self.flow()
            path.write_bytes(raw)

    def test_unchanged_reading_reuses_mechanical_validation(self):
        self.complete_section("s0000")
        expected = self.prime_verification()
        with patch.object(core, "check_section", side_effect=AssertionError("有效歷史不重查")):
            self.assertEqual(self.flow().reading_records(), expected)

    def test_unrelated_term_issue_and_later_context_keep_prior_receipt_cache(self):
        self.complete_section("s0000")
        expected = self.prime_verification()
        self.terms.append({"id": "unrelated", "ko": ["전혀다른이름"], "zh": "無關名詞", "decision": "adopted", "basis": "test", "evidence": ["合成"]})
        write_rows(self.work / "terminology.jsonl", self.terms)
        write_rows(self.work / "reviews.jsonl", [{"id": "later", "section_ids": ["s0002"], "status": "open", "note": "後節問題"}])
        self.contexts[-1]["summary"] = "後節才發生的事情"
        write_rows(self.work / "continuity.jsonl", self.contexts)
        with patch.object(core, "check_section", side_effect=AssertionError("無關範圍不重查")):
            self.assertEqual(self.flow().reading_records(), expected)

    def test_new_matching_term_invalidates_prior_reading_cache(self):
        self.complete_section("s0000")
        self.prime_verification()
        original = self.manifest["sections"][0]["blocks"][0]["text"]
        self.terms.append({"id": "new-hit", "ko": [original], "zh": "新增命中", "decision": "adopted", "basis": "test", "evidence": ["合成"]})
        write_rows(self.work / "terminology.jsonl", self.terms)
        self.assertEqual(self.flow().reading_records(), {})

    def test_prior_receipt_cache_rechecks_translation_mapping_context_and_issues(self):
        self.complete_section("s0000")
        expected = self.prime_verification()
        for path, mutate in (
            (self.work / "sections/s0000.txt", lambda b: b + "新增。".encode()),
            (self.work / "sections/s0000.map.json", lambda b: b.replace(b'"passed"', b'"failed"')),
            (self.work / "continuity.jsonl", lambda b: b.replace("敘事".encode(), "修改".encode())),
            (self.work / "reviews.jsonl", lambda b: (core.canonical({"id": "block", "section_ids": [], "status": "open", "note": "阻塞"}) + "\n").encode()),
        ):
            raw = path.read_bytes()
            path.write_bytes(mutate(raw))
            with self.subTest(path=path):
                self.assertEqual(self.flow().reading_records(), {})
            path.write_bytes(raw)
        self.assertEqual(self.flow().reading_records(), expected)

    def test_declared_implicit_term_evidence_and_style_invalidate_cached_reading(self):
        for identity in self.flow().section_chunks("s0000"):
            data = self.payload(identity)
            data["term_ids"] = ["person-001"]
            self.save(identity, data)
        self.merge()
        self.review()
        self.prime_verification()
        old_terms = deepcopy(self.terms)
        self.terms[0]["evidence"] = ["改過的稱呼依據"]
        write_rows(self.work / "terminology.jsonl", self.terms)
        self.assertEqual(self.flow().reading_records(), {})
        write_rows(self.work / "terminology.jsonl", old_terms)
        progress = core.load_json(self.work / "batch-progress.json")
        progress["style_profile"] = {"voice": "新口吻"}
        write_json(self.work / "batch-progress.json", progress)
        self.assertEqual(self.flow().reading_records(), {})

    def test_rule_change_clears_verification_proofs(self):
        self.complete_section("s0000")
        self.prime_verification()
        from verification_cache import rule_fingerprints
        with (patch("verification_cache.rule_fingerprints", return_value={**rule_fingerprints(core), "change": "new"}),
             patch.object(core, "parse_source", wraps=core.parse_source) as parsing,
              patch.object(core, "check_section", wraps=core.check_section) as checking):
            self.flow().reading_records()
        self.assertEqual(parsing.call_count, 1)
        self.assertGreater(checking.call_count, 0)

    def test_prepare_and_save_bundle_use_one_project_load_each(self):
        with patch.object(core, "load_project", wraps=core.load_project) as loading:
            bundle = self.prepare()
        self.assertEqual(loading.call_count, 1)
        identity = self.plan["chunks"][0]["id"]
        data = self.payload(identity)
        data["context_state"] = bundle["context_state"]
        with patch.object(core, "load_project", wraps=core.load_project) as loading:
            self.save(identity, data)
        self.assertEqual(loading.call_count, 1)

    def test_review_scans_history_only_once(self):
        self.complete_section("s0000")
        self.save_section("s0001")
        self.merge("s0001")
        data = self.review_payload("s0001")
        original = chunk_workflow.Workflow.reading_records
        calls = []
        def tracked(flow):
            calls.append(flow)
            return original(flow)
        with patch.object(chunk_workflow.Workflow, "reading_records", tracked):
            self.review("s0001", data)
        self.assertEqual(len(calls), 1)

    def test_submit_saves_input_and_returns_next_without_fabricating_review(self):
        ids = self.flow().section_chunks("s0000")
        result = self.submit(ids[0])
        self.assertEqual(core.load_json(Path(result["input"])), self.payload(ids[0]))
        self.assertEqual(result["next"]["action"], "translate_chunk")
        self.assertEqual(result["next"]["chunk_id"], ids[1])
        self.assertNotIn("receipt", self.flow().manifest["sections"][0])

    def test_submit_merges_with_one_load_and_does_not_attest_section(self):
        ids = self.flow().section_chunks("s0000")
        for identity in ids[:-1]:
            self.submit(identity)
        data = self.payload(ids[-1])
        with patch.object(core, "load_project", wraps=core.load_project) as loading:
            result = self.submit(ids[-1], data, merge=True, title="序章")
        self.assertEqual(loading.call_count, 1)
        self.assertEqual(result["next"]["action"], "read_section")
        mapping = core.load_json(self.work / "sections/s0000.map.json")
        self.assertEqual(mapping["reviews"], {})
        self.assertEqual(mapping["scorecard"], {})
        self.assertNotIn("receipt", self.flow().manifest["sections"][0])
        self.assertEqual(self.flow().reading_records(), {})

    def test_submit_retry_keeps_reviewed_section_and_handles_partial_merge_failure(self):
        ids = self.flow().section_chunks("s0000")
        for identity in ids:
            result = self.submit(identity, merge=True)
        self.assertEqual(result["next"]["action"], "resolve_merge")
        self.assertTrue((self.work / "chunks" / (ids[-1] + ".input.json")).exists())
        result = self.submit(ids[-1], merge=True, title="序章")
        self.assertEqual(result["next"]["action"], "read_section")
        self.review()
        before = (self.work / "sections/s0000.map.json").read_bytes()
        self.assertEqual(self.submit(ids[-1], merge=True, title="序章")["next"]["action"], "section_already_reviewed")
        self.assertEqual((self.work / "sections/s0000.map.json").read_bytes(), before)

    def test_submit_does_not_overwrite_unrelated_input_draft_or_external_translation(self):
        identity = self.plan["chunks"][0]["id"]
        self.submit(identity)
        path = self.work / "chunks" / (identity + ".input.json")
        path.write_text('{"draft": "另一份草稿"}', encoding="utf-8")
        with self.assertRaisesRegex(core.PipelineError, "輸入草稿"):
            self.submit(identity)
        text = self.work / "chunks" / (identity + ".txt")
        text.write_text("外部修訂。", encoding="utf-8")
        with self.assertRaisesRegex(core.PipelineError, "外部修改"):
            self.submit(identity, replace=True)

    def test_submit_cli_stdin_accepts_unicode_and_source_range(self):
        identity = self.plan["chunks"][0]["id"]
        data = self.payload(identity)
        for unit in data["units"]:
            ids = unit.pop("source_ids")
            unit["source_range"] = [ids[0], ids[-1]]
        result = subprocess.run([sys.executable, "-X", "utf8", str(Path(core.__file__)), "submit-chunk",
                                 "--work", str(self.work), "--chunk", identity, "--input", "-"],
                                input=core.json_text(data).encode("utf-8"), capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(result.stdout.decode("utf-8"))
        self.assertEqual(core.load_json(Path(output["input"])), data)
        self.assertNotIn("\ufffd", Path(output["translation"]).read_text(encoding="utf-8"))

    def test_review_file_input_saves_canonical_copy_and_keeps_it_on_rejection(self):
        self.save_section("s0000")
        self.merge()
        data = self.review_payload("s0000")
        incoming = self.root / "散落的複核輸入.json"
        write_json(incoming, data)
        original = incoming.read_bytes()
        result = self.invoke("review-section", section="s0000", input=incoming)
        canonical = self.work / "sections/s0000.review.json"
        self.assertTrue(canonical.is_file())
        self.assertEqual(core.load_json(canonical), data)
        self.assertEqual(Path(result["input"]).resolve(), canonical.resolve())
        self.assertEqual(incoming.read_bytes(), original)

        accepted = canonical.read_bytes()
        invalid = self.review_payload("s0000")
        invalid["scorecard"]["items"] = {}
        pending = self.work / "drafts/s0000.review.input.json"
        pending.parent.mkdir()
        write_json(pending, invalid)
        with self.assertRaises(core.PipelineError):
            self.invoke("review-section", section="s0000", input=pending)
        self.assertEqual(canonical.read_bytes(), accepted)
        self.assertEqual(core.load_json(pending), invalid)

    def test_review_stdin_saves_actual_review_and_rejects_missing_scores(self):
        self.save_section("s0000")
        self.merge()
        data = self.review_payload("s0000")
        command = [sys.executable, "-X", "utf8", str(Path(core.__file__)), "review-section",
                   "--work", str(self.work), "--section", "s0000", "--input", "-"]
        invalid = {**data, "scorecard": {}}
        result = subprocess.run(command, input=core.json_text(invalid).encode("utf-8"), capture_output=True)
        self.assertEqual(result.returncode, 2)
        self.assertFalse((self.work / "sections/s0000.review.json").exists())
        result = subprocess.run(command, input=core.json_text(data).encode("utf-8"), capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(core.load_json(self.work / "sections/s0000.review.json"), data)
        self.assertTrue(self.flow().reading_records())

    def test_full_delivery_validation_never_uses_incremental_cache(self):
        for section in self.manifest["sections"]:
            self.complete_section(section["id"])
        self.prime_verification()
        with (patch("verification_cache.VerificationCache.get", side_effect=AssertionError("最終驗收不可用快取")),
              patch.object(core, "parse_source", wraps=core.parse_source) as parsing):
            state = core.book_state(core.load_project(self.work))
            write_json(self.work / "book-review.json", {"state": state, "reviews": synthetic_reviews(core.BOOK_STAGES),
                                                       "scorecard": synthetic_scorecard(["s0000"])})
            core.attest_book(argparse.Namespace(work=self.work))
            output = self.root / "唯一最終版.txt"
            core.assemble_book(argparse.Namespace(work=self.work, output=output))
        self.assertGreater(parsing.call_count, 0)
        self.assertTrue(output.exists())


    def compact_review(self, identity="s0000"):
        data = self.review_payload(identity)
        data["format"] = "compact-review-v1"
        for name in ("reviews", "book_reviews"):
            data[name] = {key: [row["status"], [identity], row["note"]] for key, row in data[name].items()}
        data["scorecard"]["items"] = {key: [row["score"], row["evidence"], row["note"]]
                                      for key, row in data["scorecard"]["items"].items()}
        return data

    def review_and_prepare(self, identity="s0000", data=None, terms=()):
        path = self.root / "connected-review.json"
        write_json(path, self.review_payload(identity) if data is None else data)
        return self.invoke("review-section", section=identity, input=path, prepare_next=True, term_id=list(terms))

    def daily_next(self, prepare=False):
        return section_context.next_section(core, argparse.Namespace(work=self.work, prepare=prepare, term_id=[]))

    def test_merge_returns_complete_reading_bound_to_snapshot_without_attestation(self):
        self.save_section("s0000")
        result = self.merge()
        path = self.work / "sections/s0000.txt"
        self.assertEqual(result["reading"]["text"].encode(), path.read_bytes())
        self.assertEqual(result["reading"]["translation_sha256"], core.digest(path.read_bytes()))
        self.assertTrue(result["reading"]["complete"])
        self.assertEqual(result["reading"]["line_count"], 5)
        self.assertEqual(result["state"], self.invoke("review-state", section="s0000")["state"])
        self.assertEqual(self.flow().reading_records(), {})

    def test_reading_rejects_version_change_during_output(self):
        self.save_section("s0000")
        self.merge()
        flow = self.flow()
        snapshot = flow.review_state("s0000")
        with patch.object(flow, "review_state", side_effect=[snapshot, {**snapshot, "state": "changed"}]), self.assertRaisesRegex(core.PipelineError, "讀稿期間版本"):
            flow.read_section("s0000")

    def test_review_prepares_next_with_one_project_load_one_history_scan_and_one_section_check(self):
        self.save_section("s0000")
        self.merge()
        data = self.review_payload("s0000")
        path = self.root / "connected-review.json"
        write_json(path, data)
        with (patch.object(core, "load_project", wraps=core.load_project) as loading,
              patch.object(core, "check_section", wraps=core.check_section) as checking,
              patch.object(chunk_workflow.Workflow, "reading_records", autospec=True, side_effect=chunk_workflow.Workflow.reading_records) as reading):
            result = self.invoke("review-section", section="s0000", input=path, prepare_next=True, term_id=[])
        self.assertEqual(loading.call_count, 1)
        self.assertEqual(reading.call_count, 1)
        # 小段仍各自核對；本節的完整檢查不因接續取稿而重做。
        current_checks = [call for call in checking.call_args_list if call.args[1]["id"] == "s0000" and not call.kwargs.get("chunk")]
        self.assertEqual(len(current_checks), 1)
        self.assertEqual(result["next"]["section_id"], "s0001")
        bundle = result["next"]["context"]
        self.assertEqual(bundle["section"]["blocks"], self.manifest["sections"][1]["blocks"])
        self.assertEqual(bundle["previous_section"], self.contexts[0])
        chunk = bundle["chunks"][0]["id"]
        payload = self.payload(chunk)
        payload["context_state"] = bundle["context_state"]
        self.save(chunk, payload)

    def test_next_warm_prefix_reuses_validated_sections_and_stops_at_first_gap(self):
        self.complete_section("s0000")
        self.prime_verification()
        with patch.object(core, "check_section", wraps=core.check_section) as checking:
            result = self.daily_next()
        self.assertEqual(result["next"], "s0001")
        self.assertEqual(result["verified"], 1)
        self.assertEqual(result["validation_scope"], "verified_prefix")
        self.assertFalse(any(call.args[1]["id"] in ("s0000", "s0002") for call in checking.call_args_list))

    def test_next_detects_earlier_translation_change_even_with_same_mtime(self):
        import os
        self.complete_section("s0000")
        self.prime_verification()
        path = self.work / "sections/s0000.txt"
        stat = path.stat()
        path.write_bytes(path.read_bytes().replace("我".encode(), "他".encode()))
        os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        self.assertEqual(self.daily_next()["next"], "s0000")
        self.assertEqual(self.daily_next(prepare=True)["section_id"], "s0000")

    def test_next_detects_removed_receipt_and_changed_dependencies(self):
        self.complete_section("s0000")
        self.prime_verification()
        path = self.work / "manifest.json"
        manifest = core.load_json(path)
        changed = deepcopy(manifest)
        changed["sections"][0].pop("receipt")
        write_json(path, changed)
        self.assertEqual(self.daily_next()["next"], "s0000")
        write_json(path, manifest)
        self.contexts[0]["facts"].append("合成的新敘事依據")
        write_rows(self.work / "continuity.jsonl", self.contexts)
        self.assertEqual(self.daily_next()["next"], "s0000")

    def test_next_rechecks_title_consistency_even_when_receipts_are_cached(self):
        for identity in ("s0000", "s0001", "s0002"):
            self.complete_section(identity)
        text_path = self.work / "sections/s0002.txt"
        map_path = self.work / "sections/s0002.map.json"
        text = text_path.read_text(encoding="utf-8").replace("約定", "承諾")
        mapping = core.load_json(map_path)
        mapping["title_zh"] = mapping["title_zh"].replace("約定", "承諾")
        core.atomic_text(text_path, text)
        mapping["translation_sha256"] = core.digest(text)
        core.atomic_json(map_path, mapping)
        core.record_section(argparse.Namespace(work=self.work, section="s0002"))
        self.daily_next()
        with patch.object(core, "check_section", side_effect=AssertionError("已核對收據不重查")):
            result = self.daily_next()
        self.assertEqual(result["next"], "s0002")
        self.assertIn("章名", result["errors"][0]["error"])

    def test_review_without_book_checks_returns_current_reading_gap(self):
        self.save_section("s0000")
        self.merge()
        data = self.review_payload("s0000")
        data.pop("book_reviews")
        result = self.review_and_prepare(data=data)
        self.assertFalse(result["shared_book_reading"])
        self.assertEqual(result["next"]["section_id"], "s0000")
        self.assertTrue(result["next"]["book_reading_pending"])
        self.assertIn("reading", result["next"]["section"])
        self.assertEqual(self.flow().reading_records(), {})

    def test_prepared_navigation_does_not_skip_earlier_missing_book_record(self):
        self.complete_section("s0000")
        self.complete_section("s0001")
        path = self.work / "batch-progress.json"
        progress = core.load_json(path)
        progress["book_review"]["completed_batches"] = [r for r in progress["book_review"]["completed_batches"] if r["section_id"] != "s0000"]
        write_json(path, progress)
        result = self.daily_next(prepare=True)
        self.assertEqual(result["section_id"], "s0000")
        self.assertIn("reading", result["section"])

    def test_next_preparation_failure_keeps_saved_review_and_resume_instructions(self):
        self.save_section("s0000")
        self.merge()
        result = self.review_and_prepare(terms=["unknown-term"])
        self.assertEqual(result["recorded"], "s0000")
        self.assertEqual(result["next"]["action"], "resolve_next")
        self.assertIn("receipt", self.flow().manifest["sections"][0])
        self.assertTrue(self.flow().reading_records())
        self.assertEqual(self.daily_next(prepare=True)["section_id"], "s0001")

    def test_last_review_only_routes_to_final_review_not_book_attestation(self):
        for identity in ("s0000", "s0001"):
            self.complete_section(identity)
        self.save_section("s0002")
        self.merge("s0002")
        result = self.review_and_prepare("s0002")
        self.assertEqual(result["next"]["action"], "final_review")
        self.assertFalse((self.work / "book-receipt.json").exists())

    def test_compact_review_expands_and_preserves_notes_evidence_scores_and_issues(self):
        self.save_section("s0000")
        self.merge()
        rows = [{"id": "fixed", "status": "resolved", "section_ids": ["s0000"], "note": "合成案例：已依說話者修正代詞。"}]
        write_rows(self.work / "reviews.jsonl", rows)
        original_issues = (self.work / "reviews.jsonl").read_bytes()
        data = self.compact_review()
        data["reviews"]["referents"] = ["passed", ["s0000:p0001"], "合成核對：第一人稱敘事者開門，沒有其他說話者。"]
        result = self.review_and_prepare(data=data)
        stored = core.load_json(self.work / "sections/s0000.review.json")
        self.assertNotIn("format", stored)
        self.assertEqual(stored["reviews"]["referents"], {"status": "passed", "evidence": ["s0000:p0001"], "note": data["reviews"]["referents"][2]})
        self.assertEqual(stored["scorecard"], synthetic_scorecard(["s0000"]))
        mapping = core.load_json(self.work / "sections/s0000.map.json")
        self.assertEqual(mapping["reviews"], stored["reviews"])
        self.assertEqual((self.work / "reviews.jsonl").read_bytes(), original_issues)
        self.assertEqual(result["average_score"], 90)
        self.assertEqual(result["next"]["section_id"], "s0001")

    def test_compact_rejects_missing_or_invented_evidence_and_never_fills_judgments(self):
        self.save_section("s0000")
        self.merge()
        original = self.compact_review()
        variants = []
        for key, value in (("format", "unknown"), ("book_reviews", {})):
            variants.append({**deepcopy(original), key: value})
        for row in (["passed", [], "有結論"], ["passed", ["unknown"], "有結論"], ["passed", ["s0000"], ""],
                    ["passed", ["s0000"]], ["failed", ["s0000"], "主詞尚待修正"]):
            data = deepcopy(original)
            data["reviews"]["referents"] = row
            variants.append(data)
        data = deepcopy(original)
        data["scorecard"]["items"].pop("emotion")
        variants.append(data)
        for score in (84.99, True, "90"):
            data = deepcopy(original)
            for row in data["scorecard"]["items"].values():
                row[0] = score
            variants.append(data)
        for data in variants:
            with self.subTest(data=data), self.assertRaises(core.PipelineError):
                self.review(data=data)
        self.assertNotIn("receipt", self.flow().manifest["sections"][0])
        self.assertFalse((self.work / "sections/s0000.review.json").exists())

    def test_compact_still_rejects_stale_version_and_prior_reading_gap(self):
        self.save_section("s0001")
        self.merge("s0001")
        data = self.compact_review("s0001")
        with self.assertRaisesRegex(core.PipelineError, "前序全書閱讀"):
            self.review("s0001", data)
        data["state"] = "stale"
        with self.assertRaisesRegex(core.PipelineError, "版本已改變"):
            self.review("s0001", data)

    def test_connected_cli_supports_compact_stdin_and_next_prepare(self):
        self.save_section("s0000")
        self.merge()
        data = self.compact_review()
        command = [sys.executable, "-X", "utf8", str(Path(core.__file__)), "review-section", "--work", str(self.work),
                   "--section", "s0000", "--input", "-", "--prepare-next"]
        result = subprocess.run(command, input=json.dumps(data, ensure_ascii=False).encode(), capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        output = json.loads(result.stdout)
        self.assertEqual(output["next"]["section_id"], "s0001")
        result = subprocess.run([sys.executable, "-X", "utf8", str(Path(core.__file__)), "next", "--work", str(self.work), "--prepare"], capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(json.loads(result.stdout)["context"]["context_state"], output["next"]["context"]["context_state"])

    def test_connected_packed_source_workflow_reaches_full_delivery_with_real_inputs(self):
        import source_store
        source_store.migrate(core, argparse.Namespace(work=self.work, command="migrate-source", apply=True, checkpoint=True))
        next_step = self.daily_next(prepare=True)
        for identity in ("s0000", "s0001", "s0002"):
            self.assertEqual(next_step["section_id"], identity)
            bundle = next_step["context"]
            disk_bundle = core.load_json(Path(bundle["path"]))
            self.assertNotIn("blocks", disk_bundle["payload"]["section"])
            self.assertEqual(bundle["section"]["blocks"], core.section_by_id(self.manifest, identity)["blocks"])
            for chunk in bundle["chunks"]:
                data = self.payload(chunk["id"])
                data["context_state"] = bundle["context_state"]
                self.save(chunk["id"], data)
            merged = self.merge(identity)
            self.assertEqual(merged["reading"]["text"], (self.work / "sections" / (identity + ".txt")).read_text(encoding="utf-8"))
            next_step = self.review_and_prepare(identity, self.compact_review(identity))["next"]
        self.assertEqual(next_step["action"], "final_review")
        with patch("verification_cache.VerificationCache.get", side_effect=AssertionError("交付不能使用快取")):
            self.assertFalse(core.validate_project(core.load_project(self.work))["errors"])
            state = core.book_state(core.load_project(self.work))
            write_json(self.work / "book-review.json", {"state": state, "reviews": synthetic_reviews(core.BOOK_STAGES),
                                                       "scorecard": synthetic_scorecard(["s0000", "s0001", "s0002"])})
            core.attest_book(argparse.Namespace(work=self.work))
            output = self.root / "合成連續作業最終版.txt"
            core.assemble_book(argparse.Namespace(work=self.work, output=output))
        self.assertIn("木真回來了。", output.read_text(encoding="utf-8"))

    def test_warm_prepare_rechecks_external_dependency_change_in_same_operation(self):
        self.prepare("s0001")
        flow = self.flow()
        self.contexts[0]["summary"] = "資料包載入後遭外部修改。"
        write_rows(self.work / "continuity.jsonl", self.contexts)
        with self.assertRaisesRegex(core.PipelineError, "準備期間依據變動"):
            section_context.prepare_with_flow(core, flow, "s0001")

    def test_next_still_checks_source_reference_and_recovers_from_corrupt_cache(self):
        self.complete_section("s0000")
        self.daily_next()
        (self.work / "context/verification-cache.json").write_text("broken", encoding="utf-8")
        with patch.object(core, "check_section", wraps=core.check_section) as checking:
            self.assertEqual(self.daily_next()["next"], "s0001")
        self.assertTrue(any(call.args[1]["id"] == "s0000" for call in checking.call_args_list))
        for path in (self.source, self.reference):
            raw = path.read_bytes()
            path.write_bytes(raw + "\n修改".encode())
            with self.subTest(path=path), self.assertRaisesRegex(core.PipelineError, "檔案已改變"):
                self.daily_next(prepare=True)
            path.write_bytes(raw)

    def test_compact_not_applicable_stays_explicit_and_never_becomes_a_story_score(self):
        self.save_section("s0000")
        self.merge()
        data = self.compact_review()
        data["scorecard"] = {"method": "not_applicable", "reason": "合成排除聲明"}
        section = core.load_section(self.flow().project, "s0000")
        expanded = chunk_workflow.expand_review(core, data, section, self.manifest)
        self.assertEqual(expanded["scorecard"], data["scorecard"])
        self.assertEqual(core.check_scorecard(expanded["scorecard"], {"s0000"}, excluded_preamble=True)["average"], None)
        with self.assertRaises(core.PipelineError):
            self.review(data=data)


    def establish_prior_terms(self):
        """合成前節明列隱含詞條，供測試後節的既用版本呈現。"""
        write_rows(self.work / "terminology.jsonl", self.terms)
        for chunk in self.flow().section_chunks("s0000"):
            data = self.payload(chunk)
            data["term_ids"] = [term["id"] for term in self.terms]
            self.save(chunk, data)
        self.merge()
        self.review()

    def details(self, bundle, *ids):
        return section_context.term_details(core, argparse.Namespace(work=self.work, section=bundle["section"]["id"],
                                            context_state=bundle["context_state"], term_id=list(ids)))

    def test_compact_terms_keep_source_identity_scope_voice_and_all_non_evidence_fields(self):
        term = self.terms[0]
        term.update(entity_id="person-001", category="person", definition="故事中的劍客。", voice_note="句子簡短直接。",
                    address_rule="對師父使用尊稱。", name_parts=[{"ko": "목", "zh": "木"}],
                    evidence=[{"source": "reference", "line": n, "note": "合成詞條採用依據。"} for n in range(60)])
        self.establish_prior_terms()
        before = {name: (self.work / name).read_bytes() for name in ("terminology.jsonl", "manifest.json", "batch-progress.json")}
        compact = self.prepare("s0001")
        self.assertEqual(compact["section"]["blocks"], self.manifest["sections"][1]["blocks"])
        row = compact["terminology"][0]
        self.assertNotIn("evidence", row)
        for key, value in term.items():
            if key != "evidence":
                self.assertEqual(row[key], value)
        self.assertEqual(row["scope"], {"from": 0, "through": 2})
        self.assertEqual(row["evidence_ref"], term["id"])
        self.assertEqual(compact["term_attention"], [])
        self.assertNotIn("evidence", compact["character_voices"][0])
        self.assertEqual(core.load_json(Path(compact["path"]))["payload"]["terminology"], self.terms)
        full = section_context.prepare_section(core, argparse.Namespace(work=self.work, section="s0001", term_id=[],
                                                from_block=None, through_block=None, term_detail="full"))
        self.assertEqual(full["terminology"], self.terms)
        self.assertEqual(full["context_state"], compact["context_state"])
        self.assertEqual(full["section"], compact["section"])
        self.assertLess(len(core.canonical(compact)), len(core.canonical(full)))
        fetched = self.details(compact, "person-001")
        self.assertEqual(fetched["terminology"], self.terms)
        self.assertEqual(fetched["term_sha256"]["person-001"], core.digest(core.canonical(term)))
        for name, raw in before.items():
            self.assertEqual((self.work / name).read_bytes(), raw)

    def test_first_use_pending_and_changed_terms_keep_complete_evidence_and_alerts(self):
        first = self.prepare("s0001")
        self.assertEqual(first["terminology"], self.terms)
        self.assertIn("new_or_changed", first["term_attention"][0]["reasons"])
        self.terms[0]["decision"] = "pending"
        write_rows(self.work / "terminology.jsonl", self.terms)
        pending = self.prepare("s0001")
        self.assertEqual(pending["terminology"], self.terms)
        self.assertIn("pending", pending["term_attention"][0]["reasons"])
        data = self.payload(pending["chunks"][0]["id"])
        data["context_state"] = pending["context_state"]
        with self.assertRaisesRegex(core.PipelineError, "尚未決定"):
            self.save(pending["chunks"][0]["id"], data)

    def test_evidence_revision_is_never_hidden_as_an_unchanged_adopted_term(self):
        self.establish_prior_terms()
        original = self.prepare("s0001")
        self.assertNotIn("evidence", original["terminology"][0])
        self.terms[0]["evidence"] = ["合成案例的新核對依據。"]
        write_rows(self.work / "terminology.jsonl", self.terms)
        with self.assertRaisesRegex(core.PipelineError, "資料包已失效"):
            self.details(original, "person-001")
        current = self.prepare("s0001")
        self.assertIn("new_or_changed", current["term_attention"][0]["reasons"])
        self.assertEqual(current["terminology"][0]["evidence"], self.terms[0]["evidence"])
        self.assertNotEqual(original["context_state"], current["context_state"])
        with self.assertRaisesRegex(core.PipelineError, "尚未 record"):
            self.flow().verification.checked_section(self.flow().project, core.load_json(self.work / "manifest.json")["sections"][0], {})

    def test_shared_forms_and_distinct_entities_with_same_translation_are_prominent(self):
        second = {**deepcopy(self.terms[0]), "id": "person-002", "entity_id": "person-002"}
        self.terms[0]["entity_id"] = "person-001"
        self.terms.append(second)
        self.establish_prior_terms()
        packet = self.prepare("s0001")
        self.assertEqual(packet["terminology"], self.terms)
        self.assertEqual(len(packet["term_attention"]), 2)
        for alert in packet["term_attention"]:
            self.assertIn("shared_form", alert["reasons"])
            self.assertIn("same_translation", alert["reasons"])
            self.assertEqual(len(alert["related_term_ids"]), 1)

    def test_implicit_aliases_include_shared_form_candidates_within_current_scope_only(self):
        self.terms[0].update(ko=["다른이름"], entity_id="person-001")
        self.terms += [
            {**deepcopy(self.terms[0]), "id": "alias", "ko": ["별호"], "zh": "劍客"},
            {**deepcopy(self.terms[0]), "id": "other-sense", "entity_id": "other", "ko": ["별호"], "zh": "稱號"},
            {**deepcopy(self.terms[0]), "id": "future", "entity_id": "future", "scope": {"from": 2, "through": 2}},
        ]
        write_rows(self.work / "terminology.jsonl", self.terms)
        packet = self.prepare("s0001", terms=["person-001"])
        self.assertEqual({row["id"] for row in packet["terminology"]}, {"person-001", "alias", "other-sense"})
        self.assertNotIn("future", {alert["term_id"] for alert in packet["term_attention"]})
        for alert in packet["term_attention"]:
            if alert["term_id"] in ("alias", "other-sense"):
                self.assertIn("shared_form", alert["reasons"])

    def test_explicit_polysemy_and_legacy_evidence_caution_are_never_folded(self):
        self.terms[0]["attention"] = [{"kind": "polysemy", "note": "人物稱呼需要依本節說話者判讀。"}]
        self.terms[0]["evidence"] = ["合成證據：此處不是姓氏，不可套用另一人物的譯名。"]
        self.establish_prior_terms()
        packet = self.prepare("s0001")
        self.assertEqual(packet["terminology"], self.terms)
        self.assertIn("recorded_attention", packet["term_attention"][0]["reasons"])
        self.assertIn("text_caution", packet["term_attention"][0]["reasons"])

    def test_open_issue_stays_visible_and_keeps_full_evidence(self):
        self.establish_prior_terms()
        issue = {"id": "ambiguity", "status": "open", "section_ids": ["s0001"], "note": "合成角色指代尚待確認。"}
        write_rows(self.work / "reviews.jsonl", [issue])
        packet = self.prepare("s0001")
        self.assertEqual(packet["issues"], [issue])
        self.assertEqual(packet["terminology"], self.terms)
        self.assertIn("open_issue", packet["term_attention"][0]["reasons"])

    def test_term_details_rejects_unknown_terms_stale_state_and_source_changes(self):
        packet = self.prepare("s0001")
        with self.assertRaisesRegex(core.PipelineError, "不在本節"):
            self.details(packet, "missing")
        with self.assertRaisesRegex(core.PipelineError, "資料包已失效"):
            self.details({**packet, "context_state": "stale"}, "person-001")
        raw = self.source.read_bytes()
        self.source.write_bytes(raw + "\n改動".encode())
        with self.assertRaisesRegex(core.PipelineError, "檔案已改變"):
            self.details(packet, "person-001")

    def test_term_details_rechecks_changes_during_read(self):
        packet = self.prepare("s0001")
        original = section_context.require_current_bundle
        def change_after_read(*args, **kwargs):
            result = original(*args, **kwargs)
            if kwargs.get("flow"):
                self.terms[0]["evidence"] = ["讀取中被更換的合成證據。"]
                write_rows(self.work / "terminology.jsonl", self.terms)
            return result
        with patch.object(section_context, "require_current_bundle", side_effect=change_after_read), self.assertRaisesRegex(core.PipelineError, "資料包已失效"):
            self.details(packet, "person-001")

    def test_compact_input_still_binds_full_terms_and_views_do_not_change_saved_artifacts(self):
        self.establish_prior_terms()
        compact = self.prepare("s0001")
        chunk = compact["chunks"][0]["id"]
        data = self.payload(chunk)
        data["context_state"] = compact["context_state"]
        self.save(chunk, data)
        path = self.work / "chunks" / (chunk + ".map.json")
        raw = path.read_bytes()
        self.assertEqual(core.load_json(path)["term_dependencies"]["person-001"], core.digest(core.canonical(self.terms[0])))
        full = section_context.prepare_section(core, argparse.Namespace(work=self.work, section="s0001", term_id=[],
                                                from_block=None, through_block=None, term_detail="full"))
        data["context_state"] = full["context_state"]
        self.save(chunk, data)
        self.assertEqual(path.read_bytes(), raw)

    def test_connected_next_uses_compact_view_and_cli_fetches_same_full_term(self):
        self.establish_prior_terms()
        result = self.daily_next(prepare=True)
        packet = result["context"]
        self.assertEqual(packet["terminology_view"], "compact")
        self.assertNotIn("evidence", packet["terminology"][0])
        command = [sys.executable, "-X", "utf8", str(Path(core.__file__)), "term-details", "--work", str(self.work),
                   "--section", "s0001", "--context-state", packet["context_state"], "--term-id", "person-001"]
        result = subprocess.run(command, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(json.loads(result.stdout)["terminology"], self.terms)

    def test_review_prepares_compact_next_and_paging_preserves_entire_source(self):
        self.establish_prior_terms()
        result = self.review_and_prepare("s0000")
        packet = result["next"]["context"]
        self.assertNotIn("evidence", packet["terminology"][0])
        pages = [self.prepare("s0001", first=b["id"], last=b["id"]) for b in packet["section"]["blocks"]]
        self.assertEqual([b for p in pages for b in p["section"]["blocks"]], self.manifest["sections"][1]["blocks"])
        self.assertTrue(all(p["context_state"] == packet["context_state"] and p["terminology"] == packet["terminology"] for p in pages))

    def test_cli_full_term_view_retains_all_evidence_without_changing_context(self):
        self.establish_prior_terms()
        packet = self.prepare("s0001")
        result = subprocess.run([sys.executable, "-X", "utf8", str(Path(core.__file__)), "prepare-section", "--work", str(self.work),
                                 "--section", "s0001", "--term-detail", "full"], capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        full = json.loads(result.stdout)
        self.assertEqual(full["terminology"], self.terms)
        self.assertEqual(full["context_state"], packet["context_state"])


if __name__ == "__main__":
    unittest.main()
