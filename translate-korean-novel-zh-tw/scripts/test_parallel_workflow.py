"""以舊版合成派工驗證既有草稿恢復及轉回單代理；不代表翻譯品質。"""

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
import novel_pipeline as core
import parallel_workflow as parallel
from test_novel_pipeline import synthetic_reviews, synthetic_scorecard, write_json, write_rows


class ParallelWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.run_root = Path(tempfile.mkdtemp(prefix="novel-parallel-tests-"))

    def setUp(self):
        self.root = self.run_root / self._testMethodName
        self.root.mkdir()
        self.source = self.root / "合成韓文.txt"
        self.reference = self.root / "合成人工版.txt"
        self.work = self.root / "translation-work"
        # 每個 c 恰有兩個來源區塊；十個 c 超過舊版 24,000 字緩衝上限。
        lines = []
        for section, count in enumerate((3, 13, 2)):
            lines.append("서장" if section == 0 else f"<1화 약속 ({section})>")
            lines.extend(f"목진이 말했다. {'가' * 1300} {section}-{index}." for index in range(count * 2))
        self.source.write_text("\n\n".join(lines) + "\n", encoding="utf-8")
        self.reference.write_text("木真說明天回來。", encoding="utf-8")
        core.init_project(argparse.Namespace(source=self.source, reference=self.reference, work=self.work,
                          title="平行流程合成測試", source_encoding="auto", reference_encoding="auto",
                          heading_pattern=None, source_layout="inline"))
        self.manifest = core.load_json(self.work / "manifest.json")
        self.plan = core.build_batch_plan(self.manifest["sections"], self.manifest["source"]["sha256"],
                                          target_chars=2700, max_chars=2800, batch_chars=2800)
        write_json(self.work / "batch-plan.json", self.plan)
        self.terms = [{"id": "person-001", "ko": ["목진"], "zh": "木真", "decision": "adopted",
                       "basis": "human_reference", "evidence": ["合成人工版第一行"], "forbidden_zh": ["穆真"]}]
        self.contexts = [{"section_id": row["id"], "summary": f"{row['id']} 已核定合成摘要。", "facts": []}
                         for row in self.manifest["sections"]]
        write_rows(self.work / "terminology.jsonl", self.terms)
        write_rows(self.work / "continuity.jsonl", self.contexts)

    def flow(self):
        return chunk_workflow.Workflow(core, self.work)

    def invoke(self, action, data=None, actor="main"):
        if action in parallel.RETIRED_ACTIONS:
            # 僅用於重建升級前的合成測試資料；正式 CLI 已拒絕這些派工操作。
            with core.writer_lock(self.work):
                flow = self.flow()
                pipeline = parallel.Pipeline(core, flow, actor)
                method = {"init": "initialize", "assist-request": "assist_request"}.get(action, action)
                result = getattr(pipeline, method)({} if data is None else data)
                flow.verification.flush(flow.manifest)
                return result
        return self.public_invoke(action, data, actor)

    def public_invoke(self, action, data=None, actor="main"):
        return parallel.run(core, argparse.Namespace(work=self.work, actor=actor, action=action),
                            data={} if data is None else data)

    def initialize(self, through=None):
        return self.invoke("init", {"checkpoint": True, "drafter": "draft", "researcher": "research",
                                    "through_chunk": through or self.plan["chunks"][-1]["id"]})

    def claim(self):
        return self.invoke("claim", actor="draft")

    def job_data(self, job):
        return {"job_id": job["job_id"], "generation": job["generation"]}

    def draft(self, job):
        view = self.flow().views[job["chunk_id"]]
        return {"units": [{"source_ids": [block["id"]], "text": f"木真記下第{index + 1}段合成記錄。"}
                          for index, block in enumerate(view["blocks"])],
                "state_after": {"viewpoint": "合成敘事視角", "time_place": "合成場景", "speakers": ["木真"],
                                "known_facts": ["只有測試用合成事件。"], "open_threads": []},
                "term_ids": [], "candidates": [], "questions": [],
                "summary_after": f"{job['chunk_id']} 草稿完成後的合成暫定摘要。"}

    def submit(self, job, draft=None, **extra):
        return self.invoke("submit", {**self.job_data(job), "draft_context_state": job["draft_context_state"],
                                      "draft": self.draft(job) if draft is None else draft,
                                      "claim_next": False, **extra}, actor="draft")

    def saved_job(self):
        job = self.claim()["job"]
        self.submit(job)
        return job

    def review(self, job):
        return self.invoke("review", self.job_data(job))

    def accept_data(self, job, reviewed=None):
        reviewed = self.review(job) if reviewed is None else reviewed
        ids = [block["id"] for block in self.flow().views[job["chunk_id"]]["blocks"]]
        return {**self.job_data(job), "draft_sha256": reviewed["draft_sha256"],
                "review_state": reviewed["review_state"], "edits": [], "reviews": synthetic_reviews(core.STAGES),
                "state_after": deepcopy(reviewed["draft"]["state_after"]), "term_ids": [],
                "rechecks": [{"change_id": row["id"], "source_ids": ids,
                              "note": "合成測試已逐一核對這項依據差異。"} for row in reviewed["changes"]],
                "decisions": {}, "merge": False}

    def accept(self, job, data=None):
        return self.invoke("accept", self.accept_data(job) if data is None else data)

    def test_claim_resume_keeps_same_generation_until_submission(self):
        self.initialize()
        first, resumed = self.claim(), self.claim()
        self.assertEqual(first["action"], "draft")
        self.assertEqual(resumed["action"], "resume_draft")
        self.assertEqual(first["job"], resumed["job"])
        self.assertFalse((self.work / "chunks").exists())
        self.assertNotIn("receipt", core.load_json(self.work / "manifest.json")["sections"][0])

    def test_ten_complete_chunks_cross_sections_without_total_character_limit(self):
        self.initialize()
        jobs = [self.saved_job() for _ in range(10)]
        self.assertEqual([job["chunk_id"] for job in jobs], [row["id"] for row in self.plan["chunks"][:10]])
        self.assertEqual(len({job["slot"] for job in jobs}), 10)
        self.assertGreater(sum(row["chars"] for row in self.plan["chunks"][:10]), 24000)
        self.assertGreater(len({row["section_id"] for row in self.plan["chunks"][:10]}), 1)
        self.assertEqual(self.claim()["action"], "wait_capacity")
        for job in jobs:
            saved = core.load_json(self.work / "pipeline" / f"slot-{job['slot']:02d}.json")
            covered = [identity for unit in saved["draft"]["units"] for identity in unit["source_ids"]]
            expected = [block["id"] for block in self.flow().views[job["chunk_id"]]["blocks"]]
            self.assertEqual(covered, expected)
        self.assertFalse((self.work / "chunks").exists())

    def test_submission_rejects_missing_duplicate_reordered_and_foreign_sources(self):
        self.initialize()
        job = self.claim()["job"]
        valid = self.draft(job)
        self.assertEqual(len(valid["units"]), 2)
        invalid_units = [valid["units"][:1], [valid["units"][0], valid["units"][0]],
                         list(reversed(valid["units"])), valid["units"] + [{"source_ids": ["s9999-b0001"], "text": "多餘句子。"}]]
        for units in invalid_units:
            with self.subTest(units=units), self.assertRaises(core.PipelineError):
                self.submit(job, {**valid, "units": units})
        self.submit(job, valid)
        self.assertEqual(self.review(job)["draft"]["units"], valid["units"])

    def test_next_section_draft_carries_previous_draft_summary_without_formal_receipt(self):
        self.initialize()
        first_section = [row for row in self.plan["chunks"] if row["section_id"] == "s0000"]
        jobs = [self.saved_job() for _ in first_section]
        next_claim = self.claim()
        self.assertEqual(self.flow().views[next_claim["job"]["chunk_id"]]["id"], "s0001")
        packet = json.dumps(next_claim["packet"], ensure_ascii=False)
        self.assertIn(self.draft(jobs[-1])["summary_after"], packet)
        self.assertNotIn("receipt", core.load_json(self.work / "manifest.json")["sections"][0])
        self.assertEqual(self.flow().progress["chunks"], {})

    def test_missing_official_previous_summary_only_uses_provisional_memory_projection(self):
        write_rows(self.work / "continuity.jsonl", self.contexts[1:])
        unchanged = (self.work / "continuity.jsonl").read_bytes()
        self.initialize()
        jobs = [self.saved_job() for _ in range(3)]
        claimed = self.claim()
        self.assertTrue(claimed["packet"]["previous_section"]["provisional"])
        self.assertEqual(claimed["packet"]["previous_chunk"]["kind"], "provisional")
        self.assertEqual((self.work / "continuity.jsonl").read_bytes(), unchanged)
        self.submit(claimed["job"])
        for job in jobs:
            self.accept(job)
        with self.assertRaises(core.PipelineError):
            self.review(claimed["job"])
        write_rows(self.work / "continuity.jsonl", self.contexts)
        reviewed = self.review(claimed["job"])
        self.assertTrue(reviewed["changes"])
        self.assertNotIn("provisional", reviewed["packet"]["previous_section"])
        self.accept(claimed["job"], self.accept_data(claimed["job"], reviewed))

    def test_short_tail_stops_at_through_chunk_without_filling_capacity(self):
        through = self.plan["chunks"][2]["id"]
        self.initialize(through)
        jobs = [self.saved_job() for _ in range(3)]
        self.assertEqual(jobs[-1]["chunk_id"], through)
        self.assertNotIn(self.claim()["action"], ("draft", "resume_draft"))

    def test_registered_roles_cannot_write_outside_their_responsibility(self):
        self.initialize()
        for actor in ("draft", "research", "unknown"):
            with self.subTest(actor=actor), self.assertRaises(core.PipelineError):
                self.invoke("init", {"checkpoint": True, "drafter": "draft", "researcher": "research",
                                     "through_chunk": self.plan["chunks"][-1]["id"]}, actor)
        job = self.saved_job()
        for actor, actions in (("draft", ("review", "accept", "resolve", "recover", "retry-review", "assist-request", "assist-resolve", "assist-cancel")),
                               ("research", ("claim", "submit", "review", "accept", "resolve", "recover", "retry-review", "assist-request", "assist-cancel")),
                               ("main", ("claim", "submit", "assist-submit"))):
            for action in actions:
                with self.subTest(actor=actor, action=action), self.assertRaises(core.PipelineError):
                    self.invoke(action, self.job_data(job), actor)
        with self.assertRaises(core.PipelineError):
            self.invoke("status", actor="unknown")
        for actor in ("main", "draft", "research"):
            self.invoke("status", actor=actor)

    def test_accept_requires_each_actual_check_without_writing_formal_artifacts(self):
        self.initialize()
        job = self.saved_job()
        valid = self.accept_data(job)
        for stage in core.STAGES:
            invalid = deepcopy(valid)
            del invalid["reviews"][stage]
            with self.subTest(stage=stage), self.assertRaises(core.PipelineError):
                self.accept(job, invalid)
            self.assertFalse((self.work / "chunks" / (job["chunk_id"] + ".txt")).exists())
        invalid = deepcopy(valid)
        invalid["reviews"][core.STAGES[0]]["status"] = "pending"
        with self.assertRaises(core.PipelineError):
            self.accept(job, invalid)
        self.accept(job, valid)
        flow = self.flow()
        self.assertEqual(flow.progress["chunks"][job["chunk_id"]]["status"], "self_checked")
        self.assertEqual(flow.progress["book_review"].get("completed_batches", []), [])

    def test_old_generation_draft_sha_and_review_state_are_rejected(self):
        self.initialize()
        job = self.saved_job()
        valid = self.accept_data(job)
        for key, value in (("generation", job["generation"] + 1), ("draft_sha256", "0" * 64), ("review_state", "0" * 64)):
            with self.subTest(key=key), self.assertRaises(core.PipelineError):
                self.accept(job, {**valid, key: value})
        with self.assertRaises(core.PipelineError):
            self.submit({**job, "generation": job["generation"] + 1})
        self.assertFalse((self.work / "chunks").exists())

    def test_term_change_between_review_and_accept_requires_fresh_review(self):
        self.initialize()
        job = self.saved_job()
        old = self.accept_data(job)
        self.terms[0]["evidence"].append("後補的合成依據。")
        write_rows(self.work / "terminology.jsonl", self.terms)
        with self.assertRaises(core.PipelineError):
            self.accept(job, old)
        reviewed = self.review(job)
        self.assertTrue(reviewed["changes"])
        without_rechecks = self.accept_data(job, reviewed)
        without_rechecks["rechecks"] = []
        with self.assertRaises(core.PipelineError):
            self.accept(job, without_rechecks)
        self.accept(job, self.accept_data(job, reviewed))

    def test_rechecks_need_valid_source_ids_and_actual_notes(self):
        self.initialize()
        job = self.saved_job()
        self.terms[0]["evidence"].append("後補的合成依據。")
        write_rows(self.work / "terminology.jsonl", self.terms)
        reviewed = self.review(job)
        self.assertTrue(reviewed["changes"])
        valid = self.accept_data(job, reviewed)
        for field, value in (("source_ids", []), ("source_ids", ["s9999-b0001"]), ("note", "")):
            invalid = deepcopy(valid)
            invalid["rechecks"][0][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(core.PipelineError):
                self.accept(job, invalid)

    def test_style_change_between_review_and_accept_invalidates_review_state(self):
        self.initialize()
        job = self.saved_job()
        old = self.accept_data(job)
        flow = self.flow()
        flow.progress["style_profile"] = {"narrator_voice": "修改後的合成敘事設定。"}
        write_json(self.work / "batch-progress.json", flow.progress)
        with self.assertRaises(core.PipelineError):
            self.accept(job, old)
        reviewed = self.review(job)
        self.assertTrue(reviewed["changes"])
        self.accept(job, self.accept_data(job, reviewed))

    def test_candidates_and_questions_need_main_decisions_before_acceptance(self):
        self.initialize()
        job = self.claim()["job"]
        draft = self.draft(job)
        ids = draft["units"][0]["source_ids"]
        draft["candidates"] = [{"id": "candidate-001", "source_ids": ids, "ko": "목진", "zh": "木真",
                                "note": "合成測試要求主責確認此譯名。"}]
        draft["questions"] = [{"id": "question-001", "source_ids": ids,
                               "note": "合成測試要求主責確認敘事人物。", "blocks_following": False}]
        self.submit(job, draft)
        invalid = self.accept_data(job)
        with self.assertRaises(core.PipelineError):
            self.accept(job, invalid)
        valid = deepcopy(invalid)
        valid["decisions"] = {identity: {"source_ids": ids, "note": "合成測試已依來源核對並解決。", "status": "resolved"}
                              for identity in ("candidate-001", "question-001")}
        self.accept(job, valid)

    def test_blocking_question_stops_following_claim_until_main_resolves_it(self):
        self.initialize()
        job = self.claim()["job"]
        draft = self.draft(job)
        ids = draft["units"][0]["source_ids"]
        draft["questions"] = [{"id": "blocking-001", "source_ids": ids,
                               "note": "合成身分疑義會影響後續全部小段。", "blocks_following": True}]
        self.submit(job, draft)
        self.assertNotIn(self.claim()["action"], ("draft", "resume_draft"))
        self.invoke("resolve", {**self.job_data(job), "decisions": {"blocking-001": {
                    "source_ids": ids, "note": "合成測試已核對來源並解除後續阻塞。", "status": "resolved"}}})
        self.assertEqual(self.claim()["job"]["chunk_id"], self.plan["chunks"][1]["id"])
        self.assertFalse((self.work / "chunks").exists())

    def test_input_file_change_never_accepts_prior_draft(self):
        self.initialize()
        for name in ("source", "reference"):
            with self.subTest(name=name):
                job = self.saved_job()
                old = self.accept_data(job)
                path = getattr(self, name)
                original = path.read_bytes()
                path.write_bytes(original + "\n合成測試變動。".encode("utf-8"))
                try:
                    with self.assertRaises(core.PipelineError):
                        self.accept(job, old)
                finally:
                    path.write_bytes(original)
                self.accept(job, old)

    def test_accept_frees_slot_before_long_section_is_ready_for_review(self):
        self.initialize()
        first_section_count = len(self.flow().section_chunks("s0000"))
        for _ in range(first_section_count):
            self.accept(self.saved_job())
        jobs = [self.saved_job() for _ in range(10)]
        self.assertTrue(all(self.flow().views[job["chunk_id"]]["id"] == "s0001" for job in jobs))
        self.assertEqual(self.claim()["action"], "wait_capacity")
        first = jobs[0]
        self.accept(first)
        eleventh = self.claim()["job"]
        self.assertEqual(eleventh["chunk_id"], self.plan["chunks"][first_section_count + 10]["id"])
        self.assertEqual(eleventh["slot"], first["slot"])
        self.assertGreater(eleventh["generation"], first["generation"])
        self.assertFalse(any("receipt" in section for section in core.load_json(self.work / "manifest.json")["sections"]))
        with self.assertRaises(core.PipelineError):
            self.submit(first)

    def test_local_edit_replaces_only_selected_unit_and_preserves_coverage(self):
        self.initialize()
        job = self.saved_job()
        valid = self.accept_data(job)
        original = self.review(job)["draft"]["units"]
        replacement = {**original[0], "text": "木真留下經過修訂的合成記錄。"}
        valid["edits"] = [{"unit": 0, "replacement": [replacement]}]
        self.accept(job, valid)
        text = (self.work / "chunks" / (job["chunk_id"] + ".txt")).read_text(encoding="utf-8")
        self.assertIn(replacement["text"], text)
        self.assertIn(original[1]["text"], text)
        self.assertNotIn(original[0]["text"], text)

    def test_accept_rejects_edit_that_loses_source_coverage(self):
        self.initialize()
        job = self.saved_job()
        invalid = self.accept_data(job)
        invalid["edits"] = [{"unit": 0, "replacement": []}]
        with self.assertRaises(core.PipelineError):
            self.accept(job, invalid)
        self.assertFalse((self.work / "chunks").exists())

    def test_chunk_acceptance_preserves_section_score_and_book_attestation_gates(self):
        ids = [row["id"] for row in self.plan["chunks"] if row["section_id"] == "s0000"]
        self.initialize(through=ids[-1])
        for _ in ids:
            self.accept(self.saved_job())
        self.assertFalse((self.work / "book-receipt.json").exists())
        self.assertNotIn("receipt", core.load_json(self.work / "manifest.json")["sections"][0])
        chunk_workflow.run(core, argparse.Namespace(command="merge-section", work=self.work,
                                                   section="s0000", title="序章", replace=False, actor="main"))
        state = chunk_workflow.run(core, argparse.Namespace(command="review-state", work=self.work, section="s0000", actor="main"))
        data = {**state, "reviews": synthetic_reviews(core.STAGES + ("heading",)),
                "scorecard": synthetic_scorecard(["s0000"], 84.99), "book_reviews": synthetic_reviews(core.BOOK_STAGES)}
        path = self.root / "合成整節複核.json"
        write_json(path, data)
        with self.assertRaises(core.PipelineError):
            chunk_workflow.run(core, argparse.Namespace(command="review-section", work=self.work,
                                                       section="s0000", input=path, replace=False, actor="main"))
        data["scorecard"] = synthetic_scorecard(["s0000"], 85)
        write_json(path, data)
        chunk_workflow.run(core, argparse.Namespace(command="review-section", work=self.work,
                                                   section="s0000", input=path, replace=False, actor="main"))
        self.assertIn("receipt", core.load_json(self.work / "manifest.json")["sections"][0])
        self.assertFalse((self.work / "book-receipt.json").exists())
        with self.assertRaises((core.PipelineError, FileNotFoundError)):
            core.assemble_book(argparse.Namespace(work=self.work, output=self.root / "未驗收全文.txt", actor="main"))

    def test_writer_lock_blocks_mutation_without_issuing_job(self):
        self.initialize()
        with core.writer_lock(self.work), self.assertRaises(core.PipelineError):
            self.claim()
        self.assertEqual(self.claim()["job"]["chunk_id"], self.plan["chunks"][0]["id"])

    def test_later_draft_cannot_be_reviewed_or_accepted_before_first_gap(self):
        self.initialize()
        first, second = self.saved_job(), self.saved_job()
        with self.assertRaises(core.PipelineError):
            self.review(second)
        with self.assertRaises(core.PipelineError):
            self.invoke("accept", self.job_data(second))
        self.accept(first)
        self.accept(second)

    def test_recover_finishes_formal_save_before_slot_release_and_is_repeatable(self):
        self.initialize()
        job = self.saved_job()
        data = self.accept_data(job)
        original = chunk_workflow.Workflow.save_chunk

        def interrupted(flow, *args, **kwargs):
            result = original(flow, *args, **kwargs)
            if not kwargs.get("validate_only"):
                raise OSError("合成測試：正式進度寫入後中斷。")
            return result

        with patch.object(chunk_workflow.Workflow, "save_chunk", new=interrupted), self.assertRaises(OSError):
            self.accept(job, data)
        self.assertEqual(self.flow().progress["chunks"][job["chunk_id"]]["status"], "self_checked")
        self.assertEqual(self.invoke("status")["pending"], 1)
        recovered = self.invoke("recover")
        self.assertEqual(len(recovered["recovered"]), 1)
        self.assertEqual(recovered["pending"], 0)
        self.assertEqual(self.invoke("recover")["recovered"], [])
        self.assertEqual(self.claim()["job"]["chunk_id"], self.plan["chunks"][1]["id"])
        self.assertFalse((self.work / "book-receipt.json").exists())

    def test_recover_reconciles_failed_cursor_write_without_retranslating(self):
        self.initialize()
        job = self.saved_job()
        data = self.accept_data(job)
        original = core.atomic_json

        def interrupted(path, value):
            if Path(path).resolve() == (self.work / "pipeline" / "state.json").resolve() and value.get("cursor", 0) > 0:
                raise OSError("合成測試：槽位釋放前中斷。")
            return original(path, value)

        with patch.object(core, "atomic_json", side_effect=interrupted), self.assertRaises(OSError):
            self.accept(job, data)
        text_path = self.work / "chunks" / (job["chunk_id"] + ".txt")
        saved = text_path.read_bytes()
        recovered = self.invoke("recover")
        self.assertEqual(len(recovered["recovered"]), 1)
        self.assertEqual(text_path.read_bytes(), saved)
        self.assertEqual(recovered["pending"], 0)

    def test_recover_preserves_external_formal_edit_and_keeps_slot_pending(self):
        self.initialize()
        job = self.saved_job()
        data = self.accept_data(job)
        original = chunk_workflow.Workflow.save_chunk

        def interrupted(flow, *args, **kwargs):
            result = original(flow, *args, **kwargs)
            if not kwargs.get("validate_only"):
                raise OSError("合成測試：正式保存完成後中斷。")
            return result

        with patch.object(chunk_workflow.Workflow, "save_chunk", new=interrupted), self.assertRaises(OSError):
            self.accept(job, data)
        text_path = self.work / "chunks" / (job["chunk_id"] + ".txt")
        text_path.write_text("木真留下人工修訂的合成稿。\n", encoding="utf-8")
        with self.assertRaises(core.PipelineError):
            self.invoke("recover")
        self.assertEqual(text_path.read_text(encoding="utf-8"), "木真留下人工修訂的合成稿。\n")
        self.assertEqual(self.invoke("status")["pending"], 1)

    def test_recover_rechecks_accepted_prefix_before_publishing_later_chunk(self):
        self.initialize()
        first = self.saved_job()
        self.accept(first)
        second = self.saved_job()
        data = self.accept_data(second)
        original = chunk_workflow.Workflow.save_chunk

        def interrupted(flow, *args, **kwargs):
            if not kwargs.get("validate_only"):
                raise OSError("合成測試：第二小段正式寫入前中斷。")
            return original(flow, *args, **kwargs)

        with patch.object(chunk_workflow.Workflow, "save_chunk", new=interrupted), self.assertRaises(OSError):
            self.accept(second, data)
        first_path = self.work / "chunks" / (first["chunk_id"] + ".txt")
        first_path.write_text("木真留下前序人工修訂。\n", encoding="utf-8")
        with self.assertRaises(core.PipelineError):
            self.invoke("recover")
        self.assertFalse((self.work / "chunks" / (second["chunk_id"] + ".txt")).exists())
        self.assertEqual(first_path.read_text(encoding="utf-8"), "木真留下前序人工修訂。\n")
        self.assertEqual(self.invoke("status")["pending"], 1)

    def test_later_packet_retains_candidates_questions_and_main_resolutions(self):
        self.initialize()
        job = self.claim()["job"]
        draft = self.draft(job)
        ids = draft["units"][0]["source_ids"]
        draft["candidates"] = [{"id": "candidate-001", "source_ids": ids, "ko": "목진", "zh": "木真",
                                "note": "此合成候選稱呼必須供後文查看。"}]
        draft["questions"] = [{"id": "question-001", "source_ids": ids,
                               "note": "此合成稱呼會影響後文。", "blocks_following": True}]
        self.submit(job, draft)
        decisions = {"question-001": {"source_ids": ids, "note": "主責已核對合成原文，可繼續初譯。", "status": "resolved"}}
        self.invoke("resolve", {**self.job_data(job), "decisions": decisions})
        later = self.claim()["packet"]["provisional_dependencies"]
        previous = next(row for row in later if row["chunk_id"] == job["chunk_id"])
        self.assertEqual(previous["candidates"], draft["candidates"])
        self.assertEqual(previous["questions"], draft["questions"])
        self.assertEqual(previous["resolutions"], decisions)
        self.assertFalse((self.work / "chunks").exists())

    def test_recover_rejects_tampered_publication_and_preserves_original_draft(self):
        self.initialize()
        job = self.saved_job()
        data = self.accept_data(job)
        original = chunk_workflow.Workflow.save_chunk

        def interrupted(flow, *args, **kwargs):
            if not kwargs.get("validate_only"):
                raise OSError("合成測試：正式寫入前中斷。")
            return original(flow, *args, **kwargs)

        with patch.object(chunk_workflow.Workflow, "save_chunk", new=interrupted), self.assertRaises(OSError):
            self.accept(job, data)
        slot_path = self.work / "pipeline" / f"slot-{job['slot']:02d}.json"
        slot = core.load_json(slot_path)
        original_draft = deepcopy(slot["draft"])
        slot["publication"]["input"]["units"][0]["text"] = "木真留下未經主責複核的外部替換句。"
        write_json(slot_path, slot)
        with self.assertRaises(core.PipelineError):
            self.invoke("recover")
        self.assertEqual(core.load_json(slot_path)["draft"], original_draft)
        self.assertFalse((self.work / "chunks").exists())

    def test_partial_write_requires_fresh_review_after_style_change_and_keeps_real_edits(self):
        self.initialize()
        job = self.saved_job()
        data = self.accept_data(job)
        original = core.atomic_text
        text_path = self.work / "chunks" / (job["chunk_id"] + ".txt")
        map_path = self.work / "chunks" / (job["chunk_id"] + ".map.json")
        input_path = self.work / "chunks" / (job["chunk_id"] + ".input.json")

        def interrupted(path, value):
            if Path(path).resolve() == map_path.resolve():
                raise OSError("合成測試：input 與譯文落盤後、mapping 寫入前中斷。")
            return original(path, value)

        with patch.object(core, "atomic_text", side_effect=interrupted), self.assertRaises(OSError):
            self.accept(job, data)
        self.assertTrue(input_path.exists())
        self.assertTrue(text_path.exists())
        self.assertFalse(map_path.exists())
        progress = self.flow().progress
        progress["style_profile"] = {"narrator_voice": "重新確認的合成敘事設定。"}
        write_json(self.work / "batch-progress.json", progress)
        with self.assertRaises(core.PipelineError):
            self.invoke("recover")
        source_ids = self.draft(job)["units"][0]["source_ids"]
        retried = self.invoke("retry-review", {**self.job_data(job), "source_ids": source_ids,
                              "note": "風格依據改變，重新逐句核對局部落盤的合成稿。"})
        self.assertTrue(retried["replace_required"])
        self.assertEqual(self.flow().progress["chunks"][job["chunk_id"]]["status"], "awaiting_recheck")
        with self.assertRaises(core.PipelineError):
            self.accept(job, data)
        reviewed = self.review(job)
        fresh = self.accept_data(job, reviewed)
        fresh["replace"] = True
        fresh["edits"] = [{"unit": 0, "replacement": [{"source_ids": source_ids,
                            "text": "木真留下重新核對過風格的合成句子。"}]}]
        self.accept(job, fresh)
        self.assertIn("木真留下重新核對過風格的合成句子。", text_path.read_text(encoding="utf-8"))
        self.assertEqual(self.flow().progress["chunks"][job["chunk_id"]]["status"], "self_checked")
        self.assertEqual(self.invoke("status")["pending"], 0)

    def test_repeating_latest_accepted_result_does_not_consume_a_new_slot(self):
        self.initialize()
        job = self.saved_job()
        data = self.accept_data(job)
        self.accept(job, data)
        prior = self.invoke("status")
        repeated = self.accept(job, data)
        self.assertTrue(repeated["already_accepted"])
        self.assertEqual(self.invoke("status"), prior)

    def test_research_result_needs_main_resolution_and_never_grants_formal_pass(self):
        self.initialize()
        job = self.saved_job()
        ids = self.draft(job)["units"][0]["source_ids"]
        request = self.invoke("assist-request", {**self.job_data(job), "question": "核對合成原文的人物稱呼。",
                                                 "source_ids": ids})
        result = {"source_ids": ids, "note": "合成測試查證結果：此處指木真。", "uncertainty": "無其他合成疑義。"}
        for forbidden in ("passed", "reviews", "scorecard", "decision"):
            with self.subTest(forbidden=forbidden), self.assertRaises(core.PipelineError):
                self.invoke("assist-submit", {"assist_id": request["assist_id"], "result": {**result, forbidden: True}}, "research")
        submitted = self.invoke("assist-submit", {"assist_id": request["assist_id"], "result": result}, "research")
        self.assertFalse(submitted["formal_acceptance"])
        with self.assertRaises(core.PipelineError):
            self.accept(job)
        resolved = self.invoke("assist-resolve", {"assist_id": request["assist_id"], "source_ids": ids,
                               "note": "主責依合成原文核對後採納此查證。", "status": "resolved"})
        self.assertFalse(resolved["formal_acceptance"])
        self.assertFalse((self.work / "chunks").exists())
        self.accept(job)

    def test_cancelled_research_id_is_rejected_and_fresh_assignment_can_finish(self):
        self.initialize()
        job = self.saved_job()
        ids = self.draft(job)["units"][0]["source_ids"]
        request_data = {**self.job_data(job), "question": "查證合成原文的人物稱呼。", "source_ids": ids}
        original = self.invoke("assist-request", request_data)
        result = {"source_ids": ids, "note": "合成查證：此處指木真。", "uncertainty": "沒有其餘合成疑義。"}
        self.invoke("assist-submit", {"assist_id": original["assist_id"], "result": result}, "research")
        self.terms[0]["evidence"].append("查證途中更新的合成稱呼依據。")
        write_rows(self.work / "terminology.jsonl", self.terms)
        cancelled = self.invoke("assist-cancel", {"assist_id": original["assist_id"], "source_ids": ids,
                                "note": "稱呼依據已更新，撤回舊查證並重新核對。"})
        self.assertEqual(cancelled["cancelled_assist"], original["assist_id"])
        self.assertFalse(cancelled["formal_acceptance"])
        replacement = self.invoke("assist-request", request_data)
        self.assertNotEqual(replacement["assist_id"], original["assist_id"])
        with self.assertRaises(core.PipelineError):
            self.invoke("assist-submit", {"assist_id": original["assist_id"], "result": result}, "research")
        with self.assertRaises(core.PipelineError):
            self.invoke("assist-resolve", {"assist_id": original["assist_id"], "source_ids": ids,
                         "note": "這是不得採納的過期合成結論。", "status": "resolved"})
        fresh_result = {**result, "note": "已核對更新依據，合成原文仍指木真。"}
        self.invoke("assist-submit", {"assist_id": replacement["assist_id"], "result": fresh_result}, "research")
        self.invoke("assist-resolve", {"assist_id": replacement["assist_id"], "source_ids": ids,
                     "note": "主責核對更新的合成原文依據後採納。", "status": "resolved"})
        reviewed = self.review(job)
        archived = next(row for row in reviewed["findings"] if row["assist_id"] == original["assist_id"])
        self.assertFalse(archived["usable"])
        self.accept(job, self.accept_data(job, reviewed))

    def test_accepted_input_preserves_original_candidates_after_slot_reuse(self):
        self.initialize()
        job = self.claim()["job"]
        draft = self.draft(job)
        ids = draft["units"][0]["source_ids"]
        draft["candidates"] = [{"id": "candidate-history", "source_ids": ids, "ko": "목진", "zh": "木真候選",
                                "note": "這是初譯提出、待主責判定的合成稱呼。"}]
        draft["questions"] = [{"id": "question-history", "source_ids": ids,
                               "note": "合成原文此句主詞是否仍是木真。", "blocks_following": False}]
        self.submit(job, draft)
        data = self.accept_data(job)
        data["decisions"] = {identity: {"source_ids": ids, "status": "resolved",
                              "note": "核對合成原文後採既定譯名，保留原提議及疑問供追溯。"}
                             for identity in ("candidate-history", "question-history")}
        self.accept(job, data)
        input_path = self.work / "chunks" / (job["chunk_id"] + ".input.json")
        preserved = input_path.read_bytes()
        canonical_input = core.load_json(input_path)
        metadata = canonical_input["parallel_review"]["draft_metadata"]
        self.assertEqual(metadata["candidates"], draft["candidates"])
        self.assertEqual(metadata["questions"], draft["questions"])
        self.assertEqual(canonical_input["parallel_review"]["decisions"], data["decisions"])
        next_job = self.saved_job()
        self.assertEqual(next_job["slot"], job["slot"])
        self.assertGreater(next_job["generation"], job["generation"])
        self.assertEqual(input_path.read_bytes(), preserved)

    def test_cli_serial_unicode_round_trip_and_retired_dispatch(self):
        self.initialize()
        job = self.saved_job()
        payload = {"checkpoint": True}
        command = [sys.executable, "-X", "utf8", str(Path(core.__file__).resolve()), "parallel", "--work", str(self.work),
                   "--actor", "main", "--action", "serial", "--input", "-"]
        created = subprocess.run(command, input=json.dumps(payload, ensure_ascii=False), encoding="utf-8",
                                 capture_output=True, check=False)
        self.assertEqual(created.returncode, 0, created.stderr)
        self.assertFalse(json.loads(created.stdout)["active"])
        self.assertEqual(json.loads(created.stdout)["jobs"][0]["chunk_id"], job["chunk_id"])
        self.assertTrue(Path(json.loads(created.stdout)["jobs"][0]["draft_path"]).exists())
        command[command.index("main")] = "research"
        command[command.index("serial")] = "claim"
        rejected = subprocess.run(command, input="{}", encoding="utf-8", capture_output=True, check=False)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertEqual(self.invoke("status")["pending"], 1)

    def test_retired_actions_cannot_create_a_pipeline(self):
        for action in parallel.RETIRED_ACTIONS:
            with self.subTest(action=action), self.assertRaises(core.PipelineError):
                self.public_invoke(action)
        self.assertFalse((self.work / "pipeline").exists())

    def test_old_submission_saves_without_claiming_next_job(self):
        self.initialize()
        job = self.claim()["job"]
        result = self.submit(job, claim_next=True)
        self.assertEqual(result["next"]["action"], "resume_serial")
        self.assertEqual(self.invoke("status")["pending"], 1)
        self.assertEqual(self.invoke("status")["jobs"][0]["status"], "ready")

    def test_serial_preserves_pending_drafts_findings_and_formal_progress(self):
        self.initialize()
        first, second = self.saved_job(), self.saved_job()
        ids = self.draft(first)["units"][0]["source_ids"]
        request = self.invoke("assist-request", {**self.job_data(first), "question": "核對舊稿尚未決定的稱呼。",
                                                "source_ids": ids})
        self.invoke("assist-submit", {"assist_id": request["assist_id"], "result": {
            "source_ids": ids, "note": "合成原文查證待主責核對。", "uncertainty": "尚未正式採納。"}}, "research")
        state_before = core.load_json(self.work / "pipeline/state.json")
        files_before = {p.relative_to(self.work): p.read_bytes() for p in self.work.rglob("*")
                        if p.is_file() and "context" not in p.relative_to(self.work).parts}
        result = self.invoke("serial", {"checkpoint": True})
        self.assertEqual(result["mode"], "serial")
        self.assertFalse(result["active"])
        self.assertEqual([row["chunk_id"] for row in result["jobs"]], [first["chunk_id"], second["chunk_id"]])
        self.assertIsNotNone(result["assist"])
        expected = deepcopy(state_before)
        expected["active"] = False
        self.assertEqual(core.load_json(self.work / "pipeline/state.json"), expected)
        for path, content in files_before.items():
            if path.as_posix() != "pipeline/state.json":
                self.assertEqual((self.work / path).read_bytes(), content, str(path))
        saved_state = (self.work / "pipeline/state.json").read_bytes()
        self.invoke("serial", {"checkpoint": True})
        self.assertEqual((self.work / "pipeline/state.json").read_bytes(), saved_state)
        for action in ("submit", "assist-submit"):
            with self.subTest(action=action), self.assertRaises(core.PipelineError):
                self.public_invoke(action, {}, "draft" if action == "submit" else "research")
        with self.assertRaises(core.PipelineError):
            self.public_invoke("claim", {}, "draft")
        self.assertEqual((self.work / "pipeline/state.json").read_bytes(), saved_state)

    def test_serial_requires_main_checkpoint_and_common_writer_lock(self):
        self.initialize()
        self.saved_job()
        before = (self.work / "pipeline/state.json").read_bytes()
        for data, actor in (({}, "main"), ({"checkpoint": True}, "draft")):
            with self.subTest(data=data, actor=actor), self.assertRaises(core.PipelineError):
                self.invoke("serial", data, actor)
        with core.writer_lock(self.work), self.assertRaises(core.PipelineError):
            self.invoke("serial", {"checkpoint": True})
        self.assertEqual((self.work / "pipeline/state.json").read_bytes(), before)

    def test_serial_keeps_unsubmitted_job_without_treating_stale_slot_as_draft(self):
        self.initialize()
        first = self.saved_job()
        self.accept(first)
        following = self.claim()["job"]
        self.assertEqual(first["slot"], following["slot"])
        result = self.invoke("serial", {"checkpoint": True})
        self.assertEqual(result["jobs"][0]["chunk_id"], following["chunk_id"])
        self.assertEqual(result["jobs"][0]["status"], "drafting")
        self.assertIsNone(result["jobs"][0]["draft_path"])
        self.assertEqual(core.load_json(self.work / "pipeline/slot-01.json")["job_id"], first["job_id"])

    def test_serial_refuses_unfinished_publication_before_changing_state(self):
        self.initialize()
        job = self.saved_job()
        data = self.accept_data(job)
        with patch.object(parallel.Pipeline, "publish", side_effect=OSError("合成正式保存中斷")):
            with self.assertRaises(OSError):
                self.accept(job, data)
        before = (self.work / "pipeline/state.json").read_bytes()
        with self.assertRaises(core.PipelineError):
            self.invoke("serial", {"checkpoint": True})
        self.assertEqual((self.work / "pipeline/state.json").read_bytes(), before)
        self.invoke("recover")
        self.assertFalse(self.invoke("serial", {"checkpoint": True})["active"])

    def test_serial_uses_standard_save_without_actor_and_still_requires_actual_reviews(self):
        self.initialize()
        job = self.saved_job()
        draft_path = self.work / "pipeline/slot-01.json"
        stored = draft_path.read_bytes()
        draft = core.load_json(draft_path)["draft"]
        self.invoke("serial", {"checkpoint": True})
        draft["legacy_draft"] = self.job_data(job)
        path = self.root / "serial-input.json"
        write_json(path, draft)
        args = argparse.Namespace(command="submit-chunk", work=self.work, chunk=job["chunk_id"],
                                  input=path, replace=False, merge=False, title=None)
        with self.assertRaises(core.PipelineError):
            chunk_workflow.run(core, args)
        self.assertFalse((self.work / "chunks").exists())
        draft["reviews"] = synthetic_reviews(core.STAGES)
        write_json(path, draft)
        chunk_workflow.run(core, args)
        saved = core.load_json(self.work / "chunks" / (job["chunk_id"] + ".input.json"))
        self.assertEqual(saved["legacy_draft"], self.job_data(job))
        self.assertEqual(saved["units"], draft["units"])
        self.assertEqual(draft_path.read_bytes(), stored)
        self.assertNotIn("receipt", core.load_json(self.work / "manifest.json")["sections"][0])

    def test_full_term_evidence_uses_actual_draft_or_review_basis(self):
        self.initialize()
        claimed = self.claim()
        job, packet = claimed["job"], claimed["packet"]
        query = {**self.job_data(job), "basis_state": packet["basis_state"],
                 "extra_term_ids": packet["extra_term_ids"], "term_ids": ["person-001"]}
        self.assertEqual(self.invoke("details", query, "draft")["terminology"], self.terms)
        self.assertEqual(self.invoke("details", query)["terminology"], self.terms)
        self.submit(job)
        reviewed = self.review(job)
        query["basis_state"] = reviewed["packet"]["basis_state"]
        query["extra_term_ids"] = reviewed["packet"]["extra_term_ids"]
        self.assertEqual(self.invoke("details", query)["terminology"], self.terms)
        self.terms[0]["evidence"].append("合成詞條證據已改變。")
        write_rows(self.work / "terminology.jsonl", self.terms)
        with self.assertRaises(core.PipelineError):
            self.invoke("details", query)

    def test_thirteen_chunk_section_releases_slots_before_whole_section_scoring(self):
        """長節先逐小段修訂釋槽，全部齊備後才取得整節分數。"""
        section_ids = [row["id"] for row in self.plan["chunks"] if row["section_id"] == "s0001"]
        self.assertEqual(len(section_ids), 13)
        self.initialize(through=section_ids[-1])

        def score_section(identity, title):
            chunk_workflow.run(core, argparse.Namespace(command="merge-section", work=self.work,
                section=identity, title=title, replace=False, actor="main"))
            path = self.root / (identity + ".review.json")
            write_json(path, {**self.flow().review_state(identity), "reviews": synthetic_reviews(core.STAGES + ("heading",)),
                              "scorecard": synthetic_scorecard([identity]), "book_reviews": synthetic_reviews(core.BOOK_STAGES)})
            chunk_workflow.run(core, argparse.Namespace(command="review-section", work=self.work,
                section=identity, input=path, actor="main"))

        for _ in range(3):
            self.accept(self.saved_job())
        score_section("s0000", "序章")
        jobs = [self.saved_job() for _ in range(10)]
        self.assertEqual([row["chunk_id"] for row in jobs], section_ids[:10])
        self.assertEqual(self.claim()["action"], "wait_capacity")
        for expected in section_ids[10:]:
            self.accept(jobs.pop(0))
            self.assertEqual(self.invoke("status")["pending"], 9)
            self.assertNotIn("receipt", core.load_json(self.work / "manifest.json")["sections"][1])
            job = self.saved_job()
            self.assertEqual(job["chunk_id"], expected)
            jobs.append(job)
        for job in jobs:
            self.accept(job)
        self.assertEqual(self.invoke("status")["pending"], 0)
        score_section("s0001", "第1章 約定（1）")
        self.assertIn("receipt", core.load_json(self.work / "manifest.json")["sections"][1])


if __name__ == "__main__":
    unittest.main()
