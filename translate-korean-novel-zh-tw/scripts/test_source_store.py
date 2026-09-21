"""合成來源包、格式遷移與交付等價測試，不代表真實小說的語意驗收。"""

import argparse
from copy import deepcopy
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import novel_pipeline as core
import source_store
import chunk_workflow
import test_chunk_workflow as workflow_fixture
from test_novel_pipeline import SOURCE, write_json, write_rows, synthetic_reviews, synthetic_scorecard


class PackTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.run_root = Path(tempfile.mkdtemp(prefix="novel-source-packs-tests-"))

    def setUp(self):
        self.f = workflow_fixture.WorkflowTests("runTest")
        self.f.run_root, self.f._testMethodName = self.run_root, self._testMethodName
        self.f.setUp()
        self.work, self.root = self.f.work, self.f.root

    def migrate(self, apply=True, command="migrate-source", checkpoint=True):
        return source_store.migrate(core, argparse.Namespace(work=self.work, command=command, apply=apply, checkpoint=checkpoint))

    def prime(self):
        flow = self.f.flow()
        flow.reading_records()
        flow.verification.flush(flow.manifest)
        return flow

    def paths(self):
        manifest = core.load_json(self.work / "manifest.json")
        store = source_store.SourceStore(core, self.work, manifest)
        return store, [store.path(name) for name in store.packs]

    def test_group_limits_offsets_and_unicode_preserve_original_records(self):
        manifest = deepcopy(self.f.manifest)
        policy = {"target_bytes": 600, "max_bytes": 950, "max_sections": 2}
        packed, payloads = source_store.build(core, manifest, policy)
        store = source_store.SourceStore(core, self.work, packed, payloads=payloads)
        for section in manifest["sections"]:
            self.assertEqual(store.section(section["id"]), section)
        for pack in packed["source_storage"]["packs"]:
            self.assertLessEqual(pack["sections"], 2)
            self.assertTrue(pack["bytes"] <= 950 or pack["oversized_section"])
        self.assertEqual(source_store.build(core, manifest, policy), (packed, payloads))

    def test_default_policy_is_two_mib_with_bounded_pack_count(self):
        self.assertEqual(source_store.POLICY, {"target_bytes": 2097152, "max_bytes": 4194304, "max_sections": 100})
        manifest = deepcopy(self.f.manifest)
        original = manifest["sections"][0]
        manifest["sections"] = []
        for i in range(205):
            section = deepcopy(original)
            section.update(id=f"s{i:04d}", ordinal=i)
            for j, block in enumerate(section["blocks"], 1):
                block["id"] = f"s{i:04d}:p{j:04d}"
            manifest["sections"].append(section)
        packed, _ = source_store.build(core, manifest)
        self.assertEqual([p["sections"] for p in packed["source_storage"]["packs"]], [100, 100, 5])

    def test_preview_does_not_create_packs_backup_or_modify_progress(self):
        before = source_store.mutable_state(core, self.work)
        result = self.migrate(apply=False)
        self.assertEqual(result["status"], "preview")
        self.assertEqual(source_store.mutable_state(core, self.work), before)
        self.assertFalse((self.work / "context").exists())
        self.assertFalse((self.work / ".migration").exists())

    def test_input_encodings_and_crlf_preserve_byte_offsets_and_source_bytes(self):
        for encoding in ("utf-16", "cp949", "utf-8-sig"):
            with self.subTest(encoding=encoding):
                source = self.root / (encoding + ".txt")
                text = SOURCE.replace("\n", "\r\n")
                source.write_bytes(text.encode(encoding))
                before = source.read_bytes()
                work = self.root / (encoding + "-work")
                core.init_project(argparse.Namespace(source=source, reference=self.f.reference, work=work,
                                  title="編碼驗證", source_encoding=encoding, reference_encoding="auto", heading_pattern=None))
                project = core.load_project(work)
                expected = core.parse_source(text, None)
                self.assertEqual([project.source_store.section(s["id"]) for s in expected], expected)
                self.assertEqual(source.read_bytes(), before)
                for pack in project.source_store.packs:
                    self.assertNotIn(b"\r\n", project.source_store.path(pack).read_bytes())

    def test_pack_and_section_caches_are_bounded_during_random_access(self):
        manifest = deepcopy(self.f.manifest)
        template = manifest["sections"][0]
        manifest["sections"] = []
        for i in range(8):
            section = deepcopy(template)
            section.update(id=f"s{i:04d}", ordinal=i)
            manifest["sections"].append(section)
        packed, payloads = source_store.build(core, manifest, {"target_bytes": 1000, "max_bytes": 1500, "max_sections": 1})
        store = source_store.SourceStore(core, self.work, packed, payloads=payloads)
        for section in manifest["sections"] + manifest["sections"][:1]:
            self.assertEqual(store.section(section["id"]), section)
            self.assertLessEqual(len(store._packs), 2)
            self.assertLessEqual(len(store._sections), 4)

    def test_migration_preserves_artifact_bytes_receipts_and_reading(self):
        self.f.complete_section("s0000")
        before = {p.relative_to(self.work): p.read_bytes() for p in self.work.rglob("*")
                  if p.is_file() and p.name != "manifest.json" and "context" not in p.parts}
        report = core.validate_project(core.load_project(self.work))
        reading = self.f.flow().reading_records()
        self.migrate()
        after = core.load_project(self.work)
        self.assertEqual(core.validate_project(after), report)
        self.assertEqual(self.f.flow().reading_records(), reading)
        for path, raw in before.items():
            self.assertEqual((self.work / path).read_bytes(), raw)
        self.assertEqual(after[1]["sections"][0]["receipt"], core.load_json(self.work / ".migration/manifest-v1.json")["sections"][0]["receipt"])
        self.assertNotIn("blocks", after[1]["sections"][0])

    def test_warm_workflow_loads_no_body_until_current_chunk_is_requested(self):
        self.migrate()
        self.prime()
        with patch.object(core, "parse_source", side_effect=AssertionError("不可重新解析全書")):
            flow = self.f.flow()
            store = flow.project.source_store
            self.assertEqual(store.decoded_sections, set())
            self.assertEqual(store.pack_reads, 0)
            chunk = flow.views[self.f.plan["chunks"][0]["id"]]
            self.assertEqual(store.decoded_sections, {chunk["id"]})
            self.assertEqual(store.pack_reads, 1)

    def test_unchanged_historical_review_does_not_load_source_bodies(self):
        self.f.complete_section("s0000")
        self.migrate()
        expected = self.prime().reading_records()
        flow = self.f.flow()
        self.assertEqual(flow.reading_records(), expected)
        self.assertEqual(flow.project.source_store.decoded_sections, set())

    def test_new_matching_term_rescans_and_invalidates_historical_reading(self):
        self.f.complete_section("s0000")
        self.migrate()
        self.prime()
        self.f.terms.append({"id": "new", "ko": ["문"], "zh": "門", "decision": "adopted", "basis": "合成", "evidence": ["合成"]})
        write_rows(self.work / "terminology.jsonl", self.f.terms)
        flow = self.f.flow()
        self.assertEqual(flow.reading_records(), {})
        self.assertEqual(flow.project.source_store.decoded_sections, {"s0000"})

    def test_bundle_stores_reference_but_returns_complete_or_paged_source(self):
        self.migrate()
        result = self.f.prepare()
        packet = core.load_json(Path(result["path"]))
        self.assertNotIn("blocks", packet["payload"]["section"])
        self.assertIn("source_record", packet["payload"]["section"])
        self.assertEqual(result["section"]["blocks"], self.f.manifest["sections"][0]["blocks"])
        ids = [b["id"] for b in result["section"]["blocks"]]
        part = self.f.prepare(first=ids[-1], last=ids[-1])
        self.assertTrue(part["section"]["partial"])
        self.assertEqual(len(part["section"]["blocks"]), 1)
        self.assertEqual(part["context_state"], result["context_state"])

    def test_save_merge_and_review_work_with_packed_source_bundle(self):
        self.migrate()
        bundle = self.f.prepare()
        for chunk in self.f.flow().section_chunks("s0000"):
            data = self.f.payload(chunk)
            data["context_state"] = bundle["context_state"]
            self.f.submit(chunk, data, merge=True, title="序章")
        self.f.review()
        self.assertEqual(core.load_json(self.work / "manifest.json")["schema_version"], 2)
        self.assertTrue(self.f.flow().reading_records())

    def test_missing_and_corrupt_pack_rebuild_without_altering_manifest_or_artifacts(self):
        self.migrate()
        self.prime()
        store, paths = self.paths()
        before = source_store.mutable_state(core, self.work)
        for corrupt in (False, True):
            if corrupt:
                paths[0].write_bytes(b"damaged")
            else:
                paths[0].unlink()
            with self.subTest(corrupt=corrupt), self.assertRaisesRegex(core.PipelineError, "rebuild-source"):
                self.f.flow().views[self.f.plan["chunks"][0]["id"]]
            source_store.rebuild(core, argparse.Namespace(work=self.work))
            self.assertEqual(source_store.mutable_state(core, self.work), before)
            self.assertEqual(core.load_project(self.work).source_store.section("s0000")["blocks"], self.f.manifest["sections"][0]["blocks"])

    def test_changed_mother_file_rejects_reuse_and_rebuild_even_if_mtime_matches(self):
        import os
        self.migrate()
        self.prime()
        for path in (self.f.source, self.f.reference):
            raw, stat = path.read_bytes(), path.stat()
            changed = raw.replace("목".encode(), "문".encode()) if path == self.f.source else raw.replace("木".encode(), "林".encode())
            path.write_bytes(changed)
            os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
            with self.assertRaises(core.PipelineError):
                self.f.flow()
            with self.assertRaises(core.PipelineError):
                source_store.rebuild(core, argparse.Namespace(work=self.work))
            path.write_bytes(raw)

    def test_index_reorder_omission_offset_or_pack_hash_cannot_be_repaired_into_truth(self):
        self.migrate()
        original = core.load_json(self.work / "manifest.json")
        mutations = [lambda m: m["sections"].reverse(), lambda m: m["sections"].pop(),
                     lambda m: m["sections"][0]["source_record"].update(offset=1),
                     lambda m: m["source_storage"]["packs"][0].update(sha256="0" * 64)]
        for mutate in mutations:
            bad = deepcopy(original)
            mutate(bad)
            write_json(self.work / "manifest.json", bad)
            with self.assertRaisesRegex(core.PipelineError, "原稿索引"):
                core.load_project(self.work)
            with self.assertRaisesRegex(core.PipelineError, "原稿索引"):
                source_store.rebuild(core, argparse.Namespace(work=self.work))
        write_json(self.work / "manifest.json", original)

    def test_source_pack_path_traversal_is_rejected(self):
        self.migrate()
        store, _ = self.paths()
        for name in ("../manifest.json", "part-0001.jsonl/../manifest.json", str(self.f.source)):
            with self.assertRaises(core.PipelineError):
                store.path(name)

    def test_apply_requires_checkpoint_and_writer_lock(self):
        with self.assertRaisesRegex(core.PipelineError, "保存點"):
            self.migrate(checkpoint=False)
        with core.writer_lock(self.work), self.assertRaisesRegex(core.PipelineError, "已鎖定"):
            self.migrate()
        self.assertEqual(core.load_json(self.work / "manifest.json")["schema_version"], 1)

    def test_partial_pack_write_failure_leaves_v1_and_retry_is_safe(self):
        original = source_store.SourceStore.publish
        def interrupted(store, payloads):
            original(store, payloads)
            raise OSError("模擬切換前中斷")
        before = (self.work / "manifest.json").read_bytes()
        with patch.object(source_store.SourceStore, "publish", interrupted), self.assertRaises(OSError):
            self.migrate()
        self.assertEqual((self.work / "manifest.json").read_bytes(), before)
        self.assertEqual(self.migrate()["status"], "migrated")
        self.assertEqual(self.migrate()["status"], "already_current")

    def test_external_progress_change_aborts_before_manifest_switch(self):
        original = source_store.SourceStore.publish
        def changed(store, payloads):
            original(store, payloads)
            write_rows(self.work / "reviews.jsonl", [{"id": "new", "section_ids": [], "status": "open", "note": "外部修改"}])
        with patch.object(source_store.SourceStore, "publish", changed), self.assertRaisesRegex(core.PipelineError, "工作檔改變"):
            self.migrate()
        self.assertEqual(core.load_json(self.work / "manifest.json")["schema_version"], 1)

    def test_restore_inline_uses_latest_receipt_instead_of_old_backup(self):
        self.migrate()
        self.f.complete_section("s0000")
        receipt = core.load_json(self.work / "manifest.json")["sections"][0]["receipt"]
        progress = (self.work / "batch-progress.json").read_bytes()
        self.migrate(command="restore-inline")
        manifest = core.load_json(self.work / "manifest.json")
        self.assertEqual(manifest["schema_version"], 1)
        self.assertEqual(manifest["sections"][0]["receipt"], receipt)
        self.assertEqual((self.work / "batch-progress.json").read_bytes(), progress)
        self.assertEqual(core.validate_project(core.load_project(self.work))["verified"], 1)

    def test_existing_different_migration_backup_is_not_overwritten(self):
        path = self.work / ".migration/manifest-v1.json"
        path.parent.mkdir()
        path.write_text("已有備份", encoding="utf-8")
        with self.assertRaisesRegex(core.PipelineError, "不同的遷移備份"):
            self.migrate()
        self.assertEqual(path.read_text(encoding="utf-8"), "已有備份")

    def test_packed_final_output_equals_legacy_and_full_checks_bypass_cache(self):
        for section in self.f.manifest["sections"]:
            self.f.complete_section(section["id"])
        legacy = self.root / "legacy-copy"
        shutil.copytree(self.work, legacy)
        self.migrate()
        outputs = []
        for work, name in ((legacy, "舊版完整譯本.txt"), (self.work, "新版完整譯本.txt")):
            with patch("verification_cache.VerificationCache.get", side_effect=AssertionError("交付不可用快取")):
                state = core.book_state(core.load_project(work))
                write_json(work / "book-review.json", {"state": state, "reviews": synthetic_reviews(core.BOOK_STAGES),
                           "scorecard": synthetic_scorecard(["s0000"])})
                core.attest_book(argparse.Namespace(work=work))
                output = self.root / name
                core.assemble_book(argparse.Namespace(work=work, output=output))
                outputs.append(output.read_bytes())
        self.assertEqual(outputs[0], outputs[1])
        with self.assertRaisesRegex(core.PipelineError, "全書收據"):
            self.migrate(command="restore-inline")

    def test_packed_section_and_book_still_reject_scores_below_85(self):
        self.migrate()
        self.f.save_section("s0000")
        self.f.merge()
        review = self.f.review_payload("s0000")
        review["scorecard"] = synthetic_scorecard(["s0000"], 84.99)
        with self.assertRaisesRegex(core.PipelineError, "未達 85"):
            self.f.review(data=review)
        self.assertNotIn("receipt", core.load_json(self.work / "manifest.json")["sections"][0])
        self.f.review()
        for section in self.f.manifest["sections"][1:]:
            self.f.complete_section(section["id"])
        state = core.book_state(core.load_project(self.work))
        write_json(self.work / "book-review.json", {"state": state, "reviews": synthetic_reviews(core.BOOK_STAGES),
                   "scorecard": synthetic_scorecard(["s0000"], 84.99)})
        with self.assertRaisesRegex(core.PipelineError, "未達 85"):
            core.attest_book(argparse.Namespace(work=self.work))
        self.assertFalse((self.work / "book-receipt.json").exists())

    def test_init_cli_defaults_to_packs_and_commands_return_structured_results(self):
        work = self.root / "new-work"
        cmd = [sys.executable, "-X", "utf8", str(Path(core.__file__))]
        result = subprocess.run(cmd + ["init", "--source", str(self.f.source), "--reference", str(self.f.reference),
                                      "--work", str(work), "--title", "合成"], capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["schema_version"], 2)
        result = subprocess.run(cmd + ["show", "--work", str(work), "--section", "s0000"], capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(json.loads(result.stdout)["blocks"]), 2)
        result = subprocess.run(cmd + ["migrate-source", "--work", str(self.work)], capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["status"], "preview")


if __name__ == "__main__":
    unittest.main()
