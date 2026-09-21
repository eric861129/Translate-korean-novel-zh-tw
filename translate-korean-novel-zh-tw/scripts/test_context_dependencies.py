"""驗證資料包的局部失效與差異定位；不替真實譯稿建立驗收。"""

from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import novel_pipeline as core
import section_context
import test_chunk_workflow as fixture
from test_novel_pipeline import write_json, write_rows


class ScopedContextTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.run_root = Path(tempfile.mkdtemp(prefix="novel-context-scope-tests-"))

    def setUp(self):
        self.f = fixture.WorkflowTests("runTest")
        self.f.run_root, self.f._testMethodName = self.run_root, self._testMethodName
        self.f.setUp()
        self.work = self.f.work

    def prepare(self, identity="s0001", terms=None):
        return self.f.prepare(identity, terms)

    def add_term(self, **values):
        term = {**deepcopy(self.f.terms[0]), "id": "extra", "ko": ["하늘"], "zh": "天空", **values}
        self.f.terms.append(term)
        write_rows(self.work / "terminology.jsonl", self.f.terms)
        return term

    def assert_invalid(self, bundle, identity="s0001"):
        with self.assertRaises(core.PipelineError) as caught:
            section_context.require_current_bundle(core, self.work, identity, bundle["context_state"])
        self.assertTrue(caught.exception.details["requires_recheck"])
        return caught.exception.details

    def test_unrelated_term_addition_and_edit_reuse_state_and_save(self):
        before = self.prepare()
        term = self.add_term()
        for action in ("added", "modified"):
            with self.subTest(action=action):
                with patch.object(core, "load_project", wraps=core.load_project) as loading:
                    after = self.prepare()
                self.assertEqual(loading.call_count, 1)
                self.assertEqual(after["cache"], "reused")
                self.assertEqual(after["context_state"], before["context_state"])
            term["zh"] = "蒼穹"
            write_rows(self.work / "terminology.jsonl", self.f.terms)
        chunk = self.f.flow().section_chunks("s0001")[0]
        data = self.f.payload(chunk)
        data["context_state"] = before["context_state"]
        self.f.save(chunk, data)

    def test_new_match_reports_exact_term_and_source_blocks_and_rejects_old_state(self):
        before = self.prepare()
        self.add_term(ko=["내일"], zh="明天")
        report = self.assert_invalid(before)
        change = next(r for r in report["changes"] if r.get("term_id") == "extra")
        expected = [b["id"] for b in before["section"]["blocks"] if "내일" in b["text"]]
        self.assertEqual(change["source_ids"], expected)
        self.assertEqual(change["change"], "added")
        after = self.prepare()
        self.assertEqual(after["change_report"], report)
        self.assertEqual(self.prepare()["change_report"], report)
        self.assertEqual(self.assert_invalid(before), report)

    def test_scope_change_that_introduces_match_invalidates(self):
        term = self.add_term(ko=["목진"], scope={"from": 2, "through": 2})
        before = self.prepare()
        term["scope"]["from"] = 1
        write_rows(self.work / "terminology.jsonl", self.f.terms)
        self.assertEqual(self.assert_invalid(before)["changes"][0]["term_id"], "extra")

    def test_new_alias_and_shared_form_candidates_are_not_missed(self):
        self.f.terms[0]["entity_id"] = "entity-1"
        write_rows(self.work / "terminology.jsonl", self.f.terms)
        before = self.prepare()
        self.add_term(entity_id="entity-1", ko=["별명"], zh="別名")
        after = self.prepare()
        self.assertNotEqual(after["context_state"], before["context_state"])
        self.assertEqual({t["id"] for t in after["terminology"]}, {"person-001", "extra"})
        before = after
        self.add_term(id="candidate", ko=["별명"], zh="另一詞義", decision="pending")
        after = self.prepare()
        self.assertNotEqual(after["context_state"], before["context_state"])
        self.assertIn("candidate", {t["term_id"] for t in after["term_attention"]})

    def test_related_translation_evidence_voice_and_pronoun_changes_report_fields(self):
        for field, value in (("zh", "木鎮"), ("evidence", ["新核對依據"]), ("voice_note", "對長輩用敬語"), ("pronoun_note", "本節她指向甲")):
            before = self.prepare()
            self.f.terms[0][field] = value
            write_rows(self.work / "terminology.jsonl", self.f.terms)
            with self.subTest(field=field):
                report = self.assert_invalid(before)
                self.assertIn(field, report["changes"][0]["fields"])

    def test_explicit_implicit_character_remains_dependency(self):
        term = self.add_term()
        before = self.prepare(terms=["extra"])
        term["evidence"].append("隱含說話者的新依據")
        write_rows(self.work / "terminology.jsonl", self.f.terms)
        change = next(r for r in self.assert_invalid(before)["changes"] if r.get("term_id") == "extra")
        self.assertEqual(change["source_ids"], ["s0001"])

    def test_only_presented_previous_continuity_invalidates_packet(self):
        before = self.prepare("s0002")
        for ordinal in (0, 2):
            self.f.contexts[ordinal]["summary"] += "本包未呈現的補記。"
            write_rows(self.work / "continuity.jsonl", self.f.contexts)
            self.assertEqual(self.prepare("s0002")["context_state"], before["context_state"])
        self.f.contexts[1]["summary"] += "真正前節的新摘要。"
        write_rows(self.work / "continuity.jsonl", self.f.contexts)
        report = self.assert_invalid(before, "s0002")
        self.assertEqual(report["changes"][0]["section_id"], "s0001")
        self.assertEqual(report["changes"][0]["path"], "continuity.jsonl")

    def test_current_and_global_issues_invalidate_but_other_section_does_not(self):
        before = self.prepare()
        issue = {"id": "issue-1", "section_ids": ["s0002"], "status": "resolved", "note": "合成問題已修正。"}
        write_rows(self.work / "reviews.jsonl", [issue])
        self.assertEqual(self.prepare()["context_state"], before["context_state"])
        for ids in (["s0001"], []):
            issue["section_ids"] = ids
            write_rows(self.work / "reviews.jsonl", [issue])
            report = self.assert_invalid(before)
            self.assertEqual(report["changes"][0]["issue_id"], "issue-1")
            before = self.prepare()

    def test_removed_required_summary_reports_missing_previous_section(self):
        before = self.prepare()
        write_rows(self.work / "continuity.jsonl", self.f.contexts[1:])
        report = self.assert_invalid(before)
        self.assertEqual(report["changes"][0]["section_id"], "s0000")
        self.assertEqual(report["changes"][0]["change"], "missing")

    def test_missing_context_state_never_returns_no_recheck_report(self):
        self.prepare()
        with self.assertRaises(core.PipelineError) as caught:
            section_context.require_current_bundle(core, self.work, "s0001", None)
        self.assertTrue(caught.exception.details["requires_recheck"])
        self.assertEqual(caught.exception.details["changes"][0]["kind"], "context_state")

    def test_style_changes_are_located(self):
        before = self.prepare()
        progress = self.f.flow().progress
        progress["style_profile"] = {"pronouns": "須依當下說話者核對"}
        write_json(self.work / "batch-progress.json", progress)
        self.assertIn("style_profile_sha256", {r["kind"] for r in self.assert_invalid(before)["changes"]})

    def test_semantic_rules_invalidate_and_operational_docs_do_not(self):
        before = self.prepare()
        skill = Path(core.__file__).resolve().parent.parent
        original = Path.read_bytes
        for name, changed in (("translation-and-style.md", True), ("maintenance.md", False), ("resume-and-delivery.md", False)):
            path = skill / "references" / name
            def read(target):
                value = original(target)
                return value + b"\n" if target == path else value
            with self.subTest(name=name), patch.object(Path, "read_bytes", read):
                after = self.prepare()
                self.assertEqual(after["context_state"] != before["context_state"], changed)
                if changed:
                    self.assertIn("references/" + name, {r.get("path") for r in after["change_report"]["changes"]})
            before = self.prepare()

    def test_original_inputs_change_strictly_rejected_with_path(self):
        for label in ("source", "reference"):
            path = getattr(self.f, label)
            original = path.read_bytes()
            path.write_bytes(original + b"\n")
            with self.subTest(label=label), self.assertRaises(core.PipelineError) as caught:
                self.prepare()
            self.assertEqual(caught.exception.details["changes"][0]["kind"], label)
            self.assertEqual(caught.exception.details["changes"][0]["path"], str(path.resolve()))
            path.write_bytes(original)

    def test_existing_receipt_still_checks_earlier_continuity(self):
        for identity in ("s0000", "s0001", "s0002"):
            self.f.complete_section(identity)
        before = self.prepare("s0002")
        self.f.contexts[0]["summary"] += "前序人物知識變更。"
        write_rows(self.work / "continuity.jsonl", self.f.contexts)
        self.assertEqual(self.prepare("s0002")["context_state"], before["context_state"])
        report = core.validate_project(core.load_project(self.work))
        self.assertTrue(report["errors"])

    def test_prepare_does_not_rewrite_progress_receipts_or_maps(self):
        self.f.complete_section("s0000")
        before = self.prepare()
        paths = [self.work / "manifest.json", self.work / "batch-progress.json", *self.work.glob("sections/*"), *self.work.glob("chunks/*")]
        originals = {p: p.read_bytes() for p in paths}
        self.add_term(ko=["내일"], zh="明天")
        after = self.prepare()
        self.assertNotEqual(before["context_state"], after["context_state"])
        self.assertEqual(originals, {p: p.read_bytes() for p in paths})

    def test_legacy_dependencies_require_one_recheck_without_reset(self):
        before = self.prepare()
        path = Path(before["path"])
        packet = core.load_json(path)
        packet["dependencies"]["schema_version"] = 1
        packet["dependencies"]["terms"] = "legacy-file-hash"
        write_json(path, packet)
        after = self.prepare()
        self.assertEqual([r["kind"] for r in after["change_report"]["changes"]], ["dependency_policy"])
        self.assertEqual(self.prepare()["cache"], "reused")

    def test_corrupt_packets_rebuild_without_silent_trust(self):
        before = self.prepare()
        path = Path(before["path"])
        for bad in (None, {"dependencies": []}, {**core.load_json(path), "payload": {"terminology": 1}}):
            write_json(path, bad)
            with self.subTest(bad=bad):
                after = self.prepare()
                self.assertEqual(after["cache"], "rebuilt")
                self.assertTrue(after["change_report"]["requires_recheck"])

    def test_same_operation_new_match_returns_changed_location_and_no_packet(self):
        original = section_context.build_payload
        def build(*args, **kwargs):
            value = original(*args, **kwargs)
            self.add_term(ko=["내일"], zh="明天")
            return value
        with patch.object(section_context, "build_payload", side_effect=build), self.assertRaises(core.PipelineError) as caught:
            self.prepare()
        self.assertEqual(caught.exception.details["changes"][0]["term_id"], "extra")
        self.assertFalse((self.work / "context/s0001.json").exists())

    def test_term_details_cli_returns_structured_stale_location(self):
        before = self.prepare()
        self.f.terms[0]["zh"] = "木鎮"
        write_rows(self.work / "terminology.jsonl", self.f.terms)
        result = subprocess.run([sys.executable, "-X", "utf8", core.__file__, "term-details", "--work", str(self.work),
                                 "--section", "s0001", "--context-state", before["context_state"], "--term-id", "person-001"], capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        error = json.loads(result.stderr.decode("utf-8"))
        self.assertEqual(error["details"]["changes"][0]["fields"], ["zh"])


if __name__ == "__main__":
    unittest.main()
