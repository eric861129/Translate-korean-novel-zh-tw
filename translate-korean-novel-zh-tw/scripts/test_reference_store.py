"""以合成多編碼文本驗證人工版定位、快取與版本保護。"""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import novel_pipeline as core
import reference_store
from test_novel_pipeline import write_json


class ReferenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.run_root = Path(tempfile.mkdtemp(prefix="novel-reference-tests-"))

    def setUp(self):
        self.root = self.run_root / self._testMethodName
        self.root.mkdir()
        self.work = self.root / "translation-work"
        self.work.mkdir()
        self.reference, self.source = self.root / "人工版.txt", self.root / "韓文.txt"
        self.source.write_text("합성 원문", encoding="utf-8")
        self.install("第一行\n\n木真說：\r\n「明天回來。」\r第五行\n木真回來了。")

    def install(self, text, encoding="utf-8"):
        self.reference.write_bytes(text.encode(encoding))
        self.manifest = {"schema_version": 2, "source": {"path": str(self.source), "sha256": core.file_digest(self.source), "encoding": "utf-8"},
                         "reference": {"path": str(self.reference), "sha256": core.file_digest(self.reference), "encoding": encoding}}
        write_json(self.work / "manifest.json", self.manifest)

    def store(self):
        return reference_store.ReferenceStore(core, self.work)

    def command(self, name, *args):
        result = subprocess.run([sys.executable, "-X", "utf8", core.__file__, name, "--work", str(self.work), *args], capture_output=True)
        return result, json.loads(result.stdout.decode("utf-8"))

    def test_multi_query_search_returns_coalesced_complete_fragments(self):
        result = self.store().search(["木真", "明天"], before=1, after=1)
        self.assertEqual([r["lines"] for r in result["results"]], [[3, 6], [4]])
        self.assertEqual(result["fragments"][0]["range"], [2, 6])
        self.assertEqual([r["text"] for r in result["fragments"][0]["lines"]], ["", "木真說：", "「明天回來。」", "第五行", "木真回來了。"])
        self.assertEqual(result["line_count"], 6)
        self.assertFalse(result["results"][0]["more"])

    def test_encodings_bom_newlines_and_sparse_seek_are_exact(self):
        for encoding in ("utf-8", "utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "utf-32", "utf-32-be", "cp949"):
            lines = [f"목진 {i}" + (" 가" * 1000 if i == 255 else "") for i in range(1, 601)]
            lines[255] = ""
            text = "".join(line + ("\r\n", "\r", "\n")[i % 3] for i, line in enumerate(lines[:-1])) + lines[-1]
            self.install(text, encoding)
            with self.subTest(encoding=encoding):
                store = self.store()
                self.assertEqual(store.index["line_count"], 600)
                self.assertEqual(len(store.index["anchors"]), 3)
                result = store.extract([(254, 261), (600, 600)], store.state)
                self.assertEqual([r["text"] for r in result["fragments"][0]["lines"]], lines[253:261])
                self.assertEqual(result["fragments"][1]["lines"], [{"line": 600, "text": lines[-1]}])
                self.assertTrue(self.store().reused)

    def test_cold_queries_share_one_scan_warm_query_only_reads_fragments(self):
        self.install("\n".join(f"第{i}行" + (" 木真" if i == 500 else "") + (" 約定" if i == 700 else "") for i in range(1, 1001)))
        store = self.store()
        with patch.object(store, "iter_lines", wraps=store.iter_lines) as reading:
            result = store.search(["木真", "約定"], before=0, after=0)
        scans = [call for call in reading.call_args_list if len(call.args) == 1]
        self.assertEqual(len(scans), 1)
        with patch.object(reference_store.ReferenceStore, "build_index", side_effect=AssertionError("不可重建整檔索引")):
            warm = self.store()
        with patch.object(warm, "iter_lines", wraps=warm.iter_lines) as reading:
            second = warm.search(["木真", "約定"], before=0, after=0)
        self.assertEqual(second["search_cache"], "reused")
        self.assertTrue(all(len(call.args) == 2 for call in reading.call_args_list))
        self.assertEqual(second["fragments"], result["fragments"])

    def test_pagination_has_no_missing_or_duplicate_hits(self):
        self.install("\n".join("木真" if i % 2 else "另一句" for i in range(1, 24)))
        store, cursor, hits = self.store(), 0, []
        while True:
            result = store.search(["木真"], limit=3, after_line=cursor, before=0, after=0)["results"][0]
            hits.extend(result["lines"])
            if not result["more"]:
                self.assertIsNone(result["next_after_line"])
                break
            cursor = result["next_after_line"]
        self.assertEqual(hits, list(range(1, 24, 2)))

    def test_literal_queries_do_not_interpret_regex_or_program_text(self):
        self.install('第一行 a.*b\n只有 ab\n$(不執行) [甲]\n')
        result = self.store().search(["a.*b", "$(不執行)", "[甲]"], before=0, after=0)
        self.assertEqual([r["lines"] for r in result["results"]], [[1], [3], [3]])

    def test_batch_ranges_merge_and_reject_invalid_or_truncated_output(self):
        store = self.store()
        self.assertEqual([r["range"] for r in store.extract([(4, 6), (2, 4), (1, 1)], store.state)["fragments"]], [[1, 6]])
        for ranges in ([(0, 2)], [(5, 4)], [(1, 7)]):
            with self.subTest(ranges=ranges), self.assertRaises(core.PipelineError):
                store.extract(ranges, store.state)
        self.install("行\n" * 2001)
        store = self.store()
        with self.assertRaisesRegex(core.PipelineError, "不會截斷"):
            store.extract([(1, 2001)], store.state)

    def test_invalid_search_parameters_are_rejected(self):
        store = self.store()
        for queries, kwargs in (([], {}), ([" "], {}), (["甲\n乙"], {}), (["木真"], {"limit": 0}), (["木真"], {"after_line": -1}), (["木真"], {"before": 101})):
            with self.subTest(queries=queries, kwargs=kwargs), self.assertRaises(core.PipelineError):
                store.search(queries, **kwargs)

    def test_empty_reference_and_no_hit_search(self):
        self.install("")
        store = self.store()
        result = store.search(["木真"])
        self.assertEqual(result["line_count"], 0)
        self.assertEqual(result["fragments"], [])
        self.assertEqual(result["results"][0]["lines"], [])
        self.assertTrue(self.store().reused)
        with self.assertRaises(core.PipelineError):
            store.extract([(1, 1)], store.state)

    def test_corrupt_index_and_search_cache_rebuild(self):
        self.store().search(["木真"])
        index = self.work / "context/reference/index.json"
        search = self.work / "context/reference/search-cache.json"
        index.write_text("{broken", encoding="utf-8")
        search.write_text("null", encoding="utf-8")
        store = self.store()
        self.assertFalse(store.reused)
        result = store.search(["木真"])
        self.assertEqual(result["results"][0]["lines"], [3, 6])
        packet = core.load_json(index)
        packet["payload"]["anchors"] = [[1, -1]]
        packet["sha256"] = core.digest(core.canonical(packet["payload"]))
        write_json(index, packet)
        self.assertFalse(self.store().reused)

    def test_source_change_even_same_size_and_time_rejects_cached_positions(self):
        self.store().search(["木真"])
        stat = self.reference.stat()
        self.reference.write_bytes(self.reference.read_bytes().replace("木真".encode(), "李真".encode()))
        os.utime(self.reference, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        with self.assertRaises(core.PipelineError) as caught:
            self.store()
        self.assertEqual(caught.exception.details["changes"][0]["kind"], "reference")

    def test_old_range_state_rejected_after_new_manifest_version(self):
        before = self.store().state
        self.install("插入的一行\n木真回來了。")
        with self.assertRaisesRegex(core.PipelineError, "位置版本不符"):
            self.store().extract([(1, 1)], before)

    def test_change_during_search_is_rejected_before_cache_publish(self):
        store = self.store()
        original = store.fragments
        def fragments(ranges):
            result = original(ranges)
            self.reference.write_bytes(self.reference.read_bytes() + b"\n")
            return result
        with patch.object(store, "fragments", side_effect=fragments), self.assertRaises(core.PipelineError):
            store.search(["木真"])
        self.assertFalse((self.work / "context/reference/search-cache.json").exists())

    def test_only_two_bounded_cache_files_and_originals_unchanged(self):
        tracked = [self.work / "manifest.json", self.reference, self.source]
        original = {p: p.read_bytes() for p in tracked}
        store = self.store()
        entries = {core.digest(core.canonical([f"不存在的詞{number}", 20, 0])): [] for number in range(32)}
        store.write_cache("search-cache.json", {"reference_state": store.state, "queries": entries})
        store.search(["第三十三個新查詢"])
        self.assertEqual(original, {p: p.read_bytes() for p in tracked})
        root = self.work / "context/reference"
        self.assertEqual({p.name for p in root.iterdir()}, {"index.json", "search-cache.json"})
        self.assertEqual(len(core.load_json(root / "search-cache.json")["payload"]["queries"]), 32)
        self.assertNotIn(next(iter(entries)), core.load_json(root / "search-cache.json")["payload"]["queries"])

    def test_reference_cache_cannot_overwrite_input(self):
        index = self.work / "context/reference/index.json"
        index.parent.mkdir(parents=True)
        index.write_bytes(self.reference.read_bytes())
        self.manifest["reference"]["path"] = str(index)
        write_json(self.work / "manifest.json", self.manifest)
        before = index.read_bytes()
        with self.assertRaisesRegex(core.PipelineError, "不得覆蓋"):
            self.store()
        self.assertEqual(index.read_bytes(), before)

    def test_cli_batch_search_and_extract(self):
        process, result = self.command("reference-search", "--query", "木真", "--query", "明天", "--before", "0", "--after", "0")
        self.assertEqual(process.returncode, 0, process.stderr)
        process, extracted = self.command("reference-extract", "--reference-state", result["reference_state"], "--range", "2:3", "--range", "6:6")
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual([r["range"] for r in extracted["fragments"]], [[2, 3], [6, 6]])
        self.assertIn("木真說：", extracted["fragments"][0]["lines"][1]["text"])


if __name__ == "__main__":
    unittest.main()
