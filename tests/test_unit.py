"""
单元测试
"""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aidata.plugins.plugin import Plugin
from aidata.data_process.processor import DataProcessor


class TestPlugin(Plugin):
    """测试用插件"""

    def __init__(self):
        super().__init__()
        self.stats["count"] = 0

    def single_line_process(self, line, output_paths):
        self.stats["count"] += 1
        return line, 0


class TestDataProcessor(unittest.TestCase):
    """DataProcessor 单元测试"""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.input_file = os.path.join(self.test_dir, "input.jsonl")
        self.output_file = os.path.join(self.test_dir, "output.jsonl")
        self.stats_file = os.path.join(self.test_dir, "stats.json")

        # Create test plugin file
        self.plugin_file = os.path.join(self.test_dir, "test_plugin.py")
        with open(self.plugin_file, "w") as f:
            f.write(
                """
from typing import Dict, Any, Optional, List, Tuple

class Test_plugin:
    def __init__(self):
        self.stats = {'count': 0}
    
    def single_line_process(self, line, output_paths):
        self.stats['count'] += 1
        return line, 0
    
    def get_stats(self):
        return self.stats
"""
            )

    def tearDown(self):
        import shutil

        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_simple_processing(self):
        """测试简单数据处理"""
        # Create input data
        with open(self.input_file, "w") as f:
            for i in range(100):
                f.write(json.dumps({"id": i, "value": i * 2}) + "\n")

        processor = DataProcessor(
            input_path=self.input_file,
            output_paths=[self.output_file],
            stats_path=self.stats_file,
            plugin_name="test_plugin",
            file_type="jsonl",
            num_workers=2,
            keep_order=False,
            plugin_dir=self.test_dir,
            batch_size=20,
        )
        processor.process()

        # Verify output
        output_count = 0
        with open(self.output_file, "r") as f:
            for line in f:
                output_count += 1

        self.assertEqual(output_count, 100)

    def test_keep_order(self):
        """测试保持顺序"""
        # Create input data
        with open(self.input_file, "w") as f:
            for i in range(50):
                f.write(json.dumps({"id": i}) + "\n")

        processor = DataProcessor(
            input_path=self.input_file,
            output_paths=[self.output_file],
            stats_path=self.stats_file,
            plugin_name="test_plugin",
            file_type="jsonl",
            num_workers=2,
            keep_order=True,
            plugin_dir=self.test_dir,
            batch_size=10,
        )
        processor.process()

        # Verify order
        ids = []
        with open(self.output_file, "r") as f:
            for line in f:
                data = json.loads(line.strip())
                ids.append(data["id"])

        self.assertEqual(ids, list(range(50)))


if __name__ == "__main__":
    unittest.main()
