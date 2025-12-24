"""
示例插件1：分数过滤器
根据分数字段过滤数据，>=60分的数据输出到第一个文件，<60分的输出到第二个文件
"""

from typing import Dict, Any, Optional, List, Tuple
import sys
import os

# Add parent directory to path for imports
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from plugins.plugin import Plugin


class Score_filter(Plugin):
    """分数过滤插件"""

    def __init__(self):
        super().__init__()
        self.stats["total_processed"] = 0
        self.stats["passed"] = 0
        self.stats["failed"] = 0

    def single_line_process(
        self, line: Dict[str, Any], output_paths: List[str]
    ) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
        """
        处理单行数据

        Args:
            line: 输入数据
            output_paths: 输出路径列表

        Returns:
            (处理后数据, 输出路径序号) 或 (None, None)
        """
        self.stats["total_processed"] += 1

        score = line.get("score", 0)
        result = line.copy()

        if score >= 60:
            result["status"] = "passed"
            result["grade"] = self._get_grade(score)
            self.stats["passed"] += 1
            return result, 0  # 输出到第一个文件
        else:
            result["status"] = "failed"
            result["grade"] = "F"
            self.stats["failed"] += 1
            # 如果有第二个输出文件，输出到那里；否则丢弃
            if len(output_paths) > 1:
                return result, 1  # 输出到第二个文件
            return None, None  # 丢弃

    def _get_grade(self, score: float) -> str:
        """根据分数返回等级"""
        if score >= 90:
            return "A"
        elif score >= 80:
            return "B"
        elif score >= 70:
            return "C"
        elif score >= 60:
            return "D"
        return "F"
