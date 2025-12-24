"""
示例插件3：数据增强器
为数据添加额外字段和计算值
"""

from typing import Dict, Any, Optional, List, Tuple
import sys
import os
import hashlib
import time

# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from plugins.plugin import Plugin


class Data_enricher(Plugin):
    """数据增强插件"""

    def __init__(self):
        super().__init__()
        self.stats["total_processed"] = 0
        self.stats["enriched"] = 0

    def single_line_process(
        self, line: Dict[str, Any], output_paths: List[str]
    ) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
        """
        为每条数据添加额外字段
        """
        self.stats["total_processed"] += 1

        result = line.copy()

        # 添加处理时间戳
        result["processed_at"] = int(time.time() * 1000)

        # 计算内容哈希
        content_str = str(sorted(line.items()))
        result["content_hash"] = hashlib.md5(content_str.encode()).hexdigest()[:8]

        # 计算字段数量
        result["field_count"] = len(line)

        # 如果有数值字段，计算统计信息
        numeric_values = [v for v in line.values() if isinstance(v, (int, float))]
        if numeric_values:
            result["numeric_sum"] = sum(numeric_values)
            result["numeric_avg"] = sum(numeric_values) / len(numeric_values)

        self.stats["enriched"] += 1
        return result, 0
