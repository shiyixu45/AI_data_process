"""
示例插件4：JSON字段提取器
从嵌套JSON中提取特定字段
"""

from typing import Dict, Any, Optional, List, Tuple
import sys
import os

# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from plugins.plugin import Plugin


class Field_extractor(Plugin):
    """字段提取插件"""

    def __init__(self):
        super().__init__()
        self.stats["total_processed"] = 0
        self.stats["extracted"] = 0
        self.stats["skipped"] = 0
        # 要提取的字段列表
        self.fields_to_extract = ["id", "name", "value", "timestamp"]

    def single_line_process(
        self, line: Dict[str, Any], output_paths: List[str]
    ) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
        """
        提取指定字段
        """
        self.stats["total_processed"] += 1

        result = {}
        extracted_count = 0

        for field in self.fields_to_extract:
            if field in line:
                result[field] = line[field]
                extracted_count += 1
            elif "." in field:
                # 支持嵌套字段如 "user.name"
                value = self._get_nested(line, field)
                if value is not None:
                    result[field.replace(".", "_")] = value
                    extracted_count += 1

        if extracted_count == 0:
            self.stats["skipped"] += 1
            return None, None

        self.stats["extracted"] += 1
        return result, 0

    def _get_nested(self, data: Dict, path: str) -> Any:
        """获取嵌套字段值"""
        keys = path.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        return value
