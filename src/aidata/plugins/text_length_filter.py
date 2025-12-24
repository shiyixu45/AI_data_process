"""
示例插件2：文本长度过滤器
根据文本字段长度过滤数据
"""

from typing import Dict, Any, Optional, List, Tuple
from aidata.plugins.plugin import Plugin


class Text_length_filter(Plugin):
    """文本长度过滤插件"""

    def __init__(self):
        super().__init__()
        self.stats["total_processed"] = 0
        self.stats["short"] = 0
        self.stats["medium"] = 0
        self.stats["long"] = 0
        self.min_length = 10
        self.max_length = 1000

    def single_line_process(
        self, line: Dict[str, Any], output_paths: List[str]
    ) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
        """
        根据文本长度分类输出

        输出路径:
        - 0: 短文本 (<100字符)
        - 1: 中等文本 (100-500字符)
        - 2: 长文本 (>500字符)
        """
        self.stats["total_processed"] += 1

        text = line.get("text", "") or line.get("content", "") or ""
        length = len(text)

        # 过滤过短的文本
        if length < self.min_length:
            return None, None

        # 过滤过长的文本
        if length > self.max_length:
            return None, None

        result = line.copy()
        result["text_length"] = length

        if length < 100:
            result["length_category"] = "short"
            self.stats["short"] += 1
            return result, 0
        elif length < 500:
            result["length_category"] = "medium"
            self.stats["medium"] += 1
            return result, min(1, len(output_paths) - 1)
        else:
            result["length_category"] = "long"
            self.stats["long"] += 1
            return result, min(2, len(output_paths) - 1)
