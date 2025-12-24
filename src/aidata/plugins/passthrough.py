"""
示例插件5：简单透传插件
不做任何处理，直接输出到第一个文件（用于测试和基准测试）
"""

from typing import Dict, Any, Optional, List, Tuple
from aidata.plugins.plugin import Plugin


class Passthrough(Plugin):
    """透传插件"""

    def __init__(self):
        super().__init__()
        self.stats["total_processed"] = 0

    def single_line_process(
        self, line: Dict[str, Any], output_paths: List[str]
    ) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
        """直接透传数据"""
        self.stats["total_processed"] += 1
        return line, 0
