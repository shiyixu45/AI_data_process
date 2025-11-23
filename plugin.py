"""
数据处理插件基类
"""

from abc import ABC, abstractmethod
from typing import Dict, Optional, Any


class Plugin(ABC):
    """
    数据处理插件基类

    用户需要继承此类并实现 single_line_process 方法
    可以在子类中定义统计变量来跟踪处理过程
    """

    def __init__(self):
        """初始化插件，可以在子类中定义统计变量"""
        self.stats = {}

    @abstractmethod
    def single_line_process(self, line: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        处理单行数据

        Args:
            line: 输入的一行数据，字典格式

        Returns:
            处理后的数据字典，如果返回 None 则表示该行不输出
        """
        pass

    def get_stats(self) -> Dict[str, Any]:
        """
        获取统计信息

        Returns:
            统计信息字典
        """
        return self.stats

    def reset_stats(self):
        """重置统计信息"""
        self.stats = {}
