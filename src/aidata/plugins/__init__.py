"""插件模块

提供数据处理插件基类和常用插件实现。
"""

from aidata.plugins.plugin import Plugin
from aidata.plugins.passthrough import Passthrough
from aidata.plugins.score_filter import Score_filter
from aidata.plugins.text_length_filter import Text_length_filter
from aidata.plugins.field_extractor import Field_extractor
from aidata.plugins.data_enricher import Data_enricher

__all__ = [
    "Plugin",
    "Passthrough",
    "Score_filter",
    "Text_length_filter",
    "Field_extractor",
    "Data_enricher",
]
