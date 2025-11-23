"""
示例数据处理插件
演示如何使用数据处理框架
"""
from plugin import Plugin
from typing import Dict, Any, Optional


class Example(Plugin):
    """
    示例插件：过滤和转换数据
    
    功能：
    1. 过滤掉某个字段值小于阈值的记录
    2. 为每条记录添加处理时间戳
    3. 统计处理的记录数和过滤掉的记录数
    """
    
    def __init__(self):
        super().__init__()
        # 定义统计变量
        self.stats['total_processed'] = 0
        self.stats['filtered_out'] = 0
        self.stats['passed_through'] = 0
    
    def single_line_process(self, line: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        处理单行数据
        
        示例逻辑：
        - 如果记录中有 'score' 字段且值 >= 60，则保留并添加标记
        - 否则过滤掉该记录
        """
        self.stats['total_processed'] += 1
        
        # 示例：根据 score 字段过滤
        score = line.get('score', 0)
        
        if score >= 60:
            # 保留这条记录，并添加一些处理
            result = line.copy()
            result['status'] = 'passed'
            result['grade'] = self._get_grade(score)
            
            self.stats['passed_through'] += 1
            return result
        else:
            # 过滤掉这条记录
            self.stats['filtered_out'] += 1
            return None
    
    def _get_grade(self, score: float) -> str:
        """根据分数返回等级"""
        if score >= 90:
            return 'A'
        elif score >= 80:
            return 'B'
        elif score >= 70:
            return 'C'
        elif score >= 60:
            return 'D'
        else:
            return 'F'


# 如果你想用不同的类名，也可以这样：
# class MyCustomPlugin(Plugin):
#     def __init__(self):
#         super().__init__()
#         self.stats['custom_stat'] = 0
#     
#     def single_line_process(self, line: Dict[str, Any]) -> Optional[Dict[str, Any]]:
#         self.stats['custom_stat'] += 1
#         # 自定义处理逻辑
#         return line
