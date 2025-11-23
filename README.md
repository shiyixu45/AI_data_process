# 数据处理框架

一个支持 JSONL 和 Parquet 格式的高性能并行数据处理框架，具有断点续传、实时日志统计等功能。

## 功能特性

- ✅ 支持 JSONL 和 Parquet 两种文件格式
- ✅ 多进程并行处理，提高处理效率
- ✅ 按指定字段排序后处理
- ✅ 断点续传功能，支持中断后继续处理
- ✅ 实时日志统计（每10秒输出处理速度、已处理行数等）
- ✅ 自动保存日志文件（插件名+时间戳）
- ✅ 插件化设计，易于扩展自定义处理逻辑
- ✅ 支持自定义统计变量

## 文件结构

```
.
├── plugin.py           # 插件基类
├── data_process.py     # 主处理框架
├── example.py          # 示例插件
└── README.md           # 本文档
```

## 安装依赖

```bash
# 基础依赖（处理 JSONL 文件不需要额外依赖）
pip install pyarrow  # 仅在处理 Parquet 文件时需要
```

## 使用方法

### 1. 创建自定义插件

继承 `Plugin` 基类并实现 `single_line_process` 方法：

```python
from plugin import Plugin
from typing import Dict, Any, Optional

class Myplugin(Plugin):
    def __init__(self):
        super().__init__()
        # 定义统计变量
        self.stats['processed_count'] = 0
        self.stats['filtered_count'] = 0
    
    def single_line_process(self, line: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        处理单行数据
        
        Args:
            line: 输入的一行数据（字典格式）
        
        Returns:
            处理后的数据（字典格式），返回 None 表示过滤掉该行
        """
        self.stats['processed_count'] += 1
        
        # 自定义处理逻辑
        if line.get('some_field') == 'some_value':
            self.stats['filtered_count'] += 1
            return None  # 过滤掉
        
        # 处理数据
        result = line.copy()
        result['new_field'] = 'processed'
        return result
```

### 2. 运行数据处理

```bash
python data_process.py \
    --input input.jsonl \
    --output output.jsonl \
    --stats stats.json \
    --plugin myplugin \
    --type jsonl \
    --sort-key id \
    --workers 4
```

### 参数说明

- `--input`: 输入文件路径
- `--output`: 输出文件路径
- `--stats`: 统计信息输出路径（JSON格式）
- `--plugin`: 插件名称（不含.py后缀）
- `--type`: 文件类型，可选 `jsonl` 或 `parquet`
- `--sort-key`: 用于排序的字段名
- `--workers`: 并行进程数（默认4）

## 示例

### 使用示例插件处理 JSONL 文件

1. 创建测试数据文件 `test_input.jsonl`：

```jsonl
{"id": 1, "name": "Alice", "score": 85}
{"id": 2, "name": "Bob", "score": 55}
{"id": 3, "name": "Charlie", "score": 92}
{"id": 4, "name": "David", "score": 45}
{"id": 5, "name": "Eve", "score": 78}
```

2. 运行处理：

```bash
python data_process.py \
    --input test_input.jsonl \
    --output test_output.jsonl \
    --stats test_stats.json \
    --plugin example \
    --type jsonl \
    --sort-key id \
    --workers 2
```

3. 查看输出文件 `test_output.jsonl`：

```jsonl
{"id": 1, "name": "Alice", "score": 85, "status": "passed", "grade": "B"}
{"id": 3, "name": "Charlie", "score": 92, "status": "passed", "grade": "A"}
{"id": 5, "name": "Eve", "score": 78, "status": "passed", "grade": "C"}
```

4. 查看统计文件 `test_stats.json`：

```json
{
  "total_processed": 5,
  "filtered_out": 2,
  "passed_through": 3,
  "processing_time_seconds": 0.15,
  "average_speed_rows_per_second": 33.33
}
```

## 日志输出

处理过程中会自动生成日志文件（例如 `example_20231124_153045.log`），包含：

```
2023-11-24 15:30:45 - INFO - 日志文件: example_20231124_153045.log
2023-11-24 15:30:45 - INFO - ============================================================
2023-11-24 15:30:45 - INFO - 开始数据处理
2023-11-24 15:30:45 - INFO - 输入文件: test_input.jsonl
2023-11-24 15:30:45 - INFO - 输出文件: test_output.jsonl
...
2023-11-24 15:30:55 - INFO - 处理速度: 120.50 行/秒 | 已处理: 1205 行 | 已输出: 980 行 | 运行时间: 10.00 秒
...
```

## 断点续传

如果处理过程中被中断（Ctrl+C 或程序崩溃），框架会在输出文件旁创建一个 `.checkpoint` 文件记录进度。再次运行相同命令时，会自动从上次中断的位置继续处理。

## 高级特性

### 自定义统计变量

在插件的 `__init__` 方法中定义统计变量：

```python
def __init__(self):
    super().__init__()
    self.stats['success_count'] = 0
    self.stats['error_count'] = 0
    self.stats['custom_metric'] = 0
```

这些统计变量会自动汇总并保存到统计文件中。

### 处理 Parquet 文件

```bash
python data_process.py \
    --input data.parquet \
    --output processed.parquet \
    --stats stats.json \
    --plugin myplugin \
    --type parquet \
    --sort-key timestamp \
    --workers 8
```

## 注意事项

1. 插件文件名必须与插件名一致（例如 `myplugin.py` 对应插件名 `myplugin`）
2. 插件类名应为插件名的首字母大写形式（例如 `Myplugin`）
3. `single_line_process` 方法返回 `None` 表示该行不输出
4. 数据会按 `--sort-key` 指定的字段排序后处理
5. 建议根据数据量和机器配置调整 `--workers` 参数

## 许可

MIT License
