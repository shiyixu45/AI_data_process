# 数据处理框架

一个支持 JSONL 和 Parquet 格式的高性能并行数据处理框架，具有断点续传、实时日志统计、多输出路径等功能。

## 功能特性

- ✅ 支持 JSONL 和 Parquet 两种文件格式
- ✅ 多进程并行处理，提高处理效率
- ✅ 流式读取，支持超大文件处理
- ✅ 多输出路径支持，插件可按需分发数据
- ✅ 可选的顺序保持模式（--keep-order）
- ✅ 断点续传功能，支持中断后继续处理
- ✅ 实时日志统计（每10秒输出处理速度、已处理行数等）
- ✅ 插件化设计，易于扩展自定义处理逻辑
- ✅ 高性能IO优化（大缓冲区读写）

## 项目结构

```
.
├── src/
│   ├── data_process/
│   │   ├── __init__.py
│   │   ├── plugin.py         # 插件基类
│   │   └── processor.py      # 主处理框架
│   └── plugins/
│       ├── score_filter.py   # 示例：分数过滤器
│       ├── text_length_filter.py  # 示例：文本长度过滤器
│       ├── data_enricher.py  # 示例：数据增强器
│       ├── field_extractor.py # 示例：字段提取器
│       └── passthrough.py    # 示例：透传插件
├── tests/
│   ├── test_large_scale.py   # 大规模数据测试
│   ├── test_unit.py          # 单元测试
│   └── run_tests.sh          # 测试脚本
└── README.md
```

## 安装依赖

```bash
pip install pyarrow  # 仅在处理 Parquet 文件时需要
```

## 使用方法

### 1. 创建自定义插件

```python
from data_process.plugin import Plugin
from typing import Dict, Any, Optional, List, Tuple

class Myplugin(Plugin):
    def __init__(self):
        super().__init__()
        self.stats['processed_count'] = 0
    
    def single_line_process(
        self, line: Dict[str, Any], output_paths: List[str]
    ) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
        self.stats['processed_count'] += 1
        result = line.copy()
        return result, 0  # 返回处理后数据和输出路径序号
```

### 2. 运行数据处理

```bash
python -m src.data_process.processor \
    --input input.jsonl \
    --output-paths output1.jsonl output2.jsonl \
    --stats stats.json \
    --plugin myplugin \
    --plugin-dir ./src/plugins \
    --type jsonl \
    --workers 4
```

### 参数说明

| 参数 | 必需 | 说明 |
|------|------|------|
| `--input` | 是 | 输入文件路径 |
| `--output-paths` | 是 | 输出文件路径列表 |
| `--stats` | 是 | 统计信息输出路径 |
| `--plugin` | 是 | 插件名称 |
| `--plugin-dir` | 否 | 插件目录路径 |
| `--type` | 是 | 文件类型：jsonl/parquet |
| `--workers` | 否 | 并行进程数（默认4） |
| `--batch-size` | 否 | 批处理大小（默认1000） |
| `--keep-order` | 否 | 保持输出顺序与输入一致 |

## 运行测试

```bash
python tests/test_large_scale.py --rows 100000 --plugin passthrough
bash tests/run_tests.sh
```

## 许可

MIT License

