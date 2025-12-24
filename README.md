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

## 安装

### 开发模式安装（推荐）

```bash
# 克隆项目
cd AI_data_process

# 以可编辑模式安装
pip install -e .
```

安装后，`aidata` 包会被添加到 Python 环境中，可以在任何地方导入使用。

### 依赖说明

- `pyarrow>=22.0.0`：处理 Parquet 文件必需（已包含在安装中）

## 使用方法

### 1. 创建自定义插件

在 `src/aidata/plugins/` 目录下创建你的插件文件，例如 `myplugin.py`：

```python
from aidata.plugins.plugin import Plugin
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

有两种方式运行处理器：

#### 方式一：作为模块运行（推荐）

```bash
python -m aidata.data_process.processor \
    --input input.jsonl \
    --output-paths output1.jsonl output2.jsonl \
    --stats stats.json \
    --plugin myplugin \
    --type jsonl \
    --workers 4
```

#### 方式二：直接运行（需要 `pip install -e .`）

```bash
python src/aidata/data_process/processor.py \
    --input input.jsonl \
    --output-paths output1.jsonl output2.jsonl \
    --stats stats.json \
    --plugin passthrough \
    --type jsonl \
    --workers 4
```

**注意**：
- 方式一 (`python -m`) 会自动将当前目录添加到 Python 路径，适合开发调试
- 方式二需要先执行 `pip install -e .` 安装包，之后可以在任何位置运行
- `--plugin-dir` 参数是可选的，默认会从 `aidata.plugins` 包中加载插件

### 参数说明

| 参数             | 必需 | 说明                    |
| ---------------- | ---- | ----------------------- |
| `--input`        | 是   | 输入文件路径            |
| `--output-paths` | 是   | 输出文件路径列表        |
| `--stats`        | 是   | 统计信息输出路径        |
| `--plugin`       | 是   | 插件名称                |
| `--plugin-dir`   | 否   | 插件目录路径            |
| `--type`         | 是   | 文件类型：jsonl/parquet |
| `--workers`      | 否   | 并行进程数（默认4）     |
| `--batch-size`   | 否   | 批处理大小（默认1000）  |
| `--keep-order`   | 否   | 保持输出顺序与输入一致  |

## 运行测试

```bash
python tests/test_large_scale.py --rows 100000 --plugin passthrough
bash tests/run_tests.sh
```

## 许可

MIT License

