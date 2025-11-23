"""
数据处理框架主程序
支持 jsonl 和 parquet 文件格式的并行处理
"""

import argparse
import json
import logging
import os
import sys
import time
import importlib.util
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from multiprocessing import Pool, Manager
from collections import defaultdict

try:
    import pyarrow.parquet as pq
    import pyarrow as pa
except ImportError:
    pq = None
    pa = None


# 全局函数用于多进程处理
def process_batch_worker(args):
    """工作进程处理函数"""
    batch, plugin_name = args

    # 动态加载插件
    spec = importlib.util.spec_from_file_location(plugin_name, f"{plugin_name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class_name = plugin_name.capitalize()
    if hasattr(module, class_name):
        plugin_class = getattr(module, class_name)
    elif hasattr(module, plugin_name):
        plugin_class = getattr(module, plugin_name)
    else:
        raise AttributeError(f"找不到插件类")

    plugin = plugin_class()
    results = []

    for idx, line_data in batch:
        try:
            result = plugin.single_line_process(line_data)
            if result is not None:
                results.append((idx, result))
        except Exception as e:
            print(f"处理第 {idx} 行时出错: {e}")

    return results, plugin.get_stats()


class DataProcessor:
    """数据处理框架核心类"""

    def __init__(
        self,
        input_path: str,
        output_path: str,
        stats_path: str,
        plugin_name: str,
        file_type: str,
        sort_key: str,
        num_workers: int = 4,
    ):
        self.input_path = input_path
        self.output_path = output_path
        self.stats_path = stats_path
        self.plugin_name = plugin_name
        self.file_type = file_type.lower()
        self.sort_key = sort_key
        self.num_workers = num_workers

        # 检查文件类型
        if self.file_type not in ["jsonl", "parquet"]:
            raise ValueError(f"不支持的文件类型: {file_type}，仅支持 jsonl 或 parquet")

        if self.file_type == "parquet" and pq is None:
            raise ImportError("处理 parquet 文件需要安装 pyarrow: pip install pyarrow")

        # 创建输出目录
        os.makedirs(
            os.path.dirname(output_path) if os.path.dirname(output_path) else ".",
            exist_ok=True,
        )
        os.makedirs(
            os.path.dirname(stats_path) if os.path.dirname(stats_path) else ".",
            exist_ok=True,
        )

        # 设置日志
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"log/{plugin_name}_{timestamp}.log"
        self._setup_logger(log_filename)

        # 加载插件
        self.plugin_class = self._load_plugin(plugin_name)

        # 检查点文件
        self.checkpoint_file = f"{output_path}.checkpoint"

    def _setup_logger(self, log_filename: str):
        """设置日志记录器"""
        self.logger = logging.getLogger("DataProcessor")
        self.logger.setLevel(logging.INFO)

        # 文件处理器
        fh = logging.FileHandler(log_filename, encoding="utf-8")
        fh.setLevel(logging.INFO)

        # 控制台处理器
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        # 格式化器
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

        self.logger.info(f"日志文件: {log_filename}")

    def _load_plugin(self, plugin_name: str):
        """动态加载插件类"""
        plugin_file = f"{plugin_name}.py"

        if not os.path.exists(plugin_file):
            raise FileNotFoundError(f"找不到插件文件: {plugin_file}")

        # 动态导入模块
        spec = importlib.util.spec_from_file_location(plugin_name, plugin_file)
        module = importlib.util.module_from_spec(spec)
        sys.modules[plugin_name] = module
        spec.loader.exec_module(module)

        # 获取插件类（类名应与插件名相同，首字母大写）
        class_name = plugin_name.capitalize()
        if hasattr(module, class_name):
            plugin_class = getattr(module, class_name)
        elif hasattr(module, plugin_name):
            plugin_class = getattr(module, plugin_name)
        else:
            raise AttributeError(
                f"在插件文件 {plugin_file} 中找不到类 {class_name} 或 {plugin_name}"
            )

        self.logger.info(f"成功加载插件: {plugin_name}")
        return plugin_class

    def _read_checkpoint(self) -> int:
        """读取检查点，返回已处理的行号"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, "r") as f:
                    checkpoint = int(f.read().strip())
                self.logger.info(f"从检查点恢复，已处理 {checkpoint} 行")
                return checkpoint
            except Exception as e:
                self.logger.warning(f"读取检查点文件失败: {e}")
        return 0

    def _write_checkpoint(self, line_number: int):
        """写入检查点"""
        try:
            with open(self.checkpoint_file, "w") as f:
                f.write(str(line_number))
        except Exception as e:
            self.logger.error(f"写入检查点失败: {e}")

    def _read_data(self) -> List[Tuple[int, Dict[str, Any]]]:
        """读取并排序数据"""
        self.logger.info(f"开始读取 {self.file_type} 文件: {self.input_path}")
        data = []

        if self.file_type == "jsonl":
            with open(self.input_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            data.append(json.loads(line))
                        except json.JSONDecodeError as e:
                            self.logger.warning(f"JSON 解析错误: {e}")

        elif self.file_type == "parquet":
            table = pq.read_table(self.input_path)
            data = table.to_pylist()

        self.logger.info(f"共读取 {len(data)} 行数据")

        # 按指定字段排序
        self.logger.info(f"按字段 '{self.sort_key}' 排序数据")
        try:
            data.sort(key=lambda x: x.get(self.sort_key, ""))
        except Exception as e:
            self.logger.error(f"排序失败: {e}")
            raise

        # 添加序号
        indexed_data = [(i, item) for i, item in enumerate(data)]
        return indexed_data

    def _write_output(self, data: List[Dict[str, Any]]):
        """写入输出文件"""
        self.logger.info(f"写入输出文件: {self.output_path}")

        if self.file_type == "jsonl":
            with open(self.output_path, "w", encoding="utf-8") as f:
                for item in data:
                    f.write(json.dumps(item, ensure_ascii=False) + "\n")

        elif self.file_type == "parquet":
            table = pa.Table.from_pylist(data)
            pq.write_table(table, self.output_path)

        self.logger.info(f"成功写入 {len(data)} 行数据")

    def _write_stats(self, stats: Dict[str, Any]):
        """写入统计信息"""
        self.logger.info(f"写入统计文件: {self.stats_path}")

        with open(self.stats_path, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)

    def process(self):
        """执行数据处理"""
        self.logger.info("=" * 60)
        self.logger.info("开始数据处理")
        self.logger.info(f"输入文件: {self.input_path}")
        self.logger.info(f"输出文件: {self.output_path}")
        self.logger.info(f"统计文件: {self.stats_path}")
        self.logger.info(f"插件名称: {self.plugin_name}")
        self.logger.info(f"文件类型: {self.file_type}")
        self.logger.info(f"排序字段: {self.sort_key}")
        self.logger.info(f"并行进程数: {self.num_workers}")
        self.logger.info("=" * 60)

        # 读取检查点
        checkpoint = self._read_checkpoint()

        # 读取并排序数据
        indexed_data = self._read_data()

        # 过滤已处理的数据
        if checkpoint > 0:
            indexed_data = [(i, d) for i, d in indexed_data if i >= checkpoint]
            self.logger.info(
                f"跳过前 {checkpoint} 行，剩余 {len(indexed_data)} 行待处理"
            )

        if not indexed_data:
            self.logger.info("没有数据需要处理")
            return

        # 创建批次
        batch_size = max(1, len(indexed_data) // (self.num_workers * 4))
        batches = []
        for i in range(0, len(indexed_data), batch_size):
            batches.append((indexed_data[i : i + batch_size], self.plugin_name))

        self.logger.info(f"数据分为 {len(batches)} 个批次，每批约 {batch_size} 行")

        # 并行处理
        start_time = time.time()
        all_results = []
        combined_stats = defaultdict(int)
        last_log_time = start_time
        last_processed = 0

        try:
            with Pool(processes=self.num_workers) as pool:
                for batch_results, batch_stats in pool.imap_unordered(
                    process_batch_worker, batches
                ):
                    # 更新结果
                    all_results.extend(batch_results)

                    # 更新统计
                    for key, value in batch_stats.items():
                        if isinstance(value, (int, float)):
                            combined_stats[key] += value
                        else:
                            combined_stats[key] = value

                    # 每10秒输出一次日志
                    current_time = time.time()
                    if current_time - last_log_time >= 10:
                        elapsed_time = current_time - start_time
                        time_since_last = current_time - last_log_time
                        current_processed = len(all_results)
                        processed_since_last = current_processed - last_processed

                        rows_per_second = (
                            processed_since_last / time_since_last
                            if time_since_last > 0
                            else 0
                        )

                        self.logger.info(
                            f"处理速度: {rows_per_second:.2f} 行/秒 | "
                            f"已处理: {current_processed} 行 | "
                            f"已输出: {len(all_results)} 行 | "
                            f"运行时间: {elapsed_time:.2f} 秒"
                        )

                        last_log_time = current_time
                        last_processed = current_processed

        except KeyboardInterrupt:
            self.logger.warning("处理被用户中断")
            raise
        except Exception as e:
            self.logger.error(f"处理过程中出错: {e}")
            raise

        # 最后一次输出统计
        elapsed_time = time.time() - start_time
        self.logger.info(
            f"最终统计 | "
            f"已处理: {len(all_results)} 行 | "
            f"已输出: {len(all_results)} 行 | "
            f"运行时间: {elapsed_time:.2f} 秒"
        )

        # 按索引排序结果
        all_results.sort(key=lambda x: x[0])
        output_data = [result for _, result in all_results]

        # 写入输出
        self._write_output(output_data)

        # 添加框架统计信息
        combined_stats["total_processed"] = len(indexed_data)
        combined_stats["total_output"] = len(output_data)
        combined_stats["processing_time_seconds"] = elapsed_time
        combined_stats["average_speed_rows_per_second"] = (
            combined_stats["total_processed"]
            / combined_stats["processing_time_seconds"]
        )

        # 写入统计
        self._write_stats(dict(combined_stats))

        # 更新检查点
        if indexed_data:
            last_index = indexed_data[-1][0]
            self._write_checkpoint(last_index + 1)

        self.logger.info("=" * 60)
        self.logger.info("数据处理完成")
        self.logger.info(f"总处理行数: {combined_stats['total_processed']}")
        self.logger.info(f"总输出行数: {combined_stats['total_output']}")
        self.logger.info(f"总耗时: {combined_stats['processing_time_seconds']:.2f} 秒")
        self.logger.info(
            f"平均速度: {combined_stats['average_speed_rows_per_second']:.2f} 行/秒"
        )
        self.logger.info("=" * 60)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="数据处理框架")
    parser.add_argument("--input", required=True, help="输入文件路径")
    parser.add_argument("--output", required=True, help="输出文件路径")
    parser.add_argument("--stats", required=True, help="统计文件路径")
    parser.add_argument("--plugin", required=True, help="数据处理插件名")
    parser.add_argument(
        "--type", required=True, choices=["jsonl", "parquet"], help="文件类型"
    )
    parser.add_argument("--sort-key", required=True, help="排序字段名")
    parser.add_argument("--workers", type=int, default=4, help="并行进程数（默认: 4）")

    args = parser.parse_args()

    try:
        processor = DataProcessor(
            input_path=args.input,
            output_path=args.output,
            stats_path=args.stats,
            plugin_name=args.plugin,
            file_type=args.type,
            sort_key=args.sort_key,
            num_workers=args.workers,
        )
        processor.process()
    except Exception as e:
        logging.error(f"程序执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
