"""
数据处理框架主程序
支持 jsonl 和 parquet 文件格式的高性能并行处理
"""

import argparse
import json
import logging
import os
import sys
import time
import importlib.util
import io
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple, Iterator
from multiprocessing import Pool, Queue, Process, Manager
from collections import defaultdict
from queue import Empty
import threading

try:
    import pyarrow.parquet as pq
    import pyarrow as pa
except ImportError:
    pq = None
    pa = None


def load_plugin_class(plugin_name: str, plugin_dir: str = None):
    """动态加载插件类"""
    if plugin_dir:
        plugin_file = os.path.join(plugin_dir, f"{plugin_name}.py")
    else:
        plugin_file = f"{plugin_name}.py"

    if not os.path.exists(plugin_file):
        raise FileNotFoundError(f"Plugin file not found: {plugin_file}")

    spec = importlib.util.spec_from_file_location(plugin_name, plugin_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class_name = plugin_name.capitalize()
    if hasattr(module, class_name):
        return getattr(module, class_name)
    elif hasattr(module, plugin_name):
        return getattr(module, plugin_name)
    else:
        raise AttributeError(f"Cannot find plugin class in {plugin_file}")


def process_batch_worker(args):
    """工作进程处理函数"""
    batch, plugin_name, plugin_dir, output_paths, keep_order = args

    plugin_class = load_plugin_class(plugin_name, plugin_dir)
    plugin = plugin_class()
    results = []

    for idx, line_data in batch:
        try:
            result_data, output_idx = plugin.single_line_process(
                line_data, output_paths
            )
            if result_data is not None and output_idx is not None:
                if keep_order:
                    results.append((idx, result_data, output_idx))
                else:
                    results.append((result_data, output_idx))
        except Exception as e:
            print(f"Error processing line {idx}: {e}")

    return results, plugin.get_stats()


class DataProcessor:
    """数据处理框架核心类"""

    def __init__(
        self,
        input_path: str,
        output_paths: List[str],
        stats_path: str,
        plugin_name: str,
        file_type: str,
        num_workers: int = 4,
        keep_order: bool = False,
        plugin_dir: str = None,
        batch_size: int = 1000,
        write_buffer_size: int = 8 * 1024 * 1024,  # 8MB write buffer
    ):
        self.input_path = input_path
        self.output_paths = output_paths
        self.stats_path = stats_path
        self.plugin_name = plugin_name
        self.file_type = file_type.lower()
        self.num_workers = num_workers
        self.keep_order = keep_order
        self.plugin_dir = plugin_dir
        self.batch_size = batch_size
        self.write_buffer_size = write_buffer_size

        # Validate file type
        if self.file_type not in ["jsonl", "parquet"]:
            raise ValueError(f"Unsupported file type: {file_type}")

        if self.file_type == "parquet" and pq is None:
            raise ImportError("pyarrow required for parquet: pip install pyarrow")

        # Create output directories
        for output_path in output_paths:
            dir_name = os.path.dirname(output_path)
            if dir_name:
                os.makedirs(dir_name, exist_ok=True)

        stats_dir = os.path.dirname(stats_path)
        if stats_dir:
            os.makedirs(stats_dir, exist_ok=True)

        # Setup logging
        log_dir = "log"
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"{log_dir}/{plugin_name}_{timestamp}.log"
        self._setup_logger(log_filename)

        # Verify plugin exists
        load_plugin_class(plugin_name, plugin_dir)

        # Checkpoint file
        self.checkpoint_file = f"{output_paths[0]}.checkpoint"

    def _setup_logger(self, log_filename: str):
        """Setup logger"""
        self.logger = logging.getLogger("DataProcessor")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        fh = logging.FileHandler(log_filename, encoding="utf-8")
        fh.setLevel(logging.INFO)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
        self.logger.info(f"Log file: {log_filename}")

    def _write_checkpoint(self, line_number: int):
        """Write checkpoint"""
        try:
            with open(self.checkpoint_file, "w") as f:
                f.write(str(line_number))
        except Exception as e:
            self.logger.error(f"Failed to write checkpoint: {e}")

    def _iter_jsonl(self, start_line: int = 0) -> Iterator[Tuple[int, Dict[str, Any]]]:
        """Stream read JSONL file with buffering"""
        with open(
            self.input_path, "r", encoding="utf-8", buffering=8 * 1024 * 1024
        ) as f:
            for idx, line in enumerate(f):
                if idx < start_line:
                    continue
                line = line.strip()
                if line:
                    try:
                        yield idx, json.loads(line)
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"JSON decode error at line {idx}: {e}")

    def _iter_parquet(
        self, start_line: int = 0
    ) -> Iterator[Tuple[int, Dict[str, Any]]]:
        """Stream read Parquet file in batches"""
        parquet_file = pq.ParquetFile(self.input_path)
        idx = 0
        for batch in parquet_file.iter_batches(batch_size=self.batch_size):
            for row in batch.to_pylist():
                if idx >= start_line:
                    yield idx, row
                idx += 1

    def _iter_data(self, start_line: int = 0) -> Iterator[Tuple[int, Dict[str, Any]]]:
        """Stream data iterator"""
        if self.file_type == "jsonl":
            yield from self._iter_jsonl(start_line)
        elif self.file_type == "parquet":
            yield from self._iter_parquet(start_line)

    def _create_batches(self, data_iter: Iterator, batch_size: int) -> Iterator[List]:
        """Create batches from iterator"""
        batch = []
        for item in data_iter:
            batch.append(item)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def process(self):
        """Execute data processing"""
        self.logger.info("=" * 60)
        self.logger.info("Starting data processing")
        self.logger.info(f"Input: {self.input_path}")
        self.logger.info(f"Outputs: {self.output_paths}")
        self.logger.info(f"Stats: {self.stats_path}")
        self.logger.info(f"Plugin: {self.plugin_name}")
        self.logger.info(f"Type: {self.file_type}")
        self.logger.info(f"Workers: {self.num_workers}")
        self.logger.info(f"Keep order: {self.keep_order}")
        self.logger.info(f"Batch size: {self.batch_size}")
        self.logger.info("=" * 60)

        checkpoint = self._read_checkpoint()
        start_time = time.time()

        # Open all output files at once with buffering
        output_files = []
        try:
            for path in self.output_paths:
                if self.file_type == "jsonl":
                    f = open(
                        path, "w", encoding="utf-8", buffering=self.write_buffer_size
                    )
                    output_files.append(f)
                else:
                    # For parquet, we collect data and write at end
                    output_files.append([])  # List to collect data

            combined_stats = defaultdict(int)
            total_processed = 0
            total_output = 0
            last_log_time = start_time

            if self.keep_order:
                self._process_ordered(
                    output_files,
                    combined_stats,
                    checkpoint,
                    start_time,
                    lambda: time.time(),
                )
            else:
                self._process_unordered(
                    output_files,
                    combined_stats,
                    checkpoint,
                    start_time,
                    lambda: time.time(),
                )

        finally:
            # Close all output files
            for i, f in enumerate(output_files):
                if self.file_type == "jsonl" and hasattr(f, "close"):
                    f.close()
                elif self.file_type == "parquet" and isinstance(f, list) and f:
                    # Write parquet data
                    table = pa.Table.from_pylist(f)
                    pq.write_table(table, self.output_paths[i])

        elapsed = time.time() - start_time
        self._write_stats(dict(combined_stats), elapsed)
        self.logger.info("=" * 60)
        self.logger.info("Processing complete")
        self.logger.info(f"Total time: {elapsed:.2f}s")
        self.logger.info("=" * 60)

    def _process_unordered(
        self, output_files, combined_stats, checkpoint, start_time, get_time
    ):
        """Process without maintaining order - faster"""
        data_iter = self._iter_data(checkpoint)
        batch_iter = self._create_batches(data_iter, self.batch_size)

        total_processed = 0
        total_output = 0
        last_log_time = start_time
        last_processed = 0

        # Prepare batches with metadata
        def batch_with_args():
            for batch in batch_iter:
                yield (
                    batch,
                    self.plugin_name,
                    self.plugin_dir,
                    self.output_paths,
                    False,
                )

        with Pool(processes=self.num_workers) as pool:
            for results, batch_stats in pool.imap_unordered(
                process_batch_worker, batch_with_args()
            ):
                # Write results immediately
                for result_data, output_idx in results:
                    if self.file_type == "jsonl":
                        output_files[output_idx].write(
                            json.dumps(result_data, ensure_ascii=False) + "\n"
                        )
                    else:
                        output_files[output_idx].append(result_data)
                    total_output += 1

                total_processed += self.batch_size

                # Update stats
                for key, value in batch_stats.items():
                    if isinstance(value, (int, float)):
                        combined_stats[key] += value

                # Log every 10 seconds
                current_time = get_time()
                if current_time - last_log_time >= 10:
                    speed = (total_processed - last_processed) / (
                        current_time - last_log_time
                    )
                    self.logger.info(
                        f"Speed: {speed:.0f} rows/s | Processed: {total_processed} | Output: {total_output}"
                    )
                    last_log_time = current_time
                    last_processed = total_processed

        combined_stats["total_processed"] = total_processed
        combined_stats["total_output"] = total_output

    def _read_checkpoint(self) -> int:
        """Read checkpoint, return processed line count"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, "r") as f:
                    checkpoint = int(f.read().strip())
                self.logger.info(f"Resuming from checkpoint: {checkpoint} lines")
                return checkpoint
            except Exception as e:
                self.logger.warning(f"Failed to read checkpoint: {e}")
        return 0

    def _process_ordered(
        self, output_files, combined_stats, checkpoint, start_time, get_time
    ):
        """Process while maintaining input order"""
        data_iter = self._iter_data(checkpoint)
        batch_iter = self._create_batches(data_iter, self.batch_size)

        total_processed = 0
        total_output = 0
        last_log_time = start_time
        last_processed = 0

        # For ordered processing, we need to buffer and sort results
        pending_results = {}  # idx -> (result_data, output_idx)
        next_write_idx = checkpoint

        def batch_with_args():
            for batch in batch_iter:
                yield (
                    batch,
                    self.plugin_name,
                    self.plugin_dir,
                    self.output_paths,
                    True,
                )

        with Pool(processes=self.num_workers) as pool:
            for results, batch_stats in pool.imap_unordered(
                process_batch_worker, batch_with_args()
            ):
                # Buffer results
                for idx, result_data, output_idx in results:
                    pending_results[idx] = (result_data, output_idx)

                # Write results in order
                while next_write_idx in pending_results:
                    result_data, output_idx = pending_results.pop(next_write_idx)
                    if self.file_type == "jsonl":
                        output_files[output_idx].write(
                            json.dumps(result_data, ensure_ascii=False) + "\n"
                        )
                    else:
                        output_files[output_idx].append(result_data)
                    total_output += 1
                    next_write_idx += 1

                total_processed += self.batch_size

                # Update stats
                for key, value in batch_stats.items():
                    if isinstance(value, (int, float)):
                        combined_stats[key] += value

                # Log every 10 seconds
                current_time = get_time()
                if current_time - last_log_time >= 10:
                    speed = (total_processed - last_processed) / (
                        current_time - last_log_time
                    )
                    self.logger.info(
                        f"Speed: {speed:.0f} rows/s | Processed: {total_processed} | "
                        f"Output: {total_output} | Pending: {len(pending_results)}"
                    )
                    last_log_time = current_time
                    last_processed = total_processed

        # Write any remaining buffered results
        for idx in sorted(pending_results.keys()):
            result_data, output_idx = pending_results[idx]
            if self.file_type == "jsonl":
                output_files[output_idx].write(
                    json.dumps(result_data, ensure_ascii=False) + "\n"
                )
            else:
                output_files[output_idx].append(result_data)
            total_output += 1

        combined_stats["total_processed"] = total_processed
        combined_stats["total_output"] = total_output

    def _write_stats(self, stats: Dict[str, Any], elapsed: float):
        """Write statistics to file"""
        stats["processing_time_seconds"] = elapsed
        if elapsed > 0:
            stats["average_speed_rows_per_second"] = (
                stats.get("total_processed", 0) / elapsed
            )

        self.logger.info(f"Writing stats to: {self.stats_path}")
        with open(self.stats_path, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="High-performance data processing framework"
    )
    parser.add_argument("--input-path", required=True, help="Input file path")
    parser.add_argument(
        "--output-paths", required=True, nargs="+", help="Output file paths"
    )
    parser.add_argument("--stats-path", required=True, help="Statistics output path")
    parser.add_argument("--plugin", required=True, help="Plugin name")
    parser.add_argument("--plugin-dir", default=None, help="Plugin directory path")
    parser.add_argument(
        "--type", required=True, choices=["jsonl", "parquet"], help="File type"
    )
    parser.add_argument(
        "--workers", type=int, default=4, help="Number of worker processes"
    )
    parser.add_argument(
        "--keep-order", action="store_true", help="Maintain input order in output"
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for processing"
    )

    args = parser.parse_args()

    try:
        processor = DataProcessor(
            input_path=args.input_path,
            output_paths=args.output_paths,
            stats_path=args.stats_path,
            plugin_name=args.plugin,
            file_type=args.type,
            num_workers=args.workers,
            keep_order=args.keep_order,
            plugin_dir=args.plugin_dir,
            batch_size=args.batch_size,
        )
        processor.process()
    except Exception as e:
        logging.error(f"Processing failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
