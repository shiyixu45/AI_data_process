"""
大规模数据测试脚本
生成测试数据并验证处理框架的正确性和性能
"""

import json
import os
import sys
import time
import random
import string
import argparse
import tempfile
import hashlib
from pathlib import Path


def generate_test_data(output_path: str, num_rows: int, seed: int = 42):
    """生成测试数据"""
    random.seed(seed)

    print(f"Generating {num_rows:,} rows of test data...")
    start_time = time.time()

    with open(output_path, "w", encoding="utf-8", buffering=8 * 1024 * 1024) as f:
        for i in range(num_rows):
            row = {
                "id": i,
                "name": "".join(random.choices(string.ascii_letters, k=10)),
                "score": random.randint(0, 100),
                "value": random.uniform(0, 1000),
                "text": "".join(
                    random.choices(
                        string.ascii_letters + " ", k=random.randint(50, 500)
                    )
                ),
                "timestamp": int(time.time() * 1000) + i,
                "category": random.choice(["A", "B", "C", "D"]),
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

            if (i + 1) % 100000 == 0:
                print(f"  Generated {i + 1:,} rows...")

    elapsed = time.time() - start_time
    file_size = os.path.getsize(output_path) / (1024 * 1024)
    print(f"Generated {num_rows:,} rows in {elapsed:.2f}s ({file_size:.2f} MB)")
    print(f"Speed: {num_rows / elapsed:,.0f} rows/s")
    return output_path


def verify_output(input_path: str, output_paths: list, plugin_name: str) -> bool:
    """验证输出文件的正确性"""
    print("\nVerifying output...")

    # 读取输入
    input_ids = set()
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            data = json.loads(line.strip())
            input_ids.add(data["id"])

    # 读取所有输出
    output_ids = set()
    output_count = 0
    for path in output_paths:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    data = json.loads(line.strip())
                    if "id" in data:
                        output_ids.add(data["id"])
                    output_count += 1

    print(f"  Input rows: {len(input_ids):,}")
    print(f"  Output rows: {output_count:,}")

    # 对于passthrough插件，所有输入应该都在输出中
    if plugin_name == "passthrough":
        if input_ids == output_ids:
            print("  ✓ All input IDs found in output")
            return True
        else:
            missing = input_ids - output_ids
            extra = output_ids - input_ids
            print(f"  ✗ Missing IDs: {len(missing)}, Extra IDs: {len(extra)}")
            return False

    return True


def run_performance_test(
    input_path: str,
    output_dir: str,
    plugin_name: str,
    workers: int,
    batch_size: int,
    keep_order: bool,
):
    """运行性能测试"""
    from aidata.data_process.processor import DataProcessor

    output_paths = [
        os.path.join(output_dir, "output_1.jsonl"),
        os.path.join(output_dir, "output_2.jsonl"),
    ]
    stats_path = os.path.join(output_dir, "stats.json")
    plugin_dir = str(Path(__file__).parent.parent / "src" / "aidata" / "plugins")

    print(f"\nRunning performance test:")
    print(f"  Plugin: {plugin_name}")
    print(f"  Workers: {workers}")
    print(f"  Batch size: {batch_size}")
    print(f"  Keep order: {keep_order}")

    start_time = time.time()

    processor = DataProcessor(
        input_path=input_path,
        output_paths=output_paths,
        stats_path=stats_path,
        plugin_name=plugin_name,
        file_type="jsonl",
        num_workers=workers,
        keep_order=keep_order,
        plugin_dir=plugin_dir,
        batch_size=batch_size,
    )
    processor.process()

    elapsed = time.time() - start_time

    # 读取统计信息
    with open(stats_path, "r") as f:
        stats = json.load(f)

    print(f"\nResults:")
    print(f"  Total time: {elapsed:.2f}s")
    print(f"  Processed: {stats.get('total_processed', 'N/A'):,}")
    print(f"  Output: {stats.get('total_output', 'N/A'):,}")
    if elapsed > 0:
        print(f"  Speed: {stats.get('total_processed', 0) / elapsed:,.0f} rows/s")

    return output_paths, stats


def main():
    parser = argparse.ArgumentParser(description="Large-scale data processing test")
    parser.add_argument("--rows", type=int, default=100000, help="Number of test rows")
    parser.add_argument("--workers", type=int, default=4, help="Number of workers")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size")
    parser.add_argument("--plugin", default="passthrough", help="Plugin to test")
    parser.add_argument("--keep-order", action="store_true", help="Maintain order")
    parser.add_argument(
        "--skip-generate", action="store_true", help="Skip data generation"
    )
    parser.add_argument("--input", default=None, help="Use existing input file")

    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmpdir:
        if args.input and os.path.exists(args.input):
            input_path = args.input
        else:
            input_path = os.path.join(tmpdir, "test_input.jsonl")
            if not args.skip_generate:
                generate_test_data(input_path, args.rows)

        output_paths, stats = run_performance_test(
            input_path=input_path,
            output_dir=tmpdir,
            plugin_name=args.plugin,
            workers=args.workers,
            batch_size=args.batch_size,
            keep_order=args.keep_order,
        )

        verify_output(input_path, output_paths, args.plugin)

        print("\n" + "=" * 60)
        print("Test completed!")


if __name__ == "__main__":
    main()
