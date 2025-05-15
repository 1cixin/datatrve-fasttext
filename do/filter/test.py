#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import os

# 添加项目根目录到 Python 路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, project_root)

# 打印调试信息
# print("Project root:", project_root)
# print("Current working directory:", os.getcwd())
# print("Python path:", sys.path)

# 直接导入 car_filter.py
from src.datatrove.pipeline.filters.car_filter import CarLawFilter, CarManufacturingFilter, CarMoldFilter
from datatrove.executor import LocalPipelineExecutor
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.writers import JsonlWriter

if __name__ == '__main__':
    print("开始处理汽车行业相关数据...")
    
    executor = LocalPipelineExecutor(
        pipeline=[
            JsonlReader(
                data_folder="./input_data",  # 替换为您的JSONL文件目录
                text_key="text",  # 假设JSONL文件中文本字段名为"content"     通过inspect_jsonl.py查看字段名
                id_key="id",     # 假设JSONL文件中ID字段名为"id"
                recursive=False,    # 是否递归读取子目录
                glob_pattern="*.jsonl",  # 只处理jsonl文件
                # limit=1
            ),
            # CarLawFilter(),  # 使用自定义过滤器
            # CarManufacturingFilter(),
            CarMoldFilter(),
            JsonlWriter("./output_data/test/")
        ],
        tasks=1,  # 任务数量(根据CPU核心数调整)
        workers=1,  # 并行工作进程数
        logging_dir="./logs",  # 日志目录
        skip_completed=False  # 禁用自动跳过
    )
    
    executor.run()
    print("数据处理完成!")