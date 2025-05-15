#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from src.datatrove.pipeline.filters.car_filter import CarLawFilter, CarManufacturingFilter, CarMoldFilter
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.filters import FastTextClassifierFilter, LambdaFilter
from datatrove.pipeline.writers import JsonlWriter
from datatrove.executor import LocalPipelineExecutor
# importlib.reload(fasttext_filter)

# 创建处理管道
pipeline_1 = [
    JsonlReader(
        # data_folder="./input_data/mold-filter",  # 替换为您的JSONL文件目录
        data_folder=r"D:\Projects\data\fasttext_train\mold_more_than_4",  # 替换为您的JSONL文件目录
        # data_folder="./output_data/recall/",  # 替换为您的JSONL文件目录
        text_key="text",  # 假设JSONL文件中文本字段名为"content"     通过inspect_jsonl.py查看字段名
        # id_key="id",     # 假设JSONL文件中ID字段名为"id"
        recursive=False,    # 是否递归读取子目录
        glob_pattern="*.jsonl"  # 只处理jsonl文件
    ),
    
    # CarMoldFilter(),

    FastTextClassifierFilter(
        keep_labels=[("mold", 0.5)],
        model_url="D:/Projects/fasttext/model_fasttext_0514.bin",
        filter_mode="DOCUMENT",
        save_labels_in_metadata=True  # 确保保存标签到元数据
    ),
        # exclusion_writer=JsonlWriter(    # 保存被过滤掉的数据（不符合条件的数据）
        #     output_folder="./output_data/excluded_fasttext_filter",
        #     output_filename="${rank}.jsonl.gz",    # 默认压缩成jsonl.gz文件
        #     # compression=None    # 不压缩
        # )

    JsonlWriter(
        # output_folder="./output_data/recall",
        output_folder=r"D:\Projects\datatrove_e\datatrove\output_data\recall",
        output_filename="${rank}_filter.jsonl",    # 默认压缩成jsonl.gz文件
        compression=None,    # 不压缩
    )
]


# 3. 创建并运行本地执行器
executor_1 = LocalPipelineExecutor(
    pipeline=pipeline_1,
    tasks=2,  # 任务数量(根据CPU核心数调整)
    workers=2,  # 并行工作进程数
    logging_dir="./logs",  # 日志目录
    skip_completed=False,  # 禁用自动跳过
    start_method="spawn",  # 使用spawn模式
)


if __name__ == "__main__":
    print("开始处理汽车行业相关数据...")
    executor_1.run()
    print("数据处理完成!")
