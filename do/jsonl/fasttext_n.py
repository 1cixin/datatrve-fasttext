#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys


from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.filters import FastTextClassifierFilter, LambdaFilter
from datatrove.pipeline.writers import JsonlWriter
from datatrove.executor import LocalPipelineExecutor


# 要处理的文件列表
TARGET_FILES = [
    "part_000047.jsonl",
    "part_000049.jsonl"
]

# 创建处理管道
pipeline = []

# 为每个文件创建一个JsonlReader
for file in TARGET_FILES:
    pipeline.extend([
        # 读取指定的文件
        JsonlReader(
            data_folder="./input_data",  # 文件所在目录
            text_key="text",  # 假设JSONL文件中文本字段名为"text"
            id_key="id",     # 假设JSONL文件中ID字段名为"id"
            recursive=False,    # 是否递归读取子目录
            glob_pattern=file  # 处理指定的文件
        ),
        
        # 使用FastText进行语言和主题分类
        FastTextClassifierFilter(
            keep_labels=[("laws", 0.5)],  # 使用主题词标签
            # model_url="D:/Projects/fasttext/lid.176.bin",  # 替换为您的FastText模型路径
            model_url="D:/Projects/fasttext/model_fasttext_0425.bin",  # 替换为您的FastText模型路径
            filter_mode="DOCUMENT",  # 按句子过滤 , 默认按文档(DOCUMENT)过滤，还可选句子(SENTENCE)、段落(PARAGRAPH)、单词(WORDS)
            exclusion_writer=JsonlWriter(    # 保存被过滤掉的数据（不符合条件的数据）
                output_folder="./output_data/excluded_fasttext_filter",
                output_filename="${rank}.jsonl",    # 默认压缩成jsonl.gz文件
                compression=None    # 不压缩
            )
        ),
        
        # 最终输出
        JsonlWriter(
            output_folder="./output_data/filtered_car_data",
            output_filename=f"{os.path.splitext(file)[0]}_{{rank}}.jsonl",    # 使用文件名作为输出文件名的一部分
            compression=None    # 不压缩
        )
    ])

# 3. 创建并运行本地执行器
executor = LocalPipelineExecutor(
    pipeline=pipeline,
    tasks=4,  # 任务数量(根据CPU核心数调整)
    workers=4,  # 并行工作进程数
    logging_dir="./logs",  # 日志目录
    skip_completed=False,  # 禁用自动跳过
    start_method="spawn"
)

if __name__ == "__main__":
    print("开始处理汽车行业相关数据...")
    # 检查文件是否存在
    for file in TARGET_FILES:
        file_path = os.path.join("./input_data", file)
        if not os.path.exists(file_path):
            print(f"警告: 文件 {file_path} 不存在!")
        else:
            print(f"找到文件: {file_path}")
    executor.run()
    print("数据处理完成!")
