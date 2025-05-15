import os

from datatrove.executor.base import PipelineExecutor
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.dedup import ESDatasetToSequence, ESMergeSequences, ESRangeRemover
from datatrove.pipeline.filters import GopherQualityFilter, LanguageFilter
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.utils.typeshelper import Languages


def run_step_1_and_2():
    pipeline_1 = [
        JsonlReader(data_folder="./input_data", limit=1000),  # 读取jsonl文件
        GopherQualityFilter(min_stop_words=0),  # 质量过滤
        LanguageFilter(language_threshold=0.5, languages=[Languages.english]),  # 语言过滤
        JsonlWriter("intermediate/"),  # 写入中间结果
        ESDatasetToSequence(output_folder="es/"),  # 转换为序列格式
    ]

    pipeline_2 = [
        ESMergeSequences(   # 合并所有序列
            data_folder="es",
            tasks_stage_1=4,
        )
    ]

    executor_1: PipelineExecutor = LocalPipelineExecutor(pipeline=pipeline_1, workers=4, tasks=4, start_method='spawn')
    executor_2: PipelineExecutor = LocalPipelineExecutor(pipeline=pipeline_2, workers=1, tasks=1, start_method='spawn')

    print(executor_1.run())
    print(executor_2.run())


def run_step_3():
    pipeline_3 = [
        JsonlReader("intermediate/"),  # 必须与DatasetToSequence阶段使用的数据相同
        ESRangeRemover(
            sequence_folder=f"{os.getcwd()}/es/",
        ),
        JsonlWriter("final-deduped-data"),
    ]

    executor_3: PipelineExecutor = LocalPipelineExecutor(pipeline=pipeline_3, workers=4, tasks=4, start_method='spawn')

    print(executor_3.run())

if __name__ == '__main__':
    run_step_1_and_2()
    run_step_3()