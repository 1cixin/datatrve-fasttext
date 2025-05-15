from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.readers import JsonlReader, ParquetReader
from datatrove.pipeline.tokens.merger import DocumentTokenizerMerger
from datatrove.pipeline.tokens.tokenizer import DocumentTokenizer


DATASET_NAME = "tokenize_test"


dist_executor = LocalPipelineExecutor(
    pipeline=[
        # JsonlReader(
        #     # data_folder="./input_data",  # 替换为您的JSONL文件目录
        #     data_folder="D:/Projects/datatrove_e/分析/jsonl/5_laws",  # 替换为您的JSONL文件目录
        #     text_key="text",  # 假设JSONL文件中文本字段名为"content"     通过inspect_jsonl.py查看字段名
        #     id_key="id",     # 假设JSONL文件中ID字段名为"id"
        #     recursive=False,
        #     glob_pattern="*.jsonl"  # 只处理jsonl文件
        # ),
        ParquetReader(
            # data_folder="./input_data",  # 替换为您的JSONL文件目录
            data_folder=r"D:\Projects\datatrove_e\datatrove\output_data\tokenize_test",  # 替换为您的JSONL文件目录
            text_key="text",  # 假设JSONL文件中文本字段名为"content"     通过inspect_jsonl.py查看字段名
            id_key="id",     # 假设JSONL文件中ID字段名为"id"
            recursive=False,
            glob_pattern="*.parquet"  # 只处理jsonl文件
        ),
        DocumentTokenizer(  # 文档分词器
            output_folder=f"./{DATASET_NAME}/tokenized/",   # 输出文件夹
            local_working_dir=f"./{DATASET_NAME}/scratch/guilherme/tokenized/",  # 本地工作目录
            save_filename=f"{DATASET_NAME}_tokenized",  # 保存的文件名
        ), 
    ],
    tasks=5,
    workers=5,
    start_method='spawn',  # 进程启动方法
    logging_dir=f"./logs/tokenize/tokenize_{DATASET_NAME}",  # 日志目录
    skip_completed=True
)

merge_executor = LocalPipelineExecutor(
    pipeline=[
        DocumentTokenizerMerger(
            input_folder=f"./{DATASET_NAME}/tokenized/",    # 输入文件夹
            output_folder=f"./{DATASET_NAME}/standard/",   # 输出文件夹
            save_filename=f"{DATASET_NAME}",                # 保存的文件名
        ),
    ],
    tasks=1,
    workers=1, 
    start_method='spawn',  # 进程启动方法
    logging_dir=f"./logs/tokenize/tokenize_{DATASET_NAME}_merged",
    depends=dist_executor,
    skip_completed=False
)

if __name__ == "__main__":
    dist_executor.run()
    merge_executor.run()

