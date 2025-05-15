from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.tokens.merger import DocumentTokenizerMerger
from datatrove.pipeline.tokens.tokenizer import DocumentTokenizer


DATASET_NAME = "tokenize_test"


dist_executor = LocalPipelineExecutor(
    pipeline=[
        JsonlReader(
            data_folder="/data/xql_llama/CCI3-HQ",  # 替换为您的JSONL文件目录
            text_key="text",  # 假设JSONL文件中文本字段名为"content"     通过inspect_jsonl.py查看字段名
            id_key="id",     # 假设JSONL文件中ID字段名为"id"
            recursive=False,
            glob_pattern="*.jsonl",  # 只处理jsonl文件
            limit=1 * (163984),
            # skip=300
        ),
        DocumentTokenizer(
            output_folder=f"/data/datatrove/{DATASET_NAME}/tokenized/",   # 改为绝对路径
            local_working_dir=f"/data/datatrove/{DATASET_NAME}/scratch/guilherme/tokenized/",  # 改为绝对路径
            save_filename=f"{DATASET_NAME}_tokenized",
        )
    ],
    tasks=10,  # 设置为 workers 的 2-3 倍
    workers=10,  # 设置为 CPU 核心数的 83% (144 * 0.83 ≈ 120)
    # start_method='spawn',  # 进程启动方法
    logging_dir=f"/data/datatrove/logs/tokenize_{DATASET_NAME}", 
    skip_completed=True
)

merge_executor = LocalPipelineExecutor(
    pipeline=[
        DocumentTokenizerMerger(
            input_folder=f"/data/datatrove/{DATASET_NAME}/tokenized/",    # 与DocumentTokenizer的output_folder一致
            output_folder=f"/data/datatrove/{DATASET_NAME}/standard/",     # 改为绝对路径
            save_filename=f"{DATASET_NAME}",
        )
    ],
    tasks=1,
    workers=1, 
    # start_method='spawn',  # 进程启动方法
    logging_dir=f"/data/datatrove/logs/tokenize_{DATASET_NAME}_merged",
    depends=dist_executor,
    skip_completed=True
)

if __name__ == "__main__":
    dist_executor.run()
    merge_executor.run()

