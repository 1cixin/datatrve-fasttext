from datatrove.pipeline.filters.fasttext_filter import FastTextClassifierFilter
from datatrove.pipeline.filters.lambda_filter import LambdaFilter
from datatrove.pipeline.readers import ParquetReader
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.writers import ParquetWriter
from datatrove.pipeline.filters.car_filter import CarLawFilter, CarManufacturingFilter, CarMoldFilter


# 在ParquetReader配置中明确指定text_key
pipeline = [
    ParquetReader(
        # data_folder="D:/Projects/datatrove_e/datatrove/input_data/parquet",
        data_folder=r"D:\Projects\datatrove_e\datatrove\output_data\recall\top_1000000_samples_filtered_car",
        glob_pattern="*.parquet",
        text_key="text",
        # id_key="_id",
        read_metadata=True  # 必须启用元数据读取
    ),
    
    # CarMoldFilter(),

    FastTextClassifierFilter(
        keep_labels=[("mold", 0.7)],
        model_url="D:/Projects/fasttext/model_fasttext_0507.bin",
        filter_mode="DOCUMENT",
        save_labels_in_metadata=True,  # 确保保存标签到元数据
    ),
    
    ParquetWriter(  # 最终输出
        output_folder="./output_data/ParquetWriter",
        output_filename="${rank}.parquet",  # 使用rank作为文件名
        compression=None,
    )
]

executor = LocalPipelineExecutor(
    pipeline=pipeline,
    tasks=3,
    workers=3,
    logging_dir="./logs/parquet",
    skip_completed=False,
    start_method="spawn"
)

if __name__ == '__main__':
    executor.run()