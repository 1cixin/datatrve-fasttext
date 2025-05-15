import os
import pandas as pd
import pyarrow.parquet as pq
import argparse

def filter_data_by_label_score(input_file, output_file, label_name, min_score, max_score):
    """
    从Parquet文件中提取特定标签得分区间内的数据
    
    参数:
        input_file: 输入Parquet文件路径
        output_file: 输出Parquet文件路径
        label_name: 要筛选的标签名称(如'__label__other')
        min_score: 最小得分(包含)
        max_score: 最大得分(包含)
    """
    # 读取Parquet文件
    df = pd.read_parquet(input_file)
    
    # 筛选数据
    filtered_df = df[
        (df['metadata'].apply(lambda x: x[label_name]) >= min_score) &
        (df['metadata'].apply(lambda x: x[label_name]) <= max_score)
    ]
    
    # 保存结果
    filtered_df.to_parquet(output_file, index=False)
    print(f"筛选完成! 共找到 {len(filtered_df)} 条数据，已保存到 {output_file}")


if __name__ == "__main__":
    input_file = "D:/Projects/datatrove_e/datatrove/output_data/ParquetWriter/000_00000.parquet"
    
    # 从输入文件名提取基础名称（不含扩展名）
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    
    # 定义筛选参数
    label_name = "__label__mold"
    min_score = 0.9
    max_score = 1.1
    
    # 动态生成输出文件名
    output_file = os.path.join(
        os.path.dirname(input_file),
        f"{base_name}_{label_name.replace('__label__', '')}_{min_score}_{max_score}.parquet"
    )
    
    # 验证参数
    if min_score > max_score:
        raise ValueError("最小得分不能大于最大得分")
    
    # 执行筛选
    filter_data_by_label_score(input_file, output_file, label_name, min_score, max_score)