import pyarrow.parquet as pq
import pandas as pd

def inspect_parquet(filepath):
    """
    检查Parquet文件结构并统计总条数
    参数:
        filepath: Parquet文件或目录路径
    返回:
        总数据条数
    """
    table = pq.read_table(filepath)
    df = table.to_pandas()
    
    # 获取首行数据
    first_row = df.iloc[0].to_dict()
    
    print("字段结构:", list(first_row.keys()))
    print("示例数据:", first_row)
    print("总数据条数:", len(df))
    
    return len(df)

def extract_top_n(filepath, n, output_path=None):
    """
    提取前N条数据
    参数:
        filepath: Parquet文件路径
        n: 要提取的条数
        output_path: 可选，保存提取结果的Parquet文件路径
    返回:
        提取的数据DataFrame
    """
    table = pq.read_table(filepath)
    df = table.slice(0, n).to_pandas()
    
    # 可选保存到文件
    if output_path:
        pq.write_table(table.slice(0, n), output_path)
        print(f"已保存前{n}条数据到: {output_path}")
    
    return df

# 使用示例
if __name__ == "__main__":
    filepath = "D:/Projects/datatrove_e/datatrove/input_data/parquet"
    
    # 检查文件结构
    total = inspect_parquet(filepath)
    
    # 提取前5条数据
    top_5 = extract_top_n(filepath, 5)
    print("\n前5条数据样例:")
    print(top_5.head().to_string())
    
    # 提取并保存前10条到新文件
    extract_top_n(filepath, 10, "top_10_samples.parquet")