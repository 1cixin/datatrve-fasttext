import os
import glob
import pandas as pd

def merge_all_parquet_files(input_dir, output_filename, output_dir=None):
    """
    合并目录中的所有Parquet文件
    
    参数:
        input_dir: 输入目录路径
        output_filename: 自定义输出文件名（需包含.parquet扩展名）
        output_dir: 输出目录路径（默认与输入目录相同）
    """
    # 设置输出目录
    if output_dir is None:
        output_dir = input_dir
    
    # 确保输出文件名以.parquet结尾
    if not output_filename.lower().endswith('.parquet'):
        output_filename += '.parquet'
    
    # 构建完整输出路径
    output_path = os.path.join(output_dir, output_filename)
    
    # 查找目录中的所有Parquet文件
    file_list = glob.glob(os.path.join(input_dir, '*.parquet'))
    
    if not file_list:
        print(f"错误：目录中没有找到Parquet文件 - {input_dir}")
        return
    
    print(f"找到 {len(file_list)} 个Parquet文件准备合并:")
    for f in file_list:
        print(f"  - {os.path.basename(f)}")
    
    # 读取并合并所有文件
    dfs = []
    for file_path in file_list:
        try:
            df = pd.read_parquet(file_path)
            dfs.append(df)
            print(f"已加载: {os.path.basename(file_path)} (行数: {len(df)})")
        except Exception as e:
            print(f"加载文件失败: {os.path.basename(file_path)} - {str(e)}")
    
    if not dfs:
        print("错误：没有成功加载任何Parquet文件")
        return
    
    # 合并DataFrame
    try:
        merged_df = pd.concat(dfs, ignore_index=True)
        
        # 写入合并后的文件
        merged_df.to_parquet(output_path, index=False)
        
        print("\n合并完成:")
        print(f"合并文件数: {len(dfs)}")
        print(f"总行数: {len(merged_df)}")
        print(f"输出文件: {output_path}")
        
    except Exception as e:
        print(f"合并过程中出错: {str(e)}")
        if os.path.exists(output_path):
            os.remove(output_path)
            print("已删除不完整的输出文件")

if __name__ == "__main__":
    # 使用示例
    # input_directory = r"D:\Projects\datatrove_e\datatrove\output_data\ParquetWriter\09_11"  # 替换为您的输入目录
    input_directory = r"D:\Projects\data\fasttext_train\mold_more_than_4"  # 替换为您的输入目录
    custom_filename = "merged_mold_more_than_4.parquet"  # 自定义输出文件名
    
    # 可选指定输出目录（默认与输入目录相同）
    # output_directory = r"D:\your\output\directory"  
    
    print("开始合并Parquet文件...")
    merge_all_parquet_files(
        input_dir=input_directory,
        output_filename=custom_filename
        # output_dir=output_directory  # 可选
    )