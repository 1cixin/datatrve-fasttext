import gzip
import json
import os
from pathlib import Path

def convert_jsonl_gz_to_jsonl(input_dir, output_dir):
    """
    将目录中的.jsonl.gz文件批量转换为.jsonl文件
    
    参数:
        input_dir (str): 输入的目录路径（包含.jsonl.gz文件）
        output_dir (str): 输出的目录路径
    """
    # 确保输出目录存在
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    print(f"开始转换: {input_dir} -> {output_dir}")
    
    # 遍历输入目录中的所有.gz文件
    for gz_file in Path(input_dir).glob('*.jsonl.gz'):
        # 构造输出路径（保持相同文件名，去掉.gz）
        output_file = Path(output_dir) / gz_file.stem  # stem会自动去掉.jsonl.gz中的.gz
        
        print(f"正在处理: {gz_file.name} -> {output_file.name}")
        
        with gzip.open(gz_file, 'rt', encoding='utf-8') as fin, \
             open(output_file, 'w', encoding='utf-8') as fout:
            
            for line in fin:
                # 直接写入（假设已经是有效的JSON行）
                fout.write(line)
                
                # 如果需要验证JSON格式，可以取消以下注释：
                # try:
                #     json.loads(line)  # 验证JSON格式
                #     fout.write(line)
                # except json.JSONDecodeError:
                #     print(f"跳过无效JSON行: {line[:50]}...")
                #     continue

    print(f"转换完成！输出目录: {output_dir}")

if __name__ == "__main__":
    input_path = "./output_data/filtered_car_data"
    output_path = "./output_data/filtered_car_data_jsonl"
    
    convert_jsonl_gz_to_jsonl(input_path, output_path)