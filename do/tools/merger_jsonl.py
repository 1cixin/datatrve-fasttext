import os
import json
from glob import glob

def process_jsonl_files(input_dir, output_file, records_per_file=50):
    """
    处理文件夹中的所有JSONL文件，每个文件取前N条，合并后保存
    
    参数:
        input_dir: 输入文件夹路径
        output_file: 输出文件路径
        records_per_file: 每个文件取多少条记录
    """
    # 获取所有JSONL文件
    jsonl_files = glob(os.path.join(input_dir, '*.jsonl'))
    
    if not jsonl_files:
        print(f"警告: 在 {input_dir} 中没有找到任何JSONL文件")
        return
    
    total_records = 0
    merged_data = []
    
    print(f"开始处理 {len(jsonl_files)} 个JSONL文件...")
    
    for file_path in jsonl_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # 读取前50条记录
                file_records = []
                for i, line in enumerate(f):
                    if i >= records_per_file:
                        break
                    file_records.append(json.loads(line.strip()))
                
                # 添加到合并列表
                merged_data.extend(file_records)
                count = len(file_records)
                total_records += count
                print(f"已处理 {os.path.basename(file_path)}: 提取 {count} 条记录")
                
        except Exception as e:
            print(f"处理文件 {file_path} 时出错: {str(e)}")
            continue
    
    # 写入合并后的文件
    if merged_data:
        with open(output_file, 'w', encoding='utf-8') as f:
            for record in merged_data:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
        
        print(f"\n处理完成! 共合并 {len(jsonl_files)} 个文件")
        print(f"总记录数: {total_records}")
        print(f"结果已保存到: {output_file}")
    else:
        print("警告: 没有提取到任何数据")

if __name__ == "__main__":
    # 输入文件夹路径
    input_dir = r"D:\Projects\datatrove_e\datatrove\output_data\recall\last4000000\read"
    output_file = r"D:\Projects\datatrove_e\datatrove\output_data\recall\last4000000\read\part-001835-a894b46e_vllm.jsonl"
    # input_dir = r"D:\Projects\data\fasttext_train\mold_less_than_4"  # 替换为您的输入目录
    # output_file = r"D:\Projects\data\fasttext_train\merged_mold_less_than_4.jsonl"
    
    # 确保输入文件夹存在
    if not os.path.isdir(input_dir):
        raise NotADirectoryError(f"输入文件夹不存在: {input_dir}")
    
    # 确保输出目录存在
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # 执行处理
    process_jsonl_files(input_dir, output_file, records_per_file=100000)