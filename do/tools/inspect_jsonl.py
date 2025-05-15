import json
from pathlib import Path
from typing import List, Dict, Union


def inspect_jsonl(filepath):
    """检查JSONL文件结构并统计总条数"""
    total_count = 0
    with open(filepath, "r", encoding="utf-8") as f:
        # 读取第一行获取字段结构
        first_line = json.loads(next(f))
        total_count += 1
        
        # 继续统计剩余行数
        for line in f:
            if line.strip():  # 跳过空行
                total_count += 1

        print("字段结构:", first_line.keys())
        print("示例数据:", first_line)
        print("总数据条数:", total_count)
    return total_count

def process_jsonl(
    input_file: str,
    skip_n: int = 0,
    extract_n: Union[int, None] = None,
    output_file: Union[str, None] = None
) -> List[Dict]:
    """
    处理JSONL文件：跳过前N条，提取后续M条数据
    
    参数:
        input_file: 输入JSONL文件路径
        skip_n: 要跳过的行数（默认0）
        extract_n: 要提取的条数（None表示提取所有剩余数据）
        output_file: 可选，保存结果的文件路径
        
    返回:
        提取的数据列表
    """
    extracted = []
    processed = 0
    skipped = 0
    
    with open(input_file, 'r', encoding='utf-8') as infile:
        for line in infile:
            # 跳过前N条
            if skipped < skip_n:
                skipped += 1
                continue
                
            # 检查是否达到提取数量限制
            if extract_n is not None and processed >= extract_n:
                break
                
            # 处理当前行
            try:
                data = json.loads(line)
                extracted.append(data)
                processed += 1
            except json.JSONDecodeError:
                print(f"警告：跳过无效JSON行（原始行号：{skipped + processed + 1}）")
                continue
    
    # 可选保存到文件
    if output_file:
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as outfile:
            for item in extracted:
                outfile.write(json.dumps(item, ensure_ascii=False) + '\n')
        
        print(f"处理完成：跳过前{skip_n}条，提取了{processed}条数据")
        print(f"结果已保存到: {output_file}")
    
    return extracted

# 使用示例
if __name__ == "__main__":
    filepath = r"D:\Projects\data\fasttext_train\bigdata\vllm\read\00000_filter.jsonl"
    
    # 检查文件结构
    total = inspect_jsonl(filepath)
    
    # result = process_jsonl(
    #     input_file=filepath,
    #     # skip_n=4000000,
    #     skip_n=0,
    #     extract_n=100000,
    #     # extract_n=1000000,
    #     # output_file=r"D:\Projects\data\fasttext_train\bigdata\top_5000000_samples.jsonl"
    #     output_file=r"D:\Projects\data\fasttext_train\bigdata\100000_samples.jsonl"
    # )