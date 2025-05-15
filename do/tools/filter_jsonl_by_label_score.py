import os
import json
import pandas as pd
import argparse

def filter_jsonl_by_label_score(input_file, output_file, label_name, min_score, max_score):
    """
    从JSONL文件中提取特定标签得分区间内的数据
    
    参数:
        input_file: 输入JSONL文件路径
        output_file: 输出JSONL文件路径
        label_name: 要筛选的标签名称(如'__label__other')
        min_score: 最小得分(包含)
        max_score: 最大得分(包含)
    """
    matched_count = 0
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:
        
        for line in infile:
            try:
                data = json.loads(line.strip())
                score = data['metadata'][label_name]
                
                if min_score <= score <= max_score:
                    outfile.write(line)
                    matched_count += 1
            except (json.JSONDecodeError, KeyError) as e:
                print(f"警告: 跳过无效行 - {str(e)}")
    
    print(f"筛选完成! 共找到 {matched_count} 条数据，已保存到 {output_file}")

if __name__ == "__main__":
    input_file = r"D:\Projects\data\fasttext_train\bigdata\read\00000.jsonl"
    
    # 从输入文件名提取基础名称（不含扩展名）
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    
    # 定义筛选参数
    label_name = "__label__mold"
    min_score = 0.5
    max_score = 0.6
    
    # 动态生成输出文件名
    output_file = os.path.join(
        os.path.dirname(input_file),
        f"{base_name}_{label_name.replace('__label__', '')}_{min_score}_{max_score}.jsonl"
    )
    
    # 验证参数
    if min_score > max_score:
        raise ValueError("最小得分不能大于最大得分")
    
    # 执行筛选
    filter_jsonl_by_label_score(input_file, output_file, label_name, min_score, max_score)