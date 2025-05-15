import json
import glob
import os
from collections import defaultdict

def analyze_jsonl_scores(file_path):
    """
    分析JSONL文件中mold-score的分布情况
    
    参数:
        file_path: 可以是单个.jsonl文件路径或包含多个.jsonl文件的目录
        
    返回:
        dict: 包含统计结果和错误信息的字典
    """
    results = {
        'score_distribution': defaultdict(int),
        'invalid_scores': [],
        'missing_field': 0,
        'file_errors': [],
        'total_files': 0,
        'total_records': 0
    }
    
    # 获取文件列表
    if os.path.isdir(file_path):
        files = glob.glob(os.path.join(file_path, '*.jsonl')) + \
                glob.glob(os.path.join(file_path, '*.jsonlines'))
        if not files:
            results['file_errors'].append(f"目录中没有找到JSONL文件: {file_path}")
            return results
    else:
        files = [file_path]
    
    results['total_files'] = len(files)
    
    for file in files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        record = json.loads(line.strip())
                        if 'mold-score' not in record:
                            results['missing_field'] += 1
                            continue
                            
                        score = record['mold-score']
                        if isinstance(score, int) and 0 <= score <= 5:
                            results['score_distribution'][score] += 1
                            results['total_records'] += 1
                        else:
                            results['invalid_scores'].append({
                                'file': os.path.basename(file),
                                'line': line_num,
                                'value': score
                            })
                    except json.JSONDecodeError:
                        results['file_errors'].append(
                            f"JSON解析失败 {os.path.basename(file)} 第{line_num}行"
                        )
        except Exception as e:
            results['file_errors'].append(
                f"处理文件 {os.path.basename(file)} 时出错: {str(e)}"
            )
    
    return results

def print_jsonl_results(results):
    """打印JSONL分析结果"""
    print("\n" + "="*50)
    print(f"JSONL分析完成 - 共处理 {results['total_files']} 个文件")
    print(f"总有效记录数: {results['total_records']}")
    
    print("\n分数分布统计:")
    for score in range(6):
        count = results['score_distribution'].get(score, 0)
        print(f"  {score}分: {count}条 ({count/results['total_records']*100:.1f}%)" 
              if results['total_records'] > 0 else f"  {score}分: {count}条")
    
    print(f"\n缺少mold-score字段的记录: {results['missing_field']}")
    print(f"非法分数数量: {len(results['invalid_scores'])}")
    
    if results['invalid_scores']:
        print("\n前5条非法分数记录:")
        for item in results['invalid_scores'][:5]:
            print(f"  文件: {item['file']} 行: {item['line']} 值: {item['value']}")
        if len(results['invalid_scores']) > 5:
            print(f"  (仅显示前5条，共{len(results['invalid_scores'])}条非法记录)")
    
    if results['file_errors']:
        print("\n处理过程中遇到的错误:")
        for error in results['file_errors'][:3]:
            print(f"  - {error}")
        if len(results['file_errors']) > 3:
            print(f"  (仅显示前3条，共{len(results['file_errors'])}个错误)")
    
    print("="*50 + "\n")

if __name__ == "__main__":
    input_path = r"D:\Projects\data\fasttext_train\mold_train_0514"
    
    if not os.path.exists(input_path):
        print(f"错误: 路径不存在 - {input_path}")
    else:
        results = analyze_jsonl_scores(input_path)
        print_jsonl_results(results)