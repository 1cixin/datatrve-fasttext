import pandas as pd
import glob
import os
from collections import defaultdict

def analyze_mold_scores(parquet_path):
    """
    分析Parquet文件中mold-score的分布情况
    
    参数:
        parquet_path: 可以是单个.parquet文件路径或包含多个.parquet文件的目录
        
    返回:
        dict: 包含统计结果和错误信息的字典
    """
    # 结果容器
    results = {
        'score_distribution': defaultdict(int),
        'invalid_scores': [],
        'missing_field': False,
        'other_errors': []
    }
    
    # 确定输入是文件还是目录
    if os.path.isdir(parquet_path):
        file_list = glob.glob(os.path.join(parquet_path, '*.parquet'))
        if not file_list:
            results['other_errors'].append(f"目录中没有找到Parquet文件: {parquet_path}")
            return results
    else:
        file_list = [parquet_path]
    
    total_records = 0
    
    for file_path in file_list:
        try:
            df = pd.read_parquet(file_path)
            
            # 检查字段是否存在
            if 'mold-score' not in df.columns:
                results['missing_field'] = True
                results['other_errors'].append(f"文件缺少mold-score字段: {os.path.basename(file_path)}")
                continue
            
            # 统计分数分布
            for score in df['mold-score']:
                try:
                    score = int(score)
                    if 0 <= score <= 5:
                        results['score_distribution'][score] += 1
                        total_records += 1
                    else:
                        results['invalid_scores'].append(score)
                except (ValueError, TypeError):
                    results['invalid_scores'].append(score)
                    
        except Exception as e:
            results['other_errors'].append(f"处理文件 {os.path.basename(file_path)} 时出错: {str(e)}")
    
    return {
        'file_count': len(file_list),
        'total_records': total_records,
        'score_distribution': dict(results['score_distribution']),
        'invalid_score_count': len(results['invalid_scores']),
        'missing_field': results['missing_field'],
        'errors': results['other_errors']
    }

def print_analysis_results(results):
    """打印分析结果"""
    print("\n" + "="*50)
    print(f"分析完成 - 共处理 {results['file_count']} 个文件")
    print(f"总有效记录数: {results['total_records']}")
    
    print("\n分数分布统计:")
    for score in range(6):
        count = results['score_distribution'].get(score, 0)
        print(f"  {score}分: {count}条 ({count/results['total_records']*100:.1f}%)" if results['total_records'] > 0 else f"  {score}分: {count}条")
    
    print(f"\n非法分数数量: {results['invalid_score_count']}")
    if results['invalid_score_count'] > 0:
        print("  注意: 文件中包含非[0,5]范围的分数值")
    
    if results['missing_field']:
        print("\n警告: 部分文件缺少'mold-score'字段")
    
    if results['errors']:
        print("\n处理过程中遇到的错误:")
        for error in results['errors']:
            print(f"  - {error}")
    
    print("="*50 + "\n")

def extract_scores_to_new_parquet(parquet_path, output_path, target_scores):
    """
    提取指定分数段的数据并保存为新Parquet文件
    
    参数:
        parquet_path: 输入文件或目录路径
        output_path: 输出文件路径
        target_scores: 需要提取的分数列表，如[0,1]或[4,5]
    """
    if not isinstance(target_scores, (list, tuple, set)):
        raise ValueError("target_scores必须是列表、元组或集合")
    
    if os.path.isdir(parquet_path):
        file_list = glob.glob(os.path.join(parquet_path, '*.parquet'))
        if not file_list:
            print(f"错误: 目录中没有找到Parquet文件: {parquet_path}")
            return
    else:
        file_list = [parquet_path]
    
    extracted_dfs = []
    
    for file_path in file_list:
        try:
            df = pd.read_parquet(file_path)
            
            if 'mold-score' not in df.columns:
                print(f"跳过文件(缺少mold-score字段): {os.path.basename(file_path)}")
                continue
            
            # 筛选目标分数
            filtered = df[df['mold-score'].isin(target_scores)]
            if not filtered.empty:
                extracted_dfs.append(filtered)
                print(f"从 {os.path.basename(file_path)} 提取到 {len(filtered)} 条记录")
            
        except Exception as e:
            print(f"处理文件 {os.path.basename(file_path)} 时出错: {str(e)}")
    
    if not extracted_dfs:
        print("没有提取到任何符合条件的记录")
        return
    
    # 合并并保存
    final_df = pd.concat(extracted_dfs, ignore_index=True)
    final_df.to_parquet(output_path, index=False)
    
    print(f"\n成功提取 {len(final_df)} 条记录")
    print(f"已保存到: {output_path}")
    
    # 显示提取结果的分数分布
    score_counts = final_df['mold-score'].value_counts().sort_index()
    print("\n提取结果分数分布:")
    for score in sorted(target_scores):
        print(f"  {score}分: {score_counts.get(score, 0)}条")

if __name__ == "__main__":
    # 使用示例
    input_path = r"D:\Projects\datatrove_e\datatrove\output_data\recall\top_1000000_samples_filtered_car\00000_vllm.parquet"
    
    if not os.path.exists(input_path):
        print(f"错误: 路径不存在 - {input_path}")
    else:
        # 1. 先分析数据分布
        results = analyze_mold_scores(input_path)
        print_analysis_results(results)
        
        # 2. 提取特定分数数据（示例：提取4-5分数据）
        print("\n准备提取数据...")
        extract_scores_to_new_parquet(
            parquet_path=input_path,
            output_path=r"D:\Projects\datatrove_e\datatrove\output_data\recall\top_1000000_samples_filtered_car\00000_vllm_scores_45.parquet",
            target_scores=[4, 5]  # 可修改为需要提取的分数
            # target_scores=[2]  # 可修改为需要提取的分数
        )