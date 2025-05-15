import os
import json
import random
import re
import time
from snownlp import SnowNLP
from itertools import cycle
from datetime import datetime

def collect_jsonl_files(directories):
    """遍历指定目录，收集所有.jsonl文件路径"""
    file_paths = []
    for dir_path in directories:
        if not os.path.exists(dir_path):
            print(f"警告：目录 {dir_path} 不存在，已跳过")
            continue
        for root, _, files in os.walk(dir_path):
            for file in files:
                if file.lower().endswith('.jsonl'):
                    file_paths.append(os.path.join(root, file))
    return file_paths

def split_sentences(text):
    """改进的分句函数，支持中英文标点，使用正则或SnowNLP分句"""
    # # 使用正则表达式处理常见分句模式
    sentences = re.findall(r'[^。！？!?.]+[。！？!?.]?', text)
    sentences = [s.strip() for s in sentences if s.strip()]

    # # 使用SnowNLP处理常见分句模式
    # s = SnowNLP(text)
    # sentences = s.sentences

    return sentences

def process_jsonl_file(file_path, score_field, threshold, max_sentence_num=0):
    """处理单个.jsonl文件，过滤低分样本，支持分句或整文本提取"""
    samples = []
    used_file = {}
    used_ids = set()
    try:
        with open(file_path, 'rb') as f:
            # used_ids = set()
            for line_num, line_bytes in enumerate(f, 1):
                try:
                    line = line_bytes.decode('utf-8')
                except UnicodeDecodeError:
                    try:
                        line = line_bytes.decode('gbk')  # 尝试其他编码
                    except:
                        print(f"文件 {file_path} 第 {line_num} 行JSON读取失败")
                        continue
                
                try:
                    item = json.loads(line)
                    text = item.get('text', '').strip()
                    if not text:
                        continue

                    # 检查分数有效性
                    score = item.get(score_field)
                    if score is None:
                        continue
                    try:
                        score = float(score)
                    except (ValueError, TypeError):
                        continue
                    if score < threshold:    # 过滤低分样本
                        continue

                    used_ids.add(item.get('id', 'none'))

                    if max_sentence_num <= 0:
                        # if "目前,中国" in text:
                        #     pass
                        sample_text = re.sub(r'\s+', ' ', text.replace('\n', '\\n')).strip()
                        samples.append(sample_text)
                    else:
                        # 分句处理
                        sentences = split_sentences(text)
                        if not sentences:
                            continue

                        # 动态生成样本
                        current_index = 0
                        while current_index < len(sentences):
                            # 随机抽取1-10句
                            n = random.randint(1, 10)
                            end_index = current_index + n
                            if end_index > len(sentences):
                                end_index = len(sentences)
                            
                            selected = sentences[current_index:end_index]
                            sample_text = ' '.join(selected)
                            
                            # 简单清理文本
                            sample_text = re.sub(r'\s+', ' ', sample_text).strip()
                            if len(sample_text) >= 10:  # 最小长度限制
                                samples.append(sample_text)
                            
                            current_index = end_index
                except Exception as e:
                    print(f"文件 {file_path} 第 {line_num} 行JSON解析失败")

            # used_file[file_path] = used_ids

    except Exception as e:
        print(f"读取文件 {file_path} 时发生异常: {str(e)}")
    return samples, used_ids

def process_class(directories, score_field, threshold, required=None):
    """处理单个分类数据，包括文件收集、样本生成、进度监控和抽样"""
    print(f"\n{'='*40}")
    print(f"开始处理分类：{score_field} (阈值: {threshold})")
    start_time = time.time()
    
    # 收集所有JSONL文件
    jsonl_files = collect_jsonl_files(directories)
    if not jsonl_files:
        raise ValueError(f"未找到任何JSONL文件")
    
    print(f"找到 {len(jsonl_files)} 个JSONL文件")
    
    # 并行处理所有文件
    all_samples = []
    class_used_ids = set()
    total_processed = 0
    for idx, file_path in enumerate(jsonl_files, 1):
        file_samples, file_used_ids = process_jsonl_file(file_path, score_field, threshold)
        class_used_ids = class_used_ids.union(file_used_ids)
        all_samples.extend(file_samples)
        total_processed += len(file_samples)
        
        # 进度报告
        elapsed = time.time() - start_time
        avg_speed = total_processed / elapsed if elapsed > 0 else 0
        if idx == 36:
            pass
        print(f"\r处理进度: {idx}/{len(jsonl_files)} | "
              f"已生成 {total_processed} 条样本 | "
              f"平均速度 {avg_speed:.1f}条/秒", end='')
    
    # 抽样处理
    if required and len(all_samples) > required:
        print(f"\n正在随机抽样 {required} 条样本...")
        all_samples = random.sample(all_samples, required)
    
    print(f"\n处理完成，共获得 {len(all_samples)} 条有效样本")
    print(f"耗时: {time.time()-start_time:.2f}秒")
    return all_samples, class_used_ids

def sample_jsonl_files_traverse(input_folder, n):
    """
    蓄水池抽样算法，从所有JSONL文件中随机抽取n条数据，并写入输出文件。
    
    :param input_folder: 输入文件夹路径，包含多个JSONL文件
    :param output_file: 输出文件路径，结果将写入此文件
    :param n: 需要抽取的数据条数
    """
    reservoir = []
    total_count = 0  # 记录已处理的总行数
    
    # 遍历输入文件夹中的所有文件
    for filename in os.listdir(input_folder):
        if not filename.endswith('.jsonl'):
            continue  # 跳过非JSONL文件
        
        file_path = os.path.join(input_folder, filename)
        
        # 逐行读取文件
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue  # 跳过空行
                
                total_count += 1
                
                # 蓄水池抽样算法
                if len(reservoir) < n:
                    reservoir.append(line)
                else:
                    # 生成随机数决定是否替换
                    r = random.randrange(total_count)
                    if r < n:
                        reservoir[r] = line
    
    # 确保最多只保留n条数据（处理总数据量不足n的情况）
    reservoir = reservoir[:n]
    
    return reservoir

def sample_jsonl_files_beginning(folder_path, n, all_used_ids=None):
    '''按文件均匀分配抽样量，优先从文件开头读取，避免重复ID'''
    # 获取文件夹下所有的 jsonl 文件
    jsonl_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.jsonl')]
    total_files = len(jsonl_files)
    if total_files == 0:
        return []

    # 预先分配每个文件需要提供的数据数量
    initial_allocation = [n // total_files] * total_files
    remainder = n % total_files
    for i in range(remainder):
        initial_allocation[i] += 1
    dic_initial_allocation = {}
    for file_path, allocation in zip(jsonl_files, initial_allocation):
        dic_initial_allocation[file_path] = allocation

    sampled_data = []
    remaining_files = []
    remaining_n = n

    # 第一轮遍历文件
    for file_path, allocation in zip(jsonl_files, initial_allocation):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                count = 0
                for line in file:
                    if count < allocation:
                        data = json.loads(line)
                        if data['id'] not in all_used_ids:
                            sampled_data.append(data)
                            count += 1
                            remaining_n -= 1
                    else:
                        break
                if count == allocation:
                    remaining_files.append(file_path)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    # 如果还有剩余需求，再次遍历未完全贡献的文件
    if remaining_n > 0 and remaining_files:
        for file_path in remaining_files:
            if remaining_n <= 0:
                break
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    for line_index, line in enumerate(file, 1):
                        if line_index > dic_initial_allocation[file_path]:
                            if remaining_n > 0:
                                if data['id'] not in all_used_ids:
                                    data = json.loads(line)
                                    sampled_data.append(data)
                                    remaining_n -= 1
                            else:
                                break
            except Exception as e:
                print(f"Error reading {file_path}: {e}")

    return sampled_data

def generate_dataset(config):
    """主函数，协调各分类数据处理，平衡样本量，合并结果并写入文件"""
    print("\n" + "="*60)
    print(" " * 20 + "分类数据集生成开始")
    print("="*60)
    
    start_time = time.time()
    dataset = []
    all_used_ids = set()    # 初始化空数据集列表和已使用ID集合

    count_num_all_class = []     # 初始化每个分类的样本数量列表
    
    # 处理每个分类，为每个分类生成带标签的样本
    for class_name, class_config in config['classes'].items():
        try:
            samples, class_used_ids = process_class(        # 处理单个分类数据，包括文件收集、样本生成、进度监控和抽样
                directories=class_config['dirs'],
                score_field=class_config['score_field'],
                threshold=class_config['threshold'],
                required=class_config.get('required')
            )
            all_used_ids = all_used_ids.union(class_used_ids)   # 合并已使用ID集合
            # 添加标签
            labeled_samples = [f"__label__{class_name} {text}" for text in samples]     # 为每个样本添加标签
            dataset.append(labeled_samples)
            count_num_all_class.append(len(labeled_samples))     # 记录每个分类的样本数量
            print(f"{class_name} 类添加 {len(labeled_samples)} 条样本")
        except Exception as e:
            print(f"处理 {class_name} 类时出错: {str(e)}")
            continue
    #动态补充other类数据以平衡分布。
    min_length = min(len(sublist) for sublist in dataset)    # 获取最短分类的样本数量
    other_data = sample_jsonl_files_beginning(config['other_class']['dirs'][0], min_length*5, all_used_ids)  # 从other类中抽取样本
    other_data = random.sample(other_data, min_length)
    dataset.append(["__label__other " + re.sub(r'\s+', ' ', data['text']).strip() for data in other_data])
    
    for d in dataset:
        random.shuffle(d)

    result = []
    for sublist in dataset:
        result.extend(sublist[:min_length])  # 将每个分类的样本数量限制为最短分类的样本数量
    
    # 打乱数据集
    random.shuffle(result)

    # 获取当前时间并格式化  
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  
    # 分离文件名和扩展名  
    file_name, file_extension = os.path.splitext(config['output_path'])  
    # 拼接新的文件名  
    config['output_path'] = f"{file_name}_{timestamp}{file_extension}"
    
    # 写入文件
    output_dir = os.path.dirname(config['output_path'])
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    with open(config['output_path'], 'w', encoding='utf-8') as f:
        f.write('\n'.join(result))
    
    # 最终报告
    total_time = time.time() - start_time
    print("\n" + "="*60)
    print(f"数据集生成完成！")
    print(f"总样本数: {len(dataset)} 条")
    print(f"总耗时: {total_time:.2f} 秒")
    print(f"输出文件: {os.path.abspath(config['output_path'])}")
    print("="*60)

if __name__ == "__main__":
    # 配置文件
    config = {
        'classes': {
            # 'car': {
            #     'dirs': [
            #         r"\\path\to\car\jsonl\files",
            #     ],
            #     'score_field': 'score',
            #     'threshold': 4,
            #     'required': 6000
            # },
            'mold': {
                'dirs': [
                    r"D:\Projects\data\fasttext_train\mold_markdown\45",
                ],
                'score_field': 'mold-score',    # 分数字段
                'threshold': 4,   # 过滤低分样本
                'required': 3000
            },
            # 'laws': {
            #     'dirs': [
            #         r"D:\Pycharm_Projects\xuzhu_pretrain_finetune\data\CCI3-HQ-keyword-filter-qwen72-1",
            #     ],
            #     'score_field': 'law-score',
            #     'threshold': 2.9,   # 过滤低分样本
            #     # 'required': 6000
            # },
            # 'other': {
            #     'dirs': [
            #         r"D:\Data\xuzhu\CCI3-HQ\data",
            #     ],
            #     'score_field': 'score',
            #     'threshold': 4,
            #     'required': 6000
            # }
        },
        'other_class': {
                'dirs': [
                    r"D:\Projects\data\fasttext_train\mold_less_than_4",
                ],
                'score_field': 'mold-score',
                'threshold': 0,
                'required': 3000
            },
        'output_path': './training_dataset_cci3_llm_mark.txt'
    }

    generate_dataset(config)