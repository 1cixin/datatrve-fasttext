import fasttext
import pandas as pd
import json
import random
import time
import os
import io
from pathlib import Path
import jieba
import nltk
from nltk.tokenize import word_tokenize
import re
from sklearn.metrics import classification_report

def load_vectors(fname):
    """加载完整词向量文件（如.vec格式）"""
    fin = io.open(fname, 'r', encoding='utf-8', newline='\n', errors='ignore')
    n, d = map(int, fin.readline().split())  # 读取词数和维度
    data = {}
    line_count = 0
    for line in fin:
        tokens = line.rstrip().split(' ')
        data[tokens[0]] = map(float, tokens[1:])  # {单词: 向量}
        line_count += 1
    return data

def load_vectors_partial(fname, num_lines, output_fname):  
    """加载前N行词向量并保存为新文件""" 
    with io.open(fname, 'r', encoding='utf-8', newline='\n', errors='ignore') as fin:  
        # 读取文件头，表示总词数和维度  
        n, d = map(int, fin.readline().split())  
        # 实际要读取的行数不能超过原总行数  
        num_lines = min(num_lines, n)
        # 词向量数据字典  
        data = {}  
        # 读取指定行数的向量  
        for i, line in enumerate(fin):  
            if i >= num_lines:  
                break  
            tokens = line.rstrip().split(' ')  
            data[tokens[0]] = list(map(float, tokens[1:]))  
    # 写入新的vec文件  
    with io.open(output_fname, 'w', encoding='utf-8', newline='\n') as fout:  
        # 写文件头，行数改为num_lines，维度d不变  
        fout.write(f"{num_lines} {d}\n")  
        for word, vector in data.items():  
            vector_str = ' '.join(map(str, vector))  
            fout.write(f"{word} {vector_str}\n")  

    return data 

def tokenize_text(text):
    # 判断是否为中文
    return re.sub(r'\s+', ' ', ' '.join(jieba.lcut(text)))
    # if any('\u4e00' <= char <= '\u9fff' for char in text):
    #     return ' '.join(jieba.lcut(text))
    # else:
    #     return ' '.join(word_tokenize(text))

def process_file(input_file_path, output_file_path):
    """对输入文件进行分词和标签格式化"""
    start_time = time.time()
    with open(input_file_path, 'r', encoding='utf-8') as input_file:
        lines = input_file.readlines()

    output_lines = []
    for index, line in enumerate(lines, 1):
        if line.strip():
            line_split = line.split(maxsplit=1)
            if len(line_split) == 2:
                label, text = line_split
                tokenized_text = tokenize_text(text.strip())
                output_lines.append(f"{label} {tokenized_text}\n")
            else:
                print(f"第{index}行数据不符合标准")

    # 一次性写入所有过滤后的数据
    with open(output_file_path, 'w', encoding='utf-8') as output_file:
        output_file.writelines(output_lines)

    print(f"分词处理耗时 {time.time()-start_time:.1f}s")

def load_data(file_path):
    """读取CSV/JSONL/EXCEL文件并返回处理后的文本列表"""
    texts = []
    
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path, encoding='utf-8')
        raw_texts = df['text'].dropna().tolist()
    elif file_path.endswith('.jsonl'):
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    texts.append(data['text'])
                except:
                    continue
    elif file_path.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(file_path)
        raw_texts = df['text'].dropna().tolist()
    else:
        raise ValueError("仅支持CSV、JSONL和Excel文件")

    # 统一处理编码和格式化
    processed_texts = [
        t.encode('utf-8', 'ignore').decode('utf-8').strip().replace('\n', ' ') 
        for t in (raw_texts if not file_path.endswith('.jsonl') else texts)
        if len(t.strip()) > 0
    ]
    
    return processed_texts

def load_fasttext_train_data(file_path):
    """加载FastText格式的训练数据（标签+文本）"""
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split(' ', 1)
            if len(parts) == 2:
                data.append({'label': parts[0], 'text': parts[1]})
    return pd.DataFrame(data)

def prepare_training_data(texts, output_file="train.txt"):
    """准备FastText训练数据，为每条文本分配一个临时标签"""
    # 这里需要手动标注数据，这里仅作示例
    # 实际使用时需要替换为真实的标注数据
    labels = ["__label__车辆", "__label__模具", "__label__法律", "__label__其他"]
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for text in texts:
            # 这里随机分配标签仅作示例
            # 实际使用时应该使用真实标注
            label = random.choice(labels)
            f.write(f"{label} {text}\n")
    
    return output_file

def train_fasttext_model(train_file, model_path="model_fasttext.bin"):
    """训练FastText模型"""
    # 训练模型
    model = fasttext.train_supervised(
        input=train_file,
        lr=0.05,            # 学习率（控制参数更新幅度）
        epoch=32,           # 训练轮次（全数据迭代次数）
        wordNgrams=2,       # 使用2-gram特征（捕捉相邻词组合）
        verbose=2,          # 日志级别（2=详细输出）
        minCount=1,         # 词频阈值（出现次数<1的词被忽略）
        # pretrainedVectors=r"C:\Users\mg42m\Downloads\cc.zh.300.8000.vec",   # 预训练词向量路径
        pretrainedVectors=r"D:\Projects\fasttext\cc.zh.300.vec",   # 预训练词向量路径
        dim=300,            # 词向量维度（需与预训练向量一致）
    )
    
    # 保存模型
    model.save_model(model_path)
    return model

def predict_with_fasttext(model, texts, batch_size=32):
    """使用FastText模型进行预测"""
    results = []
    
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]         # 分批处理（减少内存压力）
        predictions = model.predict(batch)      # 返回格式: (labels, scores)
        
        for j, (labels, scores) in enumerate(zip(predictions[0], predictions[1])):
            results.append({
                'text': batch[j][:50],  # 只显示前50个字符
                'label': labels[0].replace('__label__', ''),
                'confidence': scores[0]
            })
    
    return results

def benchmark(file_path, model_path=None, num_runs=0):
    """执行性能测试"""
    # 处理数据
    dirname = os.path.dirname(file_path)  
    basename = os.path.basename(file_path)  
    filename, ext = os.path.splitext(basename)
    segmentation_filename = filename + "_segmentation" + ext
    segmentation_filepath = os.path.join(dirname, segmentation_filename)
    if not os.path.exists(segmentation_filepath):
        process_file(file_path, segmentation_filepath)    # 执行分词和格式标准化
    file_path = segmentation_filepath
    # 加载数据
    all_content = load_fasttext_train_data(file_path)  # DataFrame格式
    all_labels = all_content.label.to_list()
    all_texts = all_content.text.to_list()
    # all_texts = load_data(file_path)

    if num_runs == 0:
        num_runs = len(all_texts)
    elif len(all_texts) < num_runs:
        raise ValueError(f"至少需要{num_runs}条有效数据")
    
    # 随机采样
    # sampled_texts = random.sample(all_texts, num_runs)
    combined = list(zip(all_labels, all_texts))
    sampled_labels, sampled_texts = zip(*random.sample(combined, num_runs))
    
    # 加载或训练模型
    if model_path and os.path.exists(model_path):
        model = fasttext.load_model(model_path)
    else:
        # train_file = prepare_training_data(all_texts)
        # model = train_fasttext_model(train_file)
        model = train_fasttext_model(file_path)
    
    # 执行测试
    total_time = 0.0
    results = []
    correct_count = 0
    
    true_labels = []
    predicted_labels = []
    
    for i, text in enumerate(sampled_texts, 1):
        start = time.time()
        
        # if sampled_labels[i - 1].replace('__label__', '') != 'other':
            # continue

        # 执行分类
        labels, scores = model.predict(text)  # 单条预测
        results.append([labels, scores])
        
        elapsed = time.time() - start
        total_time += elapsed

        # 对比预测结果和真实标签
        predicted_label = labels[0].replace('__label__', '')
        predicted_labels.append(predicted_label)
        true_label = sampled_labels[i - 1].replace('__label__', '')
        true_labels.append(true_label)
        if predicted_label == true_label:
            correct_count += 1
        
        print(f"第{i:2d}次 | 耗时: {elapsed:.4f}s | 置信度：{scores[0]:.2%} | "
              f"分类真值: {true_label} | "
              f"分类结果: {predicted_label} | "
              f"原文截取：{text[:20].replace('/n', '')}")
    
    # 计算分类准确率
    accuracy = correct_count / num_runs

    # 输出统计结果
    avg_time = total_time / num_runs
    print(f"\n测试完成，共{num_runs}次")
    print(f"总耗时: {total_time:.2f}s")
    print(f"平均耗时: {avg_time:.4f}s/次")
    print(f"分类准确率: {accuracy:.2%}")

    # 生成分类报告
    print("\n► 分类明细报告")
    print(classification_report(
        true_labels,
        predicted_labels,
        target_names=sorted(set(true_labels + predicted_labels)),  # 获取所有唯一标签
        digits=4
    ))

if __name__ == "__main__":
    # 示例用法
    # process_file(r"D:\Pycharm_Projects\xuzhu_pretrain_finetune\simple_data_train.txt", r"D:\Pycharm_Projects\xuzhu_pretrain_finetune\simple_data_train_processed.txt")
    # load_vectors(r"C:\Users\mg42m\Downloads\cc.zh.300.vec")
    # load_vectors_partial(r"C:\Users\mg42m\Downloads\cc.zh.300.vec", 8000, r"C:\Users\mg42m\Downloads\cc.zh.300.8000.vec")
    # train_fasttext_model(r"D:\Pycharm_Projects\xuzhu_pretrain_finetune\simple_data_train_processed.txt")

    # file_path = r"D:\Pycharm_Projects\xuzhu_pretrain_finetune\training_dataset_0423.txt"  # 替换为实际的数据文件路径
    # file_path = r"D:\Pycharm_Projects\xuzhu_pretrain_finetune\training_dataset.txt"  # 替换为实际的数据文件路径
    # file_path = r"D:\Pycharm_Projects\xuzhu_pretrain_finetune\training_dataset_new_train.txt"  # 替换为实际的数据文件路径
    # file_path = r"D:\Pycharm_Projects\xuzhu_pretrain_finetune\training_dataset_new_val.txt"  # 替换为实际的数据文件路径
    # file_path = r"D:\Pycharm_Projects\xuzhu_pretrain_finetune\simple_data_train.txt"  # 替换为实际的数据文件路径
    # file_path = r"D:\Pycharm_Projects\xuzhu_pretrain_finetune\simple_data_train_160.txt"  # 替换为实际的数据文件路径
    # file_path = r"D:\Pycharm_Projects\xuzhu_pretrain_finetune\training_dataset_cci3_llm_mark_train.txt"
    # file_path = r"D:\Pycharm_Projects\xuzhu_pretrain_finetune\training_dataset_cci3_llm_mark_val.txt"
    # file_path = r"D:\Pycharm_Projects\xuzhu_pretrain_finetune\training_dataset_new_20250427_181704_train+training_dataset_cci3_llm_mark_20250427_191714_train.txt"
    file_path = r"D:\Projects\datatrove_e\datatrove\training_dataset_cci3_llm_mark_20250514_101202.txt"
    model_path = "model_fasttext.bin"  # 模型保存路径
    
    benchmark(file_path, model_path)
    # try:
    #     benchmark(file_path, model_path)
    # except Exception as e:
    #     print(f"错误: {str(e)}") 