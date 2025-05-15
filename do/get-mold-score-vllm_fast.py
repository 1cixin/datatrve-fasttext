# from langchain_ollama import ChatOllama
# from langchain_core.messages import HumanMessage
import json
from openai import OpenAI
import os
import re
import pandas as pd
# from filter_hf_dataset_mold import contains_mold_content
import requests
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from tqdm import tqdm


def do_inference(content, sys_prompt = None, sleep = 5, replace_str = None, chat_url = None):
    # 判断是否传入chat_url参数，如果传入则使用传入的url，否则使用默认的url
    payload = { #
        "model": "/hf/Qwen/Qwen2.5-72B-Instruct/",   #Qwen/QwQ-32B   qwen3-32b  Qwen/Qwen2.5-72B-Instruct
        "messages": [
            {'role': 'user',
            'content': content}
        ],
        "stream": False,
        "temperature": 0,
        "max_tokens": 10, # 返回1024个token
        "enable_thinking":False,
    }
    headers = {
        "Authorization": "EMPTY",
        "Content-Type": "application/json"
    }
    # headers = {
    #     "Authorization": f"Bearer {'sk-ornolnlzsikbftfwaaggnlmerndnhaiznatstubnpfpyaamg'}",
    #     "Content-Type": "application/json"
    # }
    # url = "https://api.siliconflow.cn/v1/chat/completions"  "http://localhost:9232/v1/chat/completions",
    try:
        # 发送POST请求
        response = requests.request("POST", "http://localhost:9232/v1/chat/completions", 
                                 json=payload, 
                                 headers=headers, 
                                 verify=True,
                                 timeout=10)  # 添加超时设置
        
        response.raise_for_status()  # 检查HTTP错误
        response_json = response.json()
        
        # 打印完整响应
        # print(f"响应内容: {json.dumps(response_json, ensure_ascii=False)}")
        
        # 检查响应格式
        if 'choices' not in response_json or not response_json['choices']:
            # print(f"Unexpected API response format: {response_json}")
            return "0"  # 返回默认值
            
        exct_key = 'content'
        res = response_json['choices'][0]['message'][exct_key]
        return res
    except requests.exceptions.RequestException as e:
        print(f"Request error: {str(e)}")
        return "0"
    except (KeyError, IndexError) as e:
        print(f"Response parsing error: {str(e)}")
        return "0"
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return "0" 
    

def get_mold_score(user_input: str) -> str:
    """极简版评分（保持相同评分标准但缩短prompt）"""
    ultra_prompt = f"""
    [模具评分规则] 0-5分标准:
    1+含模具基础信息​：若内容包含与模具主题相关的基础信息（即使含广告等非学术内容）
    2+提及模具元素​：若内容提及模具相关元素但未严格对标课程标准，可能混杂非模具内容或呈现方式散乱
    3+关键概念​：若内容连贯地介绍关键模具/冲模概念（如注塑成型、冷却系统），适合初学者但存在不足（如缺少实例或过度简化高级内容）。
    4+结构化知识​：若内容高度契合模具行业需求，提供结构化技术知识（如设计原则、案例分析），内容精炼，类似教材章节或专业教程。
    5+技术深度​：若内容中描述的技术深度突出（如高级模拟、行业标准），逻辑清晰且实用（如分步流程、故障排除指南），无冗余干扰。
    示例: "模具设计"→1 "注塑成型原理"→3 "冷却系统设计要点"→4
    请直接输出分数: {user_input[:300]}  # 限制输入长度
    """

    max_retries = 2
    for attempt in range(max_retries):
        try:
            response = do_inference(ultra_prompt)
            print(f"原始响应（尝试{attempt}）:\n{response}")

            # 新分数提取方法：直接取响应中的第一个数字
            def extract_first_number(text):
                match = re.search(r'\d', text)
                return int(match.group()) if match else None

            score = extract_first_number(response)
            
            # 验证分数有效性
            if score is not None and 0 <= score <= 5:
                return str(score)
            else:
                print(f"警告：未找到有效分数，响应内容: {response}")
                
        except Exception as e:
            print(f"尝试{attempt}出错: {str(e)}")
            if attempt == max_retries - 1:
                break

    print("所有尝试失败，返回安全值0")
    return "0"

def add_mold_score(input_file, output_file):
    # 如果输出文件不存在，创建它
    if not os.path.exists(output_file):
        open(output_file, 'w').close()
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'a', encoding='utf-8') as outfile:
        
        for line in infile:
            # 解析JSONL中的每一行
            record = json.loads(line.strip())
            try:
            # 添加mold-score字段，随机生成0-5的整数
              record['mold-score'] = int(get_mold_score(record['text']))
              #if record['mold-score'] != 0:
            
                # 将更新后的记录写入新的JSONL文件
              outfile.write(json.dumps(record, ensure_ascii=False) + '\n')
            except:
                record['mold-score'] =0

def add_mold_score(df, output_file):
    # 如果输出文件不存在，创建它
    if not os.path.exists(output_file):
        open(output_file, 'w').close()
    
    with open(output_file, 'a', encoding='utf-8') as outfile:
        for idx, record in df.iterrows():
            try:
                # 添加mold-score字段
                record['mold-score'] = int(get_mold_score(record['text']))
            except Exception as e:
                print(f"Error processing record {idx}: {str(e)}")
                record['mold-score'] = 0
            
            # 无论mold-score是多少，都写入文件
            outfile.write(json.dumps(record.to_dict(), ensure_ascii=False) + '\n') 

def add_mold_score_batch(df, output_file, batch_size=100):
    # 如果输出文件不存在，创建它
    if not os.path.exists(output_file):
        open(output_file, 'w').close()
    
    # 初始化输出文件（覆盖模式）
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("")  # 清空文件
    
    # 分批处理
    with open(output_file, 'a', encoding='utf-8') as outfile:
        for i in tqdm(range(0, len(df), batch_size), desc="处理批次"):
            batch = df.iloc[i:i+batch_size]
            
            # 处理当前批次
            batch_results = []
            for _, row in batch.iterrows():
                try:
                    score = int(get_mold_score(row['text']))
                except Exception as e:
                    print(f"\n处理失败: {str(e)}")
                    score = 0
                
                # 构造结果记录
                record = row.to_dict()
                record['mold-score'] = score
                batch_results.append(record)
            
            # 整批写入
            for record in batch_results:
                outfile.write(json.dumps(record, ensure_ascii=False) + '\n')
            
            # 立即刷新缓存（确保数据写入磁盘）
            outfile.flush()

def add_mold_score_parquet(df, output_file, batch_size=1000):
    # 确保输出目录存在
    os.makedirs(Path(output_file).parent, exist_ok=True)
    # 复制DF避免修改原数据
    df = df.copy()
    
    # 转换所有object类型的列为字符串（包括字典）
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else str(x))
    
    # 动态Schema生成（更新版）
    def auto_detect_schema(df):
        schema_fields = []
        for col, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                schema_fields.append(pa.field(col, pa.int64()))
            elif pd.api.types.is_float_dtype(dtype):
                schema_fields.append(pa.field(col, pa.float64()))
            else:
                # 所有其他类型（包括转换后的字典）存为字符串
                schema_fields.append(pa.field(col, pa.string()))
        schema_fields.append(pa.field("mold-score", pa.int32()))
        return pa.schema(schema_fields)
    
    schema = auto_detect_schema(df)
    
    # 批处理计数器
    batch_count = 0
    
    try:
        with pq.ParquetWriter(
            output_file,
            schema=schema,
            compression='snappy',
            flavor='spark'
        ) as writer:
            for start_idx in range(0, len(df), batch_size):
                batch = df.iloc[start_idx:start_idx + batch_size].copy()
                
                # 逐条获取大模型评分
                for idx in batch.index:
                    try:
                        text = str(batch.loc[idx, 'text']) if pd.notnull(batch.loc[idx, 'text']) else ""    ##text  content
                        batch.loc[idx, 'mold-score'] = int(get_mold_score(text))
                    except Exception as e:
                        print(f"Error processing row {idx}: {str(e)}")
                        batch.loc[idx, 'mold-score'] = 0
                
                # 类型安全转换
                try:
                    table = pa.Table.from_pandas(
                        batch,
                        schema=schema,
                        preserve_index=False
                    )
                    writer.write_table(table)
                    
                    batch_count += 1
                    processed_rows = min(start_idx + batch_size, len(df))
                    print(f"已处理批次 {batch_count} | 进度: {processed_rows}/{len(df)} ({processed_rows/len(df):.1%})")
                
                except pa.ArrowTypeError as e:
                    print(f"类型转换失败: {str(e)}")
                    print("当前批次数据类型:\n", batch.dtypes)
                    raise
                
    except Exception as e:
        print(f"处理过程中断: {str(e)}")
        # 删除可能不完整的输出文件
        if os.path.exists(output_file):
            os.remove(output_file)
        raise
    finally:
        if 'writer' in locals():
            writer.close()
        print(f"处理完成! 输出文件: {os.path.abspath(output_file)}")
        print(f"总处理行数: {len(df)} | 批次数: {batch_count}")


if __name__ == "__main__":

    # input_file = '/workspace/CCI3-HQ-keyword-filter-mold-bert/part_000086-keyword-filter-qwen-output.jsonl'  # 输入文件路径
    # output_file = '/workspace/CCI3-HQ-keyword-filter-mold-qwen72/part_000086-keyword-filter-qwen-output.jsonl'  # 输出文件路径
    # add_mold_score(input_file, output_file)
    # input_file = r"D:/Projects/datatrove_e/datatrove/output_data/merged_mold_09_11.parquet"
    input_file = r"D:\Projects\data\fasttext_train\bigdata\vllm\00000_filter.jsonl"
    # output_file = r"D:/Projects/datatrove_e/datatrove/output_data/merged_mold_09_11_vllm.parquet"
    output_file = r"D:\Projects\data\fasttext_train\bigdata\vllm\00000_filter_vllm.jsonl"

    # 逐行评分
    df = pd.read_json(input_file, lines=True)
    add_mold_score_batch(df, output_file, batch_size=100)

    # df = pd.read_parquet(input_file).head(1)
    # add_mold_score_parquet(df, output_file)

    # 批量评分
    # add_mold_score_batch(input_file, output_file, batch_size=20)

        
