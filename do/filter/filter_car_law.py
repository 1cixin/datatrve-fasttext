"""
这个文件实现以下功能：
1 - 读取本地的 jsonl 格式文件
2 - 过滤数据集（只保留与汽车法律法规相关的内容）
3 - 将过滤后的数据保存为新的 jsonl 文件
"""

import argparse
import json
from pathlib import Path

# 创建参数解析器
parser = argparse.ArgumentParser("Filter local jsonl files for car-related legal content")

# 添加命令行参数
parser.add_argument("input_file", type=str, help="输入的 jsonl 文件路径")
parser.add_argument("output_file", type=str, help="输出的 jsonl 文件路径")
parser.add_argument("--text_key", type=str, help="文本内容的键名", default="text")
parser.add_argument("--min_keywords", type=int, help="最少需要包含的关键词数量", default=2)

# 调整关键词组，更聚焦于法律法规
VEHICLE_TYPES = {
    "汽车", "机动车", "电动车", "新能源车", "营运车辆",
    "客车", "货车", "摩托车", "特种车辆", "网约车"
}

LEGAL_TERMS = {
    "法规", "条例", "规定", "标准", "办法", "细则", "规范",
    "决定", "实施细则", "管理办法", "强制性标准", "行政处罚", "行政许可",
    "法律", "规章", "政策", "制度"
}

REGULATORY_BODIES = {
    "交管", "交警", "运管", "公安", "交通管理局", "交通运输部",
    "市场监管", "交通执法", "交通部门"
}

SPECIFIC_REGULATIONS = {
    "道路交通安全法", "机动车管理条例", "汽车产品召回管理条例",
    "机动车维修管理规定", "机动车强制报废标准", "道路运输条例",
    "机动车登记规定", "机动车驾驶证申领规定"
}

def contains_car_legal_content(text):
    """
    使用更严格的规则检查文本是否包含汽车法律相关内容
    """
    # 1. 首先检查文本长度，过短的文本可能不是完整的法规内容
    if len(text) < 100:
        return False
    
    # 2. 统计文本中包含的关键词
    found_vehicle = set(k for k in VEHICLE_TYPES if k in text)
    found_legal = set(k for k in LEGAL_TERMS if k in text)
    found_regulatory = set(k for k in REGULATORY_BODIES if k in text)
    found_specific = set(k for k in SPECIFIC_REGULATIONS if k in text)
    
    # 3. 必须满足以下所有条件：
    # - 至少包含1个车辆类型词
    # - 至少包含2个法律术语
    # - 至少包含1个监管机构或具体法规名称
    if len(found_vehicle) < 1 or len(found_legal) < 2 or (len(found_regulatory) + len(found_specific)) < 1:
        return False
    
    # 4. 检查是否包含具体法规名称（这是最强的指标）
    if found_specific:
        return True
    
    # 5. 如果没有具体法规名称，需要满足更严格的组合要求
    required_terms = ["规定", "条例", "办法", "标准", "管理"]
    if not any(term in text for term in required_terms):
        return False
    
    # 6. 检查文本开头是否包含典型的法规文件特征
    regulation_starts = ["第一条", "第一章", "总则", "为了", "根据"]
    if not any(text.startswith(start) for start in regulation_starts):
        # 如果不是以典型法规开头，需要满足更多关键词
        return len(found_vehicle) >= 2 and len(found_legal) >= 3 and len(found_regulatory) >= 1
    
    return True

def process_jsonl(input_file, output_file, text_key, min_keywords=2):
    """
    处理 jsonl 文件并过滤内容
    """
    # 确保输出目录存在
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    
    filtered_count = 0
    total_count = 0
    
    with open(input_file, 'r', encoding='utf-8') as fin, \
         open(output_file, 'w', encoding='utf-8') as fout:
        
        for line in fin:
            total_count += 1
            try:
                data = json.loads(line.strip())
                text = data.get(text_key, "")
                
                if contains_car_legal_content(text):
                    filtered_count += 1
                    json.dump(data, fout, ensure_ascii=False)
                    fout.write('\n')
            except json.JSONDecodeError:
                print(f"警告：跳过无效的 JSON 行")
                continue
    
    print(f"处理完成！")
    print(f"总条目数：{total_count}")
    print(f"过滤后条目数：{filtered_count}")
    print(f"过滤率：{filtered_count/total_count*100:.2f}%")

if __name__ == "__main__":
    # 测试时手动指定参数（替代命令行输入）
    test_args = [
        "./input_data",         # input_file
        "./output_data",         # output_file
        "--text_key", "text",         # 文本内容的键名
        "--min_keywords", "2"     # 最少需要包含的关键词数量
    ]
    args = parser.parse_args(test_args)
    # args = parser.parse_args()
    process_jsonl(args.input_file, args.output_file, args.text_key, args.min_keywords)
