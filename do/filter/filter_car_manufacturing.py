"""
这个文件实现以下功能：
1 - 读取本地的 jsonl 格式文件
2 - 过滤数据集（只保留与汽车制造相关的内容）
3 - 将过滤后的数据保存为新的 jsonl 文件
"""

import argparse
import json
from pathlib import Path

# 创建参数解析器
parser = argparse.ArgumentParser("Filter local jsonl files for car manufacturing content")

# 添加命令行参数
parser.add_argument("input_file", type=str, help="输入的 jsonl 文件路径")
parser.add_argument("output_file", type=str, help="输出的 jsonl 文件路径")
parser.add_argument("--text_key", type=str, help="文本内容的键名", default="text")
parser.add_argument("--min_keywords", type=int, help="最少需要包含的关键词数量", default=3)

# 定义汽车制造相关的关键词组
VEHICLE_TYPES = {
    "轿车", "SUV", "新能源车", "电动汽车", "混合动力车", "商用车",
    "乘用车", "客车", "货车", "专用车", "重型车", "轻型车"
}

MANUFACTURING_PROCESSES = {
    "生产线", "装配线", "冲压", "焊装", "涂装", "总装",
    "下线", "制造", "组装", "加工", "生产制造", "工艺",
    "自动化", "智能制造", "柔性生产", "精益生产"
}

COMPONENTS = {
    "车身", "底盘", "发动机", "变速箱", "动力总成", "电池组",
    "电机", "电控", "悬架", "制动系统", "转向系统", "传动系统",
    "车架", "车门", "仪表盘", "座椅", "内饰", "外饰"
}

QUALITY_STANDARDS = {
    "质量标准", "技术标准", "工艺标准", "产品标准", "检验标准",
    "测试规范", "质量体系", "ISO", "质量管理", "品质控制",
    "可靠性", "耐久性", "一致性", "合格率"
}

MANUFACTURING_EQUIPMENT = {
    "机器人", "自动化设备", "生产设备", "检测设备", "装配设备",
    "焊接设备", "喷涂设备", "冲压设备", "模具", "夹具",
    "AGV", "立体仓库", "物流系统"
}

MANUFACTURING_MANAGEMENT = {
    "产能", "良品率", "产量", "工时", "节拍", "效率",
    "成本控制", "库存管理", "供应链", "物料管理", "生产计划",
    "质量管理", "工艺管理", "设备管理"
}

TECHNICAL_PARAMETERS = {
    "技术参数", "工艺参数", "设计参数", "性能指标", "公差",
    "精度", "尺寸", "强度", "硬度", "材料规格"
}

R_AND_D = {
    "研发", "设计", "开发", "试制", "试验", "测试",
    "样车", "原型车", "设计验证", "工艺验证", "性能验证"
}

# 合并所有关键词组
CAR_MANUFACTURING_KEYWORDS = (
    VEHICLE_TYPES | MANUFACTURING_PROCESSES | COMPONENTS |
    QUALITY_STANDARDS | MANUFACTURING_EQUIPMENT | MANUFACTURING_MANAGEMENT |
    TECHNICAL_PARAMETERS | R_AND_D
)

# 定义必须同时出现的关键词组合
REQUIRED_COMBINATIONS = [
    (VEHICLE_TYPES, MANUFACTURING_PROCESSES),  # 车型和制造工艺
    (VEHICLE_TYPES, COMPONENTS),  # 车型和零部件
    (MANUFACTURING_PROCESSES, COMPONENTS),  # 制造工艺和零部件
    (MANUFACTURING_PROCESSES, QUALITY_STANDARDS),  # 制造工艺和质量标准
]

def contains_car_manufacturing_content(text):
    """
    检查文本是否包含汽车制造相关内容
    """
    # 1. 检查文本长度
    if len(text) < 100:
        return False
    
    # 2. 统计各类关键词出现情况
    found_vehicle = set(k for k in VEHICLE_TYPES if k in text)
    found_process = set(k for k in MANUFACTURING_PROCESSES if k in text)
    found_component = set(k for k in COMPONENTS if k in text)
    found_quality = set(k for k in QUALITY_STANDARDS if k in text)
    found_equipment = set(k for k in MANUFACTURING_EQUIPMENT if k in text)
    
    # 3. 基本要求：必须包含足够的制造相关词
    if len(found_process) < 2:
        return False
    
    # 4. 检查必要组合
    combinations_met = 0
    for group1, group2 in REQUIRED_COMBINATIONS:
        if any(k1 in text for k1 in group1) and any(k2 in text for k2 in group2):
            combinations_met += 1
    
    # 至少满足两种组合
    if combinations_met < 2:
        return False
    
    # 5. 特殊情况：如果包含大量制造设备和工艺词，可以直接通过
    if len(found_process) >= 3 and len(found_equipment) >= 2:
        return True
    
    # 6. 检查是否包含制造相关的专业术语
    manufacturing_indicators = [
        "生产线", "制造", "工艺", "装配", "质量",
        "设备", "产能", "良品率", "工时"
    ]
    
    indicator_count = sum(1 for term in manufacturing_indicators if term in text)
    if indicator_count < 3:
        return False
    
    return True

def process_jsonl(input_file, output_file, text_key, min_keywords=3):
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
                
                if contains_car_manufacturing_content(text):
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
    args = parser.parse_args()
    process_jsonl(args.input_file, args.output_file, args.text_key, args.min_keywords) 