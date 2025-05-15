"""
这个文件实现以下功能：
1 - 读取本地的 jsonl 格式文件
2 - 过滤数据集（只保留与模具相关的内容）
3 - 将过滤后的数据保存为新的 jsonl 文件
"""

import argparse
import json
from pathlib import Path

# 创建参数解析器
parser = argparse.ArgumentParser("Filter local jsonl files for mold-related content")

# 添加命令行参数
parser.add_argument("input_file", type=str, help="输入的 jsonl 文件路径")
parser.add_argument("output_file", type=str, help="输出的 jsonl 文件路径")
parser.add_argument("--text_key", type=str, help="文本内容的键名", default="text")
parser.add_argument("--min_keywords", type=int, help="最少需要包含的关键词数量", default=2)

# 定义模具相关的关键词组
MOLD_TYPES = {
    "模具", "模型", "模板", "模芯", "模仁", "模座", "模架", "模盒",
    "注塑模", "压铸模", "冲压模", "锻造模", "吹塑模", "挤出模", "热压模"
}

MOLD_MATERIALS = {
    "钢材", "铝材", "铜材", "合金", "碳钢", "不锈钢", "模具钢", "硬质合金",
    "热作钢", "冷作钢", "P20", "H13", "SKD11", "NAK80", "S136", "718", "2738"
}

MANUFACTURING_PROCESSES = {
    "加工", "制造", "生产", "设计", "开发", "制作", "铣削", "车削", "磨削",
    "电火花", "线切割", "抛光", "热处理", "淬火", "回火", "氮化", "镀铬"
}

MOLD_COMPONENTS = {
    "顶针", "推杆", "导柱", "导套", "斜顶", "滑块", "分型面", "浇口", "流道",
    "冷却系统", "加热系统", "脱模系统", "定位环", "定位块", "镶件", "镶针"
}

QUALITY_STANDARDS = {
    "精度", "公差", "表面粗糙度", "尺寸", "形位公差", "硬度", "强度", "耐磨性",
    "耐腐蚀性", "使用寿命", "模次", "疲劳强度", "热膨胀", "热变形"
}

PRODUCT_APPLICATIONS = {
    "塑料制品", "金属制品", "汽车零部件", "家电外壳", "电子产品", "医疗器械",
    "包装容器", "玩具", "建材", "航空航天", "军工产品", "日用品"
}

INDUSTRY_TERMS = {
    "注塑", "压铸", "冲压", "锻造", "吹塑", "挤出", "热压", "旋压", "拉深",
    "成型", "脱模", "浇注", "收缩", "变形", "开模", "合模", "试模"
}

TECHNICAL_PARAMETERS = {
    "温度", "压力", "速度", "时间", "流量", "力", "扭矩", "应力", "应变",
    "摩擦", "磨损", "润滑", "冷却", "加热", "保压", "填充", "排气"
}

# 合并所有关键词组
MOLD_KEYWORDS = (
    MOLD_TYPES | MOLD_MATERIALS | MANUFACTURING_PROCESSES | MOLD_COMPONENTS |
    QUALITY_STANDARDS | PRODUCT_APPLICATIONS | INDUSTRY_TERMS | TECHNICAL_PARAMETERS
)

# 定义必须同时出现的关键词组合
REQUIRED_COMBINATIONS = [
    (MOLD_TYPES, MANUFACTURING_PROCESSES),  # 必须同时包含模具类型和制造工艺
    (MOLD_TYPES, MOLD_MATERIALS),  # 模具类型和材料
    (MOLD_TYPES, MOLD_COMPONENTS),  # 模具类型和组件
    (MOLD_TYPES, INDUSTRY_TERMS),  # 模具类型和行业术语
]

def contains_mold_content(text):
    """
    检查文本是否包含模具相关内容，使用更严格的过滤规则
    """
    # 统计文本中包含的关键词
    found_keywords = set()
    for keyword in MOLD_KEYWORDS:
        if keyword in text:
            found_keywords.add(keyword)
    
    # 检查是否满足必要的关键词组合
    for group1, group2 in REQUIRED_COMBINATIONS:
        if any(k1 in text for k1 in group1) and any(k2 in text for k2 in group2):
            return True
            
    return False

def process_jsonl(input_file, output_file, text_key, min_keywords=2, max_samples=1000000):
    """
    处理 jsonl 文件并过滤内容
    参数：
        input_file: 输入文件路径
        output_file: 输出文件路径
        text_key: 文本内容的键名
        min_keywords: 最少需要包含的关键词数量
        max_samples: 最多处理的样本数量，默认为10000
    """
    # 确保输出目录存在
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    
    filtered_count = 0
    total_count = 0
    
    with open(input_file, 'r', encoding='utf-8') as fin, \
         open(output_file, 'w', encoding='utf-8') as fout:
        
        for line in fin:
            total_count += 1
            # 限制只处理前max_samples个样本
            if total_count > max_samples:
                break
                
            try:
                data = json.loads(line.strip())
                text = data.get(text_key, "")
                
                if contains_mold_content(text):
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
