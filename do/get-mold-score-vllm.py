# from langchain_ollama import ChatOllama
# from langchain_core.messages import HumanMessage
import json
from openai import OpenAI
import os
import re
import jsonlines
import pandas as pd
# from filter_hf_dataset_mold import contains_mold_content
import requests
import argparse
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Union


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
        "max_tokens": 2048,
        "enable_thinking":False,
        "temperature": 0.3,
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
                                 timeout=60)  # 添加超时设置
        
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
    """
    获取模具相关评分（严格保持评分标准，增强截断处理）
    
    参数:
        user_input: 待评估的文本内容
        
    返回:
        字符串形式的评分(0-5)
    """
    # 严格保持原始prompt结构（您提供的完整评分标准）
    strict_prompt = f"""
    以下是一段内容。请根据下述累加式5分评分标准，评估该内容是否与模具/冲模设计、制造或行业应用高度相关。评分基于每条标准的满足程度逐项累加：
    ​加1分​：若内容包含与模具主题相关的基础信息（即使含广告等非学术内容）
    ​加1分​：若内容提及模具相关元素但未严格对标课程标准，可能混杂非模具内容或呈现方式散乱
    ​加1分​：若内容连贯地介绍关键模具/冲模概念（如注塑成型、冷却系统），适合初学者但存在不足（如缺少实例或过度简化高级内容）。
    ​加1分​：若内容高度契合模具行业需求，提供结构化技术知识（如设计原则、案例分析），内容精炼，类似教材章节或专业教程。
    ​加1分​：若内容中描述的技术深度突出（如高级模拟、行业标准），逻辑清晰且实用（如分步流程、故障排除指南），无冗余干扰。
    示例：
    - 用户输入："模具厂招聘：现招聘模具设计工程师，要求熟悉CAD软件，有3年以上工作经验"
      输出：1
    - 用户输入："在工业生产中，模具和机械加工都是重要的制造工艺，但模具更注重成型工艺"
      输出：2
    - 用户输入："注塑模具的基本结构包括模架、型腔、浇注系统等，这些部件共同完成塑料制品的成型"
      输出：3
    - 用户输入："模具设计中的冷却系统设计要点：1.冷却水道应均匀分布 2.水道直径通常为8-12mm 3.注意避免与顶针等部件干涉"
      输出：4
    - 用户输入：“第3章 模架结构  \n现代模具设计多采用模架，这使得模具厂可以节省大量的制模时间，缩短了工期，且使得产品的质量与精度得到了保证。  \n根据产品的特点，模架分为大水口模架与细水口模架。细水口模架又可再细分为简化型细水口模架。每个产品的模具内部结构虽有不同，但其模架结构却都相似。掌握模架的相关内容对于模具设计师来说十分有必要。  \n本章重点阐述标准模架的规格与型号，并就一副简单的模具做了详细的介绍”
      输出：5
    - 用户输入：“热的不良导体，具有消声、减振作用  \n一般来讲，塑料的导热性是比较低的，相当于钢的1／225～1／75，泡沫塑料的微孔中含有气体，其隔热、隔音、防振性好的优点。如聚氯乙烯（PVC）的导热系数仅为钢材的1／357，铝材的1／1250。在隔热能力上，单玻塑窗比单玻铝窗高40％。将塑料窗体与中空玻璃结合起来后，在住宅、写字楼、病房、宾馆中使用，冬天节省暖气、夏季节约空调开支，好处十分明显。”
      输出：5
    - 用户输入：“7.1.5 滑块压板设计  \n滑块在运动过程中要求平稳、准确，因此必须要给滑块设计导向装置。  \n原身T形槽的形式适合简单且小型的滑块，滑槽用T形刀直接在模板上加工出来的，这种形式的滑槽一旦不合适，加工修改将很麻烦。  \n压板的宽B和高A一般不小于15mm，长度L一般为模仁边至模板边的距离。可用两个或多个螺钉进行锁定，螺钉不要小于M6。”
      输出：5
    - 用户输入：“\n## 前 言\n\n本标准按照GB／T 1.1-2009给出的规则起草。\n\n本标准代替了QC／T 402-1999《卡套式锥螺纹三通接头体》。本标准与QC/T402-1999相比，主要技术变化如下：\n\n-修改了锥螺纹的引用标准和螺纹代号（见表1和表2）；\n\n-修改了接头体的压力规格，调整接头体部分尺寸（见表1）；\n\n-修改了表面处理的引用标准（见表2）；\n\n-增加了介质温度要求（见表2）。\n\n本标准的附录A为资料性附录。\n\n本标准由全国汽车标准化技术委员会（SAC／TC 114）提出并归口。\n\n本标准起草单位：柳州五菱汽车工业有限公司、中国汽车技术研究中心、瑞安市瑞强标准件有限公司、东风汽车有限公司东风商用车技术中心。\n\n本标准主要起草人：李铮、张德利、朱彤、徐枭、范丽霞、文代志、周伟。\n\n本标准所代替标准的历次版本发布情况为：\n\n-QC/T 402-1999。\n\n## 卡套式锥螺纹三通接头体\n\n### 1 范围\n\n本标准规定了卡套式锥螺纹三通接头体的型式与尺寸、技术条件。\n\n本标准适用于管子外径为4mm～35mm，最大工作压力10MPa～25MPa的汽车用油、气及一般腐蚀介质的管路系统。\n\n### 2 规范性引用文件\n\n下列文件对于本文件的应用是必不可少的。凡是注日期的引用文件，仅所注日期的版本适用于本文件。凡是不注日期的引用文件，其最新版本（包括所有的修改单）适用于本文件。\n\nGB／T 3 普通螺纹收尾、肩距、退刀槽和倒角（GB／T 3-1997，eqv ISO 3508： 1976；eqv ISO 4755：1983)\n\nGB／T 90.1 紧固件 验收检查（GB／T 90.1-2002，idt ISO 3269：2000）\n\nGB／T 90.2 紧固件 标志与包装\n\nGB／T 196 普通螺纹 基本尺寸（GB／T 196-2003，ISO 724：1993，MOD）\n\nGB／T 197 普通螺纹 公差（GB／T 197-2003，ISO965-1：1998，MOD）\n\nGB／T 3765 卡套式管接头技术条件\n\nGB／T 12716 60°密封管螺纹（GB／T 12716-2011，ASME B1.20.2M：2006，MOD）\n\nQC／T 326 汽车标准件产品编号规则\n\nQC／T 625 汽车用涂镀层和化学处理层\n\n### 3 型式与尺寸\n\n注：其余未规定的细节由制造商确定。\n\n表1 尺寸 mm\n\n|最大工作压力MPa|管子外径|d。”
      输出：0
    - 用户输入：“7．汽车车载电子网络  \n随着电子控制器件在汽车上的应用越来越多，车载电子设备间的数据通信变得越来越重要。以分布式控制系统为基础构造汽车车载电子网络系统是很有必要的。大量数据的快速交换、高可靠性及价廉是对汽车电子网络系统的要求。在该系统中，各个从处理机独立运行，改善汽车某一方面的性能，同时在其他处理器需要时提供数据服务。主处理机收集整理各个从处理机的数据，并生成车况显示。通信控制器保证数据的正常流动。  \n此外，电子技术中的集成化制造技术等在未来几年内也将会有大的突破。  \n纵观近10年来汽车技术的重大成就，大都是在应用电子技术上进行的突破，电子技术已成为汽车工业发展的重要动力源泉。目前，我国汽车工业面临入世后的巨大冲击，能否在未来的世界汽车业竞争中掌握主动权，关键取决于能否在电子技术上占领制高点。加快汽车电子技术新领域的研究是我国汽车工业发展的当务之急。  \n电子技术已经广泛应用于汽车的各个领域，极大地改善了汽车的综合性能，使汽车在安全、节能、环保、舒适等各方面都有了长足的进步。目前，汽车电子控制技术发展的最新动向包括：智能控制方法（自适应控制、模糊控制、神经网络控制、鲁棒控制、最优控制等）的引入；控制系统开发方式（车载CAN的采用、现代开发工具dSPACE的运用、层次化系统结构、X-By-Wire控制方式开发等）的革新；控制系统单元技术［半导体、多重通信、故障探测与识别（FDI）与故障诊断支持、ECU软件开发系统等］的发展，从而形成了汽车电子技术中信息处理部分的集中化，控制处理部分的分散化（危险分散、功能分散）等分层控制思想的发展趋势。  \n#### 1.2 汽车电子驱动控制技术  \n##### 1．新能源汽车中的电机控制  \n混合动力汽车和电动汽车中最常见的5种电机是感应电机（IM）、开关磁阻电机（SRM）、绕线转子同步电机（SM）、直流电机（DC）和永磁同步电机（PMSM）。储能电池最常见为4种，包含镍的电池，例如镍-铁-碱，镍金属氢化物（Ni-MH）和镍镉（NiCd）；包含钠的电池，例如氯化镍／氯化钠（Na/NiCl2）和硫化钠（NaS）；包含锂的电池，例如锂离子电池以及铅酸电池。  \n图1-1为上述电机与储能电池所构成的组合，其中PMSM＋锂电池的方案最常见。表1-1为目前汽车中驱动电动机及储能电池的使用情况。”
      输出：0
    
    请严格按以下格式输出：
    [根据上述标准逐条分析，每条用✓/✗标记并简说明]
    SCORE: [0-5]
    
    待评估内容：
    {user_input}
    """

    # 简洁版prompt（仅当截断时使用，但仍保持评分标准）
    concise_prompt = f"""
    以下是一段内容。请根据下述累加式5分评分标准，评估该内容是否与模具/冲模设计、制造或行业应用高度相关。评分基于每条标准的满足程度逐项累加：
    ​加1分​：若内容包含与模具主题相关的基础信息（即使含广告等非学术内容）
    ​加1分​：若内容提及模具相关元素但未严格对标课程标准，可能混杂非模具内容或呈现方式散乱
    ​加1分​：若内容连贯地介绍关键模具/冲模概念（如注塑成型、冷却系统），适合初学者但存在不足（如缺少实例或过度简化高级内容）。
    ​加1分​：若内容高度契合模具行业需求，提供结构化技术知识（如设计原则、案例分析），内容精炼，类似教材章节或专业教程。
    ​加1分​：若内容中描述的技术深度突出（如高级模拟、行业标准），逻辑清晰且实用（如分步流程、故障排除指南），无冗余干扰。
    示例：
    - 用户输入："模具厂招聘：现招聘模具设计工程师，要求熟悉CAD软件，有3年以上工作经验"
      输出：1
    - 用户输入："在工业生产中，模具和机械加工都是重要的制造工艺，但模具更注重成型工艺"
      输出：2
    - 用户输入："注塑模具的基本结构包括模架、型腔、浇注系统等，这些部件共同完成塑料制品的成型"
      输出：3
    - 用户输入："模具设计中的冷却系统设计要点：1.冷却水道应均匀分布 2.水道直径通常为8-12mm 3.注意避免与顶针等部件干涉"
      输出：4
    - 用户输入：“第3章 模架结构  \n现代模具设计多采用模架，这使得模具厂可以节省大量的制模时间，缩短了工期，且使得产品的质量与精度得到了保证。  \n根据产品的特点，模架分为大水口模架与细水口模架。细水口模架又可再细分为简化型细水口模架。每个产品的模具内部结构虽有不同，但其模架结构却都相似。掌握模架的相关内容对于模具设计师来说十分有必要。  \n本章重点阐述标准模架的规格与型号，并就一副简单的模具做了详细的介绍”
      输出：5
    - 用户输入：“热的不良导体，具有消声、减振作用  \n一般来讲，塑料的导热性是比较低的，相当于钢的1／225～1／75，泡沫塑料的微孔中含有气体，其隔热、隔音、防振性好的优点。如聚氯乙烯（PVC）的导热系数仅为钢材的1／357，铝材的1／1250。在隔热能力上，单玻塑窗比单玻铝窗高40％。将塑料窗体与中空玻璃结合起来后，在住宅、写字楼、病房、宾馆中使用，冬天节省暖气、夏季节约空调开支，好处十分明显。”
      输出：5
    - 用户输入：“7.1.5 滑块压板设计  \n滑块在运动过程中要求平稳、准确，因此必须要给滑块设计导向装置。  \n原身T形槽的形式适合简单且小型的滑块，滑槽用T形刀直接在模板上加工出来的，这种形式的滑槽一旦不合适，加工修改将很麻烦。  \n压板的宽B和高A一般不小于15mm，长度L一般为模仁边至模板边的距离。可用两个或多个螺钉进行锁定，螺钉不要小于M6。”
      输出：5
    - 用户输入：“\n## 前 言\n\n本标准按照GB／T 1.1-2009给出的规则起草。\n\n本标准代替了QC／T 402-1999《卡套式锥螺纹三通接头体》。本标准与QC/T402-1999相比，主要技术变化如下：\n\n-修改了锥螺纹的引用标准和螺纹代号（见表1和表2）；\n\n-修改了接头体的压力规格，调整接头体部分尺寸（见表1）；\n\n-修改了表面处理的引用标准（见表2）；\n\n-增加了介质温度要求（见表2）。\n\n本标准的附录A为资料性附录。\n\n本标准由全国汽车标准化技术委员会（SAC／TC 114）提出并归口。\n\n本标准起草单位：柳州五菱汽车工业有限公司、中国汽车技术研究中心、瑞安市瑞强标准件有限公司、东风汽车有限公司东风商用车技术中心。\n\n本标准主要起草人：李铮、张德利、朱彤、徐枭、范丽霞、文代志、周伟。\n\n本标准所代替标准的历次版本发布情况为：\n\n-QC/T 402-1999。\n\n## 卡套式锥螺纹三通接头体\n\n### 1 范围\n\n本标准规定了卡套式锥螺纹三通接头体的型式与尺寸、技术条件。\n\n本标准适用于管子外径为4mm～35mm，最大工作压力10MPa～25MPa的汽车用油、气及一般腐蚀介质的管路系统。\n\n### 2 规范性引用文件\n\n下列文件对于本文件的应用是必不可少的。凡是注日期的引用文件，仅所注日期的版本适用于本文件。凡是不注日期的引用文件，其最新版本（包括所有的修改单）适用于本文件。\n\nGB／T 3 普通螺纹收尾、肩距、退刀槽和倒角（GB／T 3-1997，eqv ISO 3508： 1976；eqv ISO 4755：1983)\n\nGB／T 90.1 紧固件 验收检查（GB／T 90.1-2002，idt ISO 3269：2000）\n\nGB／T 90.2 紧固件 标志与包装\n\nGB／T 196 普通螺纹 基本尺寸（GB／T 196-2003，ISO 724：1993，MOD）\n\nGB／T 197 普通螺纹 公差（GB／T 197-2003，ISO965-1：1998，MOD）\n\nGB／T 3765 卡套式管接头技术条件\n\nGB／T 12716 60°密封管螺纹（GB／T 12716-2011，ASME B1.20.2M：2006，MOD）\n\nQC／T 326 汽车标准件产品编号规则\n\nQC／T 625 汽车用涂镀层和化学处理层\n\n### 3 型式与尺寸\n\n注：其余未规定的细节由制造商确定。\n\n表1 尺寸 mm\n\n|最大工作压力MPa|管子外径|d。”
      输出：0
    - 用户输入：“7．汽车车载电子网络  \n随着电子控制器件在汽车上的应用越来越多，车载电子设备间的数据通信变得越来越重要。以分布式控制系统为基础构造汽车车载电子网络系统是很有必要的。大量数据的快速交换、高可靠性及价廉是对汽车电子网络系统的要求。在该系统中，各个从处理机独立运行，改善汽车某一方面的性能，同时在其他处理器需要时提供数据服务。主处理机收集整理各个从处理机的数据，并生成车况显示。通信控制器保证数据的正常流动。  \n此外，电子技术中的集成化制造技术等在未来几年内也将会有大的突破。  \n纵观近10年来汽车技术的重大成就，大都是在应用电子技术上进行的突破，电子技术已成为汽车工业发展的重要动力源泉。目前，我国汽车工业面临入世后的巨大冲击，能否在未来的世界汽车业竞争中掌握主动权，关键取决于能否在电子技术上占领制高点。加快汽车电子技术新领域的研究是我国汽车工业发展的当务之急。  \n电子技术已经广泛应用于汽车的各个领域，极大地改善了汽车的综合性能，使汽车在安全、节能、环保、舒适等各方面都有了长足的进步。目前，汽车电子控制技术发展的最新动向包括：智能控制方法（自适应控制、模糊控制、神经网络控制、鲁棒控制、最优控制等）的引入；控制系统开发方式（车载CAN的采用、现代开发工具dSPACE的运用、层次化系统结构、X-By-Wire控制方式开发等）的革新；控制系统单元技术［半导体、多重通信、故障探测与识别（FDI）与故障诊断支持、ECU软件开发系统等］的发展，从而形成了汽车电子技术中信息处理部分的集中化，控制处理部分的分散化（危险分散、功能分散）等分层控制思想的发展趋势。  \n#### 1.2 汽车电子驱动控制技术  \n##### 1．新能源汽车中的电机控制  \n混合动力汽车和电动汽车中最常见的5种电机是感应电机（IM）、开关磁阻电机（SRM）、绕线转子同步电机（SM）、直流电机（DC）和永磁同步电机（PMSM）。储能电池最常见为4种，包含镍的电池，例如镍-铁-碱，镍金属氢化物（Ni-MH）和镍镉（NiCd）；包含钠的电池，例如氯化镍／氯化钠（Na/NiCl2）和硫化钠（NaS）；包含锂的电池，例如锂离子电池以及铅酸电池。  \n图1-1为上述电机与储能电池所构成的组合，其中PMSM＋锂电池的方案最常见。表1-1为目前汽车中驱动电动机及储能电池的使用情况。”
      输出：0
    
    请严格按以下格式输出：
    [简要分析过程，每条用✓/✗标记，不要分析，直接给出分数]
    SCORE: [0-5]
    
    内容：
    {user_input[:900]}...  # 限制输入长度
    """

    # 分数提取方法（严格匹配SCORE:格式）
    def extract_score(response: str) -> Union[int, None]:
        # 优先匹配标准格式
        score_match = re.search(r'SCORE:\s*([0-5])', response)
        if score_match:
            return int(score_match.group(1))

    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            # 首次尝试使用完整标准prompt
            current_prompt = strict_prompt if attempt == 0 else concise_prompt
            response = do_inference(current_prompt)
            print(f"原始响应（尝试{attempt}）:\n{response}")

            # 检测截断（关键标记缺失或长度异常）
            is_truncated = (
                len(response.split()) > 900 or  # 接近token限制
                ('SCORE:' not in response) or
                ('</think>' not in response and attempt == 0)
            )

            score = extract_score(response)
            
            # 验证分数有效性
            if score is not None and 0 <= score <= 5:
                if not is_truncated:
                  return str(score)
                # print(f"警告：响应可能截断，但成功提取分数: {score}")
                print(f"警告：响应可能截断")
                
            # 准备重试（最后一次尝试仍使用完整标准）
            if attempt < max_retries - 1:
                print("-> 分数提取失败，准备降级重试...")
                continue
                
        except Exception as e:
            print(f"尝试{attempt}出错: {str(e)}")
            if attempt >= max_retries:
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
    input_file = r"D:\Projects\datatrove_e\datatrove\output_data\recall\top_1000000_samples_filtered_car\00000.jsonl"
    # output_file = r"D:/Projects/datatrove_e/datatrove/output_data/merged_mold_09_11_vllm.parquet"
    output_file = r"D:/Projects/datatrove_e/datatrove/output_data/recall/top_1000000_samples_filtered_car/00000_vllm.jsonl"

    df = pd.read_json(input_file, lines=True, chunksize=100000)
    # df = pd.read_parquet(input_file).head(1)

    # add_mold_score(df, output_file)
    add_mold_score_parquet(df, output_file)

        
