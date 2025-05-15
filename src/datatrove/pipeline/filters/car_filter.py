from datatrove.data import Document
from datatrove.pipeline.filters.base_filter import BaseFilter
from datatrove.pipeline.writers.disk_base import DiskWriter

class CarLawFilter(BaseFilter):
    name = "🚗 Car Law Filter"
    
    def __init__(self, exclusion_writer: DiskWriter = None):
        super().__init__(exclusion_writer)
        self.vehicle_types = {"汽车", "机动车", "电动车", "新能源车", "营运车辆", "客车", "货车", "摩托车", "特种车辆", "网约车"}
        self.legal_terms = {"法规", "条例", "规定", "标准", "办法", "细则", "规范", "决定", "实施细则", "管理办法", "强制性标准", "行政处罚", "行政许可", "法律", "规章", "政策", "制度"}
        self.regulatory_bodies = {"交管", "交警", "运管", "公安", "交通管理局", "交通运输部", "市场监管", "交通执法", "交通部门"}
        self.specific_regulations = {"道路交通安全法", "机动车管理条例", "汽车产品召回管理条例", "机动车维修管理规定", "机动车强制报废标准", "道路运输条例", "机动车登记规定", "机动车驾驶证申领规定"}
        self.required_terms = {"规定", "条例", "办法", "标准", "管理"}
        self.regulation_starts = {"第一条", "第一章", "总则", "为了", "根据"}
    
    def filter(self, doc: Document) -> bool | tuple[bool, str]:
        # 1. 检查文本长度
        if len(doc.text) < 100:
            return False, "text_too_short"
        
        # 2. 统计关键词
        found_vehicle = sum(1 for k in self.vehicle_types if k in doc.text)
        found_legal = sum(1 for k in self.legal_terms if k in doc.text)
        found_regulatory = sum(1 for k in self.regulatory_bodies if k in doc.text)
        found_specific = sum(1 for k in self.specific_regulations if k in doc.text)
        
        # 3. 基本条件检查
        if found_vehicle < 1 or found_legal < 2 or (found_regulatory + found_specific) < 1:
            return False, "insufficient_keywords"
        
        # 4. 检查具体法规名称
        if found_specific > 0:
            return True
        
        # 5. 检查必要术语
        if not any(term in doc.text for term in self.required_terms):
            return False, "missing_required_terms"
        
        # 6. 检查文本开头特征
        if not any(doc.text.startswith(start) for start in self.regulation_starts):
            # 如果不是以典型法规开头，需要满足更严格的组合要求
            if found_vehicle >= 2 and found_legal >= 3 and found_regulatory >= 1:
                return True
            return False, "strict_combination_not_met"
        
        return True

class CarManufacturingFilter(BaseFilter):
    name = "🏭 Car Manufacturing Filter"
    
    def __init__(self, exclusion_writer: DiskWriter = None):
        super().__init__(exclusion_writer)
        # 定义汽车制造相关的关键词组
        self.vehicle_types = {
            "轿车", "SUV", "新能源车", "电动汽车", "混合动力车", "商用车",
            "乘用车", "客车", "货车", "专用车", "重型车", "轻型车"
        }
        
        self.manufacturing_processes = {
            "生产线", "装配线", "冲压", "焊装", "涂装", "总装",
            "下线", "制造", "组装", "加工", "生产制造", "工艺",
            "自动化", "智能制造", "柔性生产", "精益生产"
        }
        
        self.components = {
            "车身", "底盘", "发动机", "变速箱", "动力总成", "电池组",
            "电机", "电控", "悬架", "制动系统", "转向系统", "传动系统",
            "车架", "车门", "仪表盘", "座椅", "内饰", "外饰"
        }
        
        self.quality_standards = {
            "质量标准", "技术标准", "工艺标准", "产品标准", "检验标准",
            "测试规范", "质量体系", "ISO", "质量管理", "品质控制",
            "可靠性", "耐久性", "一致性", "合格率"
        }
        
        self.manufacturing_equipment = {
            "机器人", "自动化设备", "生产设备", "检测设备", "装配设备",
            "焊接设备", "喷涂设备", "冲压设备", "模具", "夹具",
            "AGV", "立体仓库", "物流系统"
        }
        
        self.manufacturing_management = {
            "产能", "良品率", "产量", "工时", "节拍", "效率",
            "成本控制", "库存管理", "供应链", "物料管理", "生产计划",
            "质量管理", "工艺管理", "设备管理"
        }
        
        self.technical_parameters = {
            "技术参数", "工艺参数", "设计参数", "性能指标", "公差",
            "精度", "尺寸", "强度", "硬度", "材料规格"
        }
        
        self.r_and_d = {
            "研发", "设计", "开发", "试制", "试验", "测试",
            "样车", "原型车", "设计验证", "工艺验证", "性能验证"
        }
        
        # 定义必须同时出现的关键词组合
        self.required_combinations = [
            (self.vehicle_types, self.manufacturing_processes),  # 车型和制造工艺
            (self.vehicle_types, self.components),  # 车型和零部件
            (self.manufacturing_processes, self.components),  # 制造工艺和零部件
            (self.manufacturing_processes, self.quality_standards),  # 制造工艺和质量标准
        ]
        
        # 制造相关的专业术语
        self.manufacturing_indicators = {
            "生产线", "制造", "工艺", "装配", "质量",
            "设备", "产能", "良品率", "工时"
        }
    
    def filter(self, doc: Document) -> bool | tuple[bool, str]:
        # 1. 检查文本长度
        if len(doc.text) < 100:
            return False, "text_too_short"
        
        # 2. 统计各类关键词出现情况
        found_vehicle = set(k for k in self.vehicle_types if k in doc.text)
        found_process = set(k for k in self.manufacturing_processes if k in doc.text)
        found_component = set(k for k in self.components if k in doc.text)
        found_quality = set(k for k in self.quality_standards if k in doc.text)
        found_equipment = set(k for k in self.manufacturing_equipment if k in doc.text)
        
        # 3. 基本要求：必须包含足够的制造相关词
        if len(found_process) < 2:
            return False, "insufficient_manufacturing_terms"
        
        # 4. 检查必要组合
        combinations_met = 0
        for group1, group2 in self.required_combinations:
            if any(k1 in doc.text for k1 in group1) and any(k2 in doc.text for k2 in group2):
                combinations_met += 1
        
        # 至少满足两种组合
        if combinations_met < 2:
            return False, "insufficient_combinations"
        
        # 5. 特殊情况：如果包含大量制造设备和工艺词，可以直接通过
        if len(found_process) >= 3 and len(found_equipment) >= 2:
            return True
        
        # 6. 检查是否包含制造相关的专业术语
        indicator_count = sum(1 for term in self.manufacturing_indicators if term in doc.text)
        if indicator_count < 3:
            return False, "insufficient_manufacturing_indicators"
        
        return True

class CarMoldFilter(BaseFilter):
    name = "🔧 Car Mold Filter"
    
    def __init__(self, exclusion_writer: DiskWriter = None):
        super().__init__(exclusion_writer)
        # 定义模具相关的关键词组
        self.mold_types = {
            "模具", "模型", "模板", "模芯", "模仁", "模座", "模架", "模盒",
            "注塑模", "压铸模", "冲压模", "锻造模", "吹塑模", "挤出模", "热压模"
        }
        
        self.mold_materials = {
            "钢材", "铝材", "铜材", "合金", "碳钢", "不锈钢", "模具钢", "硬质合金",
            "热作钢", "冷作钢", "P20", "H13", "SKD11", "NAK80", "S136", "718", "2738"
        }
        
        self.manufacturing_processes = {
            "加工", "制造", "生产", "设计", "开发", "制作", "铣削", "车削", "磨削",
            "电火花", "线切割", "抛光", "热处理", "淬火", "回火", "氮化", "镀铬"
        }
        
        self.mold_components = {
            "顶针", "推杆", "导柱", "导套", "斜顶", "滑块", "分型面", "浇口", "流道",
            "冷却系统", "加热系统", "脱模系统", "定位环", "定位块", "镶件", "镶针"
        }
        
        self.quality_standards = {
            "精度", "公差", "表面粗糙度", "尺寸", "形位公差", "硬度", "强度", "耐磨性",
            "耐腐蚀性", "使用寿命", "模次", "疲劳强度", "热膨胀", "热变形"
        }
        
        self.product_applications = {
            "塑料制品", "金属制品", "汽车零部件", "家电外壳", "电子产品", "医疗器械",
            "包装容器", "玩具", "建材", "航空航天", "军工产品", "日用品"
        }
        
        self.industry_terms = {
            "注塑", "压铸", "冲压", "锻造", "吹塑", "挤出", "热压", "旋压", "拉深",
            "成型", "脱模", "浇注", "收缩", "变形", "开模", "合模", "试模"
        }
        
        self.technical_parameters = {
            "温度", "压力", "速度", "时间", "流量", "力", "扭矩", "应力", "应变",
            "摩擦", "磨损", "润滑", "冷却", "加热", "保压", "填充", "排气"
        }
        
        # 定义必须同时出现的关键词组合
        self.required_combinations = [
            (self.mold_types, self.manufacturing_processes),  # 必须同时包含模具类型和制造工艺
            (self.mold_types, self.mold_materials),  # 模具类型和材料
            (self.mold_types, self.mold_components),  # 模具类型和组件
            (self.mold_types, self.industry_terms),  # 模具类型和行业术语
        ]
    
    def filter(self, doc: Document) -> bool | tuple[bool, str]:
        # 1. 检查文本长度
        if len(doc.text) < 100:
            return False, "text_too_short"
        
        # 2. 统计关键词
        found_mold = set(k for k in self.mold_types if k in doc.text)
        found_material = set(k for k in self.mold_materials if k in doc.text)
        found_process = set(k for k in self.manufacturing_processes if k in doc.text)
        found_component = set(k for k in self.mold_components if k in doc.text)
        found_quality = set(k for k in self.quality_standards if k in doc.text)
        found_industry = set(k for k in self.industry_terms if k in doc.text)

        # # 简单单层过滤：如果包含任何关键词，则返回True
        # if (len(found_mold) >= 1 or len(found_material) >= 1 or len(found_process) >= 1 or len(found_component) >= 1 or len(found_quality) >= 1 or len(found_industry) >= 1):
        #     return True
        
        # 3. 基本要求：必须包含模具类型
        if len(found_mold) < 1:
            return False, "no_mold_type_found"
        
        # 4. 检查必要组合
        combinations_met = 0
        for group1, group2 in self.required_combinations:
            if any(k1 in doc.text for k1 in group1) and any(k2 in doc.text for k2 in group2):
                combinations_met += 1
        
        # 至少满足一种组合
        if combinations_met < 1:
            return False, "insufficient_combinations"
        
        # 5. 特殊情况：如果包含大量模具制造相关词，可以直接通过
        if (len(found_mold) >= 2 and len(found_process) >= 2) or \
           (len(found_mold) >= 2 and len(found_component) >= 2):
            return True
        
        # 6. 检查是否包含足够的专业术语
        if len(found_industry) < 2:
            return False, "insufficient_industry_terms"
        
        return True