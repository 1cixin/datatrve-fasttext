import json
from multiprocessing.dummy import freeze_support
import os
import sys
import io
from datatrove.io import DataFolder
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.writers import JsonlWriter
from datatrove.executor.local import LocalPipelineExecutor
from loguru import logger

def main():
    # ==================== 1. 强制全局UTF-8环境 ====================
    os.environ.update({
        "PYTHONUTF8": "1",
        "PYTHONIOENCODING": "utf-8",
        "DATATROVE_COLORIZE_LOGS": "0",
        "NO_COLOR": "1"
    })

    # 重定向标准输出
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

    # ==================== 2. 配置日志系统 ====================
    logger.remove()
    logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
    logger.add("test_jsonlreader.log", encoding="utf-8", rotation="10 MB")

    # ==================== 3. 准备测试数据 ====================
    TEST_DATA = [
        {"text": "普通ASCII文本", "meta": {"id": 1}},
        {"text": "中文测试", "meta": {"id": 2}},
        {"text": "特殊符号 ★☆♡", "meta": {"id": 3}},
        {"text": "Emoji测试 📊📈", "meta": {"id": 4}}
    ]

    input_dir = "./input_data"
    output_dir = "./test_output"
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    # 写入测试JSONL文件（UTF-8编码）
    input_file = os.path.join(input_dir, "/part_000047.jsonl")
    with open(input_file, "w", encoding="utf-8") as f:
        for item in TEST_DATA:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    # ==================== 4. 配置并运行管道 ====================
    try:
        logger.info("=== 开始JsonlReader测试 ===")
        
        # 推荐方式 - 在创建DataFolder时指定编码
        data_folder = DataFolder("./input_data", encoding="utf-8") 

        # 然后在Reader中使用这个DataFolder
        reader = JsonlReader(data_folder=data_folder)

        # # 关键配置：显式指定编码
        # reader = JsonlReader(
        #     input_dir,
        #     encoding="utf-8",  # 必须明确指定
        #     errors="replace",  # 处理非法字符
        #     compression=None   # 明确禁用压缩
        # )
        
        writer = JsonlWriter(output_dir)
        
        pipeline = [reader, writer]
        
        # 执行管道
        executor = LocalPipelineExecutor(
            pipeline,
            tasks=1,
            logging_dir=output_dir
        )
        
        executor.run()
        
        # 验证输出文件
        output_file = os.path.join(output_dir, "0.jsonl")
        if os.path.exists(output_file):
            with open(output_file, "r", encoding="utf-8") as f:
                lines = [json.loads(line) for line in f]
                logger.success(f"成功处理 {len(lines)} 条记录")
                for i, item in enumerate(lines[:2]):  # 打印前两条验证
                    logger.info(f"记录{i+1}: {str(item)[:100]}...")
        else:
            logger.error("输出文件未生成")

    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        # 打印详细的错误上下文
        import traceback
        logger.error(traceback.format_exc())

    finally:
        # 清理测试文件（可选）
        import shutil
        shutil.rmtree(input_dir, ignore_errors=True)
        shutil.rmtree(output_dir, ignore_errors=True)
        logger.info("已清理测试文件")

    logger.info("=== 测试结束 ===")
    
    pass

if __name__ == '__main__':
    freeze_support()  # Windows多进程必须调用
    main()  # 将原有代码移到main()函数中