FROM python:3.10-slim

# 安装基础系统依赖（按需添加）
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        vim-tiny \
        htop \
        && \
    rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制依赖文件（利用缓存层优化）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    -i https://pypi.tuna.tsinghua.edu.cn/simple \
    --trusted-host pypi.tuna.tsinghua.edu.cn

# 复制代码（结构保持与本地一致）
COPY ./src /app/src
COPY ./do /app/do
COPY ./tests /app/tests

# 设置环境变量
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    HISTFILE=/app/.bash_history

# 创建挂载点目录（可选）
RUN mkdir -p /app/data /app/models && chmod -R 777 /app

# 启动交互式shell（添加基础工具）
CMD ["bash", "-c", "tail -f /dev/null"]  # 保持容器运行