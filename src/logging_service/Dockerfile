FROM python:3.10-slim

WORKDIR /app

# 安装 Kafka 客户端依赖
RUN apt-get update && \
    apt-get install -y gcc python3-dev && \
    rm -rf /var/lib/apt/lists/*

# 安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 拷贝源码
COPY . .

EXPOSE 50052

CMD ["python", "server.py"]
