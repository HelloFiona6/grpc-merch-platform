FROM python:3.10-slim

WORKDIR /app

# Install psycopg2 dependencies
RUN apt-get update && \
    apt-get install -y gcc python3-dev libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Expose gRPC port
EXPOSE 50051

CMD ["python", "server.py"]
