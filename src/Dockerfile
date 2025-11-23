FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc python3-dev && rm -rf /var/lib/apt/lists/*

COPY api_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api_service /app
COPY protos_shared /app/protos_shared

EXPOSE 8080

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080"]
