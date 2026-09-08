FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY binance_ws_latency_probe.py .

CMD ["python", "binance_ws_latency_probe.py"]
