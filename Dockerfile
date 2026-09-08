FROM python:3.12-slim

WORKDIR /app
COPY binance_latency_probe.py .

CMD ["python", "binance_latency_probe.py"]
