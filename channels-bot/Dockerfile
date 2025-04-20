FROM python:3.11-slim
WORKDIR /app
COPY main.py .
COPY requirements.txt .
COPY cleanup_worker.py .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "main.py"] 