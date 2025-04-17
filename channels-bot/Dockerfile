FROM python:3.11-slim
WORKDIR /app
COPY main.py .
RUN pip install --no-cache-dir aiogram aiokafka python-dotenv minio
CMD ["python", "main.py"] 