FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN apt-get update && apt-get install -y gcc && pip install --no-cache-dir -r requirements.txt && apt-get remove -y gcc && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# Создаем директорию для хранения состояния и устанавливаем права
RUN mkdir -p /app/data && chmod 777 /app/data

COPY updated_sources.txt ./
COPY . .

# Устанавливаем переменную среды для указания пути к файлу состояния
ENV STATE_FILE=/app/data/state.json 

CMD ["python", "main.py"] 