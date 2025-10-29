FROM python:3.11-slim

WORKDIR /app

# Установка системных зависимостей
RUN apt-get update && apt-get install -y gcc && apt-get clean

# Копирование requirements и установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Создание директорий
RUN mkdir -p /app/session /app/config

# Копирование кода
COPY . .

# Запуск синхронизации
CMD ["python", "-u", "sync_channels.py"]
