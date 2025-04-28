#!/bin/bash
set -e

if [ -f /app/entities.csv ]; then
    echo "[ENTRYPOINT] Импортируем сущности из entities.csv в базу..."
    python import_entities_from_csv.py
else
    echo "[ENTRYPOINT] Файл entities.csv не найден, пропускаем импорт сущностей."
fi

exec python main.py 