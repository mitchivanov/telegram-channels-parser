import asyncpg
import csv
import base64
import pickle
import os
import asyncio
from datetime import datetime

PG_DSN = os.environ.get("PG_DSN") or "postgresql://postgres:postgres@db:5432/filter"
CSV_PATH = "entities.csv"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS telegram_entities (
    id BIGINT PRIMARY KEY,
    username VARCHAR(128) UNIQUE,
    entity_data BYTEA NOT NULL,
    updated_at TIMESTAMP DEFAULT now()
);
"""

async def import_entities():
    conn = await asyncpg.connect(PG_DSN)
    await conn.execute(CREATE_TABLE_SQL)
    with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            username = row['username']
            entity_id = int(row['id'])
            entity_data = base64.b64decode(row['entity_data'])
            updated_at = row.get('updated_at')
            if updated_at:
                if isinstance(updated_at, str):
                    updated_at = datetime.fromisoformat(updated_at)
            else:
                updated_at = datetime.utcnow()
            # UPSERT
            await conn.execute("""
                INSERT INTO telegram_entities (id, username, entity_data, updated_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (id) DO UPDATE
                SET username = EXCLUDED.username,
                    entity_data = EXCLUDED.entity_data,
                    updated_at = EXCLUDED.updated_at
            """, entity_id, username, entity_data, updated_at)
            print(f"[OK] {username} ({entity_id}) импортирован")
    await conn.close()

if __name__ == '__main__':
    asyncio.run(import_entities())