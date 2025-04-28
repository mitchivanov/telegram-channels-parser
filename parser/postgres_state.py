import os
import asyncpg
import logging
import pickle

class PostgresStateManager:
    def __init__(self, pool):
        self.pool = pool
        self.logger = logging.getLogger("parser.postgres_state")

    @classmethod
    async def create(cls):
        pg_host = os.environ.get("POSTGRES_HOST", "db")
        pg_port = int(os.environ.get("POSTGRES_PORT", 5432))
        pg_db = os.environ.get("POSTGRES_DB", "filter")
        pg_user = os.environ.get("POSTGRES_USER", "postgres")
        pg_password = os.environ.get("POSTGRES_PASSWORD", "postgres")
        pool = await asyncpg.create_pool(
            host=pg_host,
            port=pg_port,
            database=pg_db,
            user=pg_user,
            password=pg_password,
            min_size=1,
            max_size=10
        )
        return cls(pool)

    async def get_last_id(self, channel):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT last_id FROM channel_state WHERE channel = $1", channel
            )
            last_id = row["last_id"] if row else 0
            self.logger.info(f"[STATE][PG] Получение last_id для {channel}: {last_id}")
            return last_id

    async def set_last_id(self, channel, msg_id):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO channel_state (channel, last_id) VALUES ($1, $2)
                ON CONFLICT (channel) DO UPDATE SET last_id = EXCLUDED.last_id
                """,
                channel, msg_id
            )
            self.logger.info(f"[STATE][PG] Установка last_id для {channel}: {msg_id}")

    async def is_album_processed(self, grouped_id):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM processed_albums WHERE grouped_id = $1", grouped_id
            )
            return row is not None

    async def mark_album_processed(self, grouped_id):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO processed_albums (grouped_id) VALUES ($1) ON CONFLICT DO NOTHING",
                grouped_id
            )
            self.logger.info(f"[STATE][PG] Альбом {grouped_id} отмечен как обработанный")

    async def get_entity_by_username(self, username):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT entity_data FROM telegram_entities WHERE username = $1", username
            )
            if not row:
                self.logger.error(f"[ENTITY][PG] Entity для {username} не найден в базе!")
                return None
            entity = pickle.loads(row['entity_data'])
            self.logger.info(f"[ENTITY][PG] Entity для {username} успешно получен из базы")
            return entity

    async def get_all_usernames(self):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT username FROM telegram_entities")
            return [row['username'] for row in rows] 