import asyncpg

CHANNEL_STATE_TABLE = """
CREATE TABLE IF NOT EXISTS channel_state (
    channel TEXT PRIMARY KEY,
    last_id BIGINT NOT NULL
);
"""

PROCESSED_ALBUMS_TABLE = """
CREATE TABLE IF NOT EXISTS processed_albums (
    grouped_id BIGINT PRIMARY KEY
);
"""

async def init_db_schema(conn):
    await conn.execute(CHANNEL_STATE_TABLE)
    await conn.execute(PROCESSED_ALBUMS_TABLE) 