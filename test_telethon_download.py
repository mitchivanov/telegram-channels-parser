import os
import sys
import asyncio
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from dotenv import load_dotenv

load_dotenv()


TELEGRAM_API_ID = int(os.environ.get('TELEGRAM_API_ID'))
TELEGRAM_API_HASH = os.environ.get('TELEGRAM_API_HASH')
TELEGRAM_SESSION = os.environ.get('TELEGRAM_SESSION', 'test.session')
TEMP_DIR = '/app/temp'

async def main():
    if len(sys.argv) < 2:
        print("Usage: python test_telethon_download.py <channel>")
        sys.exit(1)
    channel = sys.argv[1]
    client = TelegramClient(TELEGRAM_SESSION, TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await client.start()
    entity = await client.get_entity(channel)
    # Получаем 20 последних сообщений
    history = await client(GetHistoryRequest(
        peer=entity,
        limit=20,
        offset_id=0,
        offset_date=None,
        add_offset=0,
        max_id=0,
        min_id=0,
        hash=0
    ))
    msgs = history.messages
    msg = next((m for m in msgs if m.media and (isinstance(m.media, MessageMediaPhoto) or isinstance(m.media, MessageMediaDocument))), None)
    if not msg:
        print(f"Нет сообщений с медиа в последних 20 сообщениях канала {channel}")
        await client.disconnect()
        return
    if isinstance(msg.media, MessageMediaPhoto):
        file_path = os.path.join(TEMP_DIR, f"{msg.id}_photo")
    elif isinstance(msg.media, MessageMediaDocument):
        file_path = os.path.join(TEMP_DIR, f"{msg.id}_document")
    else:
        print(f"Тип медиа не поддерживается: {type(msg.media)}")
        await client.disconnect()
        return
    print(f"Скачиваю медиа из сообщения {msg.id} в {file_path} ...")
    await client.download_media(msg, file=file_path)
    if os.path.exists(file_path):
        print(f"SUCCESS: File downloaded: {file_path}, size: {os.path.getsize(file_path)} bytes")
    else:
        print(f"FAIL: File not found after download: {file_path}")
        print(f"Dir contents: {os.listdir(TEMP_DIR)}")
        print(f"Dir perms: {oct(os.stat(TEMP_DIR).st_mode)}")
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main()) 