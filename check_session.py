#!/usr/bin/env python3
"""Скрипт для проверки состояния сессии Telegram"""

import asyncio
import os
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from dotenv import load_dotenv

load_dotenv()

API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('TELEGRAM_PHONE')
SESSION = os.getenv('TELEGRAM_SESSION', 'session/telegram')

async def check_session():
    client = TelegramClient(SESSION, API_ID, API_HASH)
    
    try:
        print(f"Подключение к Telegram...")
        await client.connect()
        
        if await client.is_user_authorized():
            print("✅ Сессия активна и авторизована!")
            me = await client.get_me()
            print(f"Вы вошли как: {me.first_name} {me.last_name or ''} (@{me.username or 'без username'})")
        else:
            print("❌ Сессия не авторизована")
            print(f"Попытка запросить код для номера: {PHONE}")
            try:
                result = await client.send_code_request(PHONE)
                print(f"✅ Код успешно запрошен! Проверьте SMS или Telegram")
            except FloodWaitError as e:
                wait_hours = e.seconds / 3600
                print(f"❌ ОШИБКА: Telegram требует подождать {e.seconds} секунд ({wait_hours:.1f} часов)")
                print(f"Попробуйте снова после: {wait_hours:.1f} часов")
                
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(check_session())

