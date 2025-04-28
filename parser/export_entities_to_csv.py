import csv
import pickle
import base64
from datetime import datetime
import time
from telethon.sync import TelegramClient
from telethon.errors import FloodWaitError

API_ID = 29089923  # Ваш API_ID
API_HASH = '0b51a4cae4a19740a2c41a9344438ade'  # Ваш API_HASH
SESSION = 'entity_export.session'

def extract_username(url):
    if url.startswith('https://t.me/'):
        part = url[len('https://t.me/'):]
        if part.startswith('+'):
            return url  # invite link, используем как есть
        return part.split('/')[0]
    return url

def main():
    with open('parser/updated_sources.txt', encoding='utf-8') as f:
        channels = [line.strip() for line in f if line.strip()]

    with TelegramClient(SESSION, API_ID, API_HASH) as client, \
         open('entities.csv', 'w', newline='', encoding='utf-8') as csvfile, \
         open('entities_errors.log', 'w', encoding='utf-8') as errfile:

        writer = csv.writer(csvfile)
        writer.writerow(['username', 'id', 'entity_data', 'updated_at'])

        for url in channels:
            username = extract_username(url)
            try:
                entity = client.get_entity(username)
                entity_bytes = pickle.dumps(entity)
                entity_b64 = base64.b64encode(entity_bytes).decode('ascii')
                entity_id = getattr(entity, 'id', None)
                updated_at = datetime.utcnow().isoformat()
                writer.writerow([username, entity_id, entity_b64, updated_at])
                print(f"[OK] {username} -> {entity_id}")
                time.sleep(1.5)  # Пауза для избежания floodwait
            except FloodWaitError as e:
                print(f"[FLOODWAIT] {username}: FloodWait {e.seconds}s, sleeping...")
                time.sleep(e.seconds + 1)
                continue
            except Exception as e:
                errfile.write(f"{username}: {e}\n")
                print(f"[ERR] {username}: {e}")

if __name__ == '__main__':
    main()