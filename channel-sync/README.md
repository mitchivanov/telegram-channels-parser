# Channel Sync Service

Сервис для синхронизации списка Telegram каналов из Google Sheets.

## Функциональность

- Читает список каналов из Google Sheets каждые 5 минут
- Резолвит ссылки в Telegram сущности через Telethon (используя общую сессию с парсером)
- Сохраняет сущности в PostgreSQL таблицу `telegram_entities`
- Отправляет уведомления об ошибках через Redis

## Важно

Сервис использует общую Telegram сессию с парсером:
- При старте автоматически ждет создания файла сессии
- Не требует отдельной авторизации
- Можно запускать одновременно с парсером

## Конфигурация

### Переменные окружения

```bash
# Google Sheets
GOOGLE_SHEETS_ID=1ZbgxGz0jvNTnB1HMmsjGukxvTGhsTXtq_Bt1f4mLcWM
GOOGLE_SHEET_NAME=Sheet1
GOOGLE_COLUMN=1
SYNC_INTERVAL_MINUTES=5

# Telegram API
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash

# PostgreSQL
PG_DSN=postgresql://postgres:postgres@db:5432/filter

# Redis
REDIS_URL=redis://redis:6379/0
```

### Google Service Account

Для доступа к Google Sheets нужен сервисный аккаунт:

1. Создайте проект в Google Cloud Console
2. Включите Google Sheets API
3. Создайте Service Account
4. Скачайте JSON ключ
5. Дайте доступ сервисному аккаунту к вашей таблице

Есть два способа передать credentials:

1. **Через файл** (рекомендуется):
   - Положите файл `google_service_account.json` в корень проекта
   - Он будет смонтирован в контейнер через docker-compose

2. **Через переменную окружения**:
   - Передайте JSON как строку в переменной `GOOGLE_SERVICE_ACCOUNT_KEY`

## Формат Google Sheets

В указанном столбце должны быть ссылки на Telegram каналы:
- `https://t.me/channelname`
- `https://t.me/+invitelink`
- `@channelname`
- `channelname`

## Уведомления об ошибках

При ошибках сервис отправляет уведомления через Redis в очередь `admin_notifications`.
Бот уведомлений (`notifications-bot`) читает эти сообщения и отправляет администраторам.
