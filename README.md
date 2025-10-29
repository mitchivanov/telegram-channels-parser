# Telegram Channels Parser

Система для парсинга и модерации контента из Telegram каналов с автоматической синхронизацией списка каналов из Google Sheets.

## Основные возможности

- 📊 **Автоматическая синхронизация каналов из Google Sheets**
- 🔄 Парсинг сообщений из Telegram каналов в реальном времени
- 🛡️ Модерация контента с автоматическим удалением
- 🤖 Боты для управления и уведомлений
- 📱 Веб-интерфейс для настройки фильтров
- 🔔 Уведомления администраторам об ошибках

## Архитектура

### Сервисы

1. **parser** - Парсинг сообщений из Telegram (должен быть запущен первым для авторизации)
2. **channel-sync** - Синхронизация каналов из Google Sheets (использует сессию парсера)
3. **filter-backend** - API для управления фильтрами
4. **filter-frontend** - Веб-интерфейс
5. **filter-worker** - Обработка сообщений и модерация
6. **channels-bot** - Бот для публикации отфильтрованного контента
7. **filter-bot** - Бот для управления фильтрами
8. **notifications-bot** - Бот для уведомлений администраторам

## Быстрый старт

### 1. Настройка Google Sheets

См. [GOOGLE_SHEETS_SETUP.md](GOOGLE_SHEETS_SETUP.md) для подробной инструкции.

### 2. Конфигурация

Создайте файл `.env` на основе переменных:

```bash
# Telegram API
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash

# Google Sheets
GOOGLE_SHEETS_ID=your_sheet_id
GOOGLE_SHEET_NAME=Sheet1
GOOGLE_COLUMN=1
SYNC_INTERVAL_MINUTES=5

# Bot tokens
TELEGRAM_BOT_TOKEN=your_bot_token
CODE_BOT_TOKEN=your_notification_bot_token

# Channel IDs для публикации
CHANNEL1_ID=-1001234567890
CHANNEL2_ID=-1001234567891
CHANNEL3_ID=-1001234567892

# Admin IDs для уведомлений
ADMIN_IDS=123456789,987654321
```

### 3. Запуск

```bash
# Запуск всех сервисов одной командой
docker compose up -d

# При первом запуске следите за запросом кода авторизации
docker compose logs -f notifications-bot

# Просмотр логов
docker compose logs -f

# Остановка
docker compose down
```

## Настройка каналов в Google Sheets

В указанном столбце таблицы добавьте ссылки на каналы:
- `https://t.me/channelname`
- `https://t.me/+invitelink`
- `@channelname`
- `channelname`

Сервис автоматически синхронизирует каналы каждые 5 минут.

## Веб-интерфейс

После запуска доступен по адресу: http://localhost:8085

## Мониторинг

- Логи: `docker compose logs -f service_name`
- Состояние: `docker compose ps`

## Разработка

### Структура проекта

```
.
├── channel-sync/       # Синхронизация с Google Sheets
├── parser/            # Парсер Telegram каналов
├── filter/            # Backend API + Worker
│   └── frontend/      # React приложение
├── channels-bot/      # Бот публикации
├── filter-bot/        # Бот управления
└── notifications-bot/ # Бот уведомлений
```

## Лицензия

MIT