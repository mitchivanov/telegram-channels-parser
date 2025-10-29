# Инструкция по миграции на Google Sheets

## Что изменилось

- Удалена зависимость от файла `entities.csv`
- Добавлен новый сервис `channel-sync` для синхронизации с Google Sheets
- Парсер теперь получает каналы из базы данных, которая обновляется из Google Sheets

## Шаги миграции

### 1. Остановите текущие контейнеры

```bash
docker compose down
```

### 2. Очистите базу данных (как вы и хотели)

```bash
docker compose down -v  # Удалит все volumes включая базу данных
```

### 3. Настройте Google Sheets

1. Следуйте инструкции в [GOOGLE_SHEETS_SETUP.md](GOOGLE_SHEETS_SETUP.md)
2. Создайте файл `google_service_account.json` в корне проекта
3. Заполните Google Sheets ссылками на каналы

### 4. Обновите .env файл

Добавьте новые переменные:

```bash
# Google Sheets Configuration
GOOGLE_SHEETS_ID=1ZbgxGz0jvNTnB1HMmsjGukxvTGhsTXtq_Bt1f4mLcWM
GOOGLE_SHEET_NAME=Sheet1
GOOGLE_COLUMN=1
SYNC_INTERVAL_MINUTES=5
```

### 5. Пересоберите образы

```bash
docker compose build
```

### 6. Запустите систему

```bash
# Запускаем все контейнеры сразу
docker compose up -d

# При первом запуске следите за кодом авторизации
docker compose logs -f notifications-bot
```

### 7. Проверьте логи

```bash
# Проверьте синхронизацию каналов
docker compose logs -f channel-sync

# Проверьте работу парсера
docker compose logs -f parser
```

**Примечание**: Channel-sync автоматически дождется создания сессии парсером. Вам не нужно запускать сервисы в определенном порядке.

## Что происходит при первом запуске

1. `parser` запускается и требует авторизацию в Telegram (если нет сессии)
2. После авторизации парсера создается файл сессии в общем volume
3. `channel-sync` ждет появления файла сессии
4. Когда сессия создана, `channel-sync` читает каналы из Google Sheets
5. Резолвит каждую ссылку через Telegram API (используя общую сессию)
6. Сохраняет сущности в таблицу `telegram_entities`
7. Парсер читает каналы из `telegram_entities` и начинает парсинг

## Важно

- Парсер должен быть запущен и авторизован первым
- Channel-sync использует ту же Telegram сессию, что и парсер

## Возможные проблемы

### FloodWait от Telegram
- Сервис автоматически ждет указанное время
- При большом количестве каналов первая синхронизация может занять время

### Недоступные каналы
- Администраторы получат уведомление через бота
- Канал будет пропущен

### Ошибки доступа к Google Sheets
- Проверьте, что дали доступ Service Account к таблице
- Проверьте правильность GOOGLE_SHEETS_ID
