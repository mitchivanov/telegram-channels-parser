# Настройка Google Sheets интеграции

## 1. Создание Google Service Account

1. Перейдите в [Google Cloud Console](https://console.cloud.google.com/)
2. Создайте новый проект или выберите существующий
3. Включите Google Sheets API:
   - Перейдите в "APIs & Services" → "Library"
   - Найдите "Google Sheets API"
   - Нажмите "Enable"

4. Создайте Service Account:
   - Перейдите в "APIs & Services" → "Credentials"
   - Нажмите "Create Credentials" → "Service Account"
   - Заполните имя и описание
   - Нажмите "Create and Continue"
   - Пропустите шаги с ролями
   - Нажмите "Done"

5. Создайте ключ для Service Account:
   - Найдите созданный Service Account в списке
   - Нажмите на email сервисного аккаунта
   - Перейдите во вкладку "Keys"
   - Нажмите "Add Key" → "Create new key"
   - Выберите JSON формат
   - Сохраните файл как `google_service_account.json`

## 2. Настройка Google Sheets

1. Создайте или откройте существующую таблицу в Google Sheets
2. Скопируйте ID таблицы из URL:
   - URL выглядит так: `https://docs.google.com/spreadsheets/d/1ZbgxGz0jvNTnB1HMmsjGukxvTGhsTXtq_Bt1f4mLcWM/edit`
   - ID: `1ZbgxGz0jvNTnB1HMmsjGukxvTGhsTXtq_Bt1f4mLcWM`

3. Дайте доступ Service Account к таблице:
   - Нажмите кнопку "Share" в правом верхнем углу
   - Введите email вашего Service Account (найдете в JSON файле в поле "client_email")
   - Выберите права "Viewer"
   - Нажмите "Send"

## 3. Формат данных в таблице

В первом столбце (или указанном в `GOOGLE_COLUMN`) должны быть ссылки на каналы:

```
https://t.me/channelname
https://t.me/+invitelink
@channelname
channelname
```

## 4. Конфигурация проекта

1. Положите файл `google_service_account.json` в корень проекта
2. Добавьте в `.env`:

```bash
GOOGLE_SHEETS_ID=ваш_id_таблицы
GOOGLE_SHEET_NAME=Sheet1
GOOGLE_COLUMN=1
SYNC_INTERVAL_MINUTES=5
```

## 5. Запуск

```bash
# Просто запустите все контейнеры сразу
docker compose up -d

# Channel-sync автоматически дождется авторизации парсера
# Следите за запросом кода авторизации:
docker compose logs -f notifications-bot

# После ввода кода проверьте статус синхронизации:
docker compose logs -f channel-sync
```

**Что происходит при запуске:**
- Парсер запускается и ждет код авторизации (если нет сессии)
- Channel-sync запускается и ждет появления файла сессии
- После авторизации парсера, channel-sync автоматически начнет работу
- Синхронизация будет происходить каждые 5 минут