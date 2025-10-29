# 📊 Настройка интеграции с Google Sheets

Эта инструкция поможет вам настроить автоматическую синхронизацию списка Telegram-каналов из Google Sheets в PostgreSQL.

## 🎯 Что это дает?

- ✅ Управление списком каналов через Google Sheets (удобно для команды)
- ✅ Автоматическая синхронизация каждые 5 минут
- ✅ Парсер автоматически подхватывает новые каналы без перезапуска
- ✅ История изменений в Google Sheets

## 📋 Шаг 1: Создание проекта в Google Cloud

1. **Перейдите в Google Cloud Console:**
   https://console.cloud.google.com/

2. **Создайте новый проект:**
   - Нажмите на выпадающий список проектов (вверху)
   - Нажмите "New Project" / "Создать проект"
   - Введите название: `telegram-parser-sync` (или любое другое)
   - Нажмите "Create" / "Создать"

3. **Активируйте Google Sheets API:**
   - Перейдите в "APIs & Services" → "Library"
   - Найдите "Google Sheets API"
   - Нажмите "Enable" / "Включить"

4. **Активируйте Google Drive API:**
   - Там же найдите "Google Drive API"
   - Нажмите "Enable" / "Включить"

## 🔑 Шаг 2: Создание Service Account

1. **Перейдите в "APIs & Services" → "Credentials"**

2. **Создайте Service Account:**
   - Нажмите "+ CREATE CREDENTIALS" → "Service account"
   - Введите имя: `sheets-sync-bot`
   - Описание (опционально): `Bot for syncing Telegram channels from Google Sheets`
   - Нажмите "Create and Continue" / "Создать и продолжить"

3. **Роль (опционально):**
   - Можете пропустить этот шаг
   - Нажмите "Continue" / "Продолжить"

4. **Права доступа (опционально):**
   - Можете пропустить
   - Нажмите "Done" / "Готово"

## 📄 Шаг 3: Создание JSON ключа

1. **Найдите созданный Service Account:**
   - В списке "Service Accounts" найдите `sheets-sync-bot@...`
   - Нажмите на email

2. **Создайте ключ:**
   - Перейдите на вкладку "Keys" / "Ключи"
   - Нажмите "Add Key" → "Create new key"
   - Выберите формат: **JSON**
   - Нажмите "Create" / "Создать"

3. **Сохраните файл:**
   - Файл автоматически скачается (например, `telegram-parser-sync-abc123.json`)
   - **Переименуйте его в `credentials.json`**
   - **Поместите в корень проекта** (рядом с `docker-compose.yml`)

⚠️ **ВАЖНО:** Не загружайте этот файл в Git! Он уже добавлен в `.gitignore`

## 📊 Шаг 4: Создание Google Sheets таблицы

1. **Создайте новую таблицу:**
   - Перейдите на https://sheets.google.com/
   - Нажмите "+ Создать" → "Google Таблицы"

2. **Настройте таблицу:**
   - Назовите таблицу: `Telegram Channels` (или любое имя)
   - В **первую колонку (A)** добавьте ссылки на каналы (по одной на строку)

3. **Примеры форматов ссылок:**
   ```
   https://t.me/channelname
   https://t.me/+abc123def456
   @channelname
   channelname
   -1001234567890
   ```

4. **Комментарии (опционально):**
   - Строки, начинающиеся с `#` будут игнорироваться
   ```
   # Это комментарий - будет проигнорирован
   https://t.me/validchannel
   ```

## 🔐 Шаг 5: Предоставление доступа

1. **Скопируйте email Service Account:**
   - Откройте файл `credentials.json`
   - Найдите поле `client_email`
   - Скопируйте email (например: `sheets-sync-bot@telegram-parser-sync.iam.gserviceaccount.com`)

2. **Дайте доступ к таблице:**
   - Откройте вашу Google Sheets таблицу
   - Нажмите "Поделиться" (Share) справа вверху
   - Вставьте скопированный email
   - Выберите роль: **Просмотр** (Viewer) - достаточно для чтения
   - **Снимите галочку "Уведомить пользователей"** (не нужно слать письмо боту)
   - Нажмите "Поделиться" / "Share"

## 🆔 Шаг 6: Получение ID таблицы

1. **Откройте вашу Google Sheets таблицу**

2. **Скопируйте ID из URL:**
   ```
   https://docs.google.com/spreadsheets/d/1A2B3C4D5E6F7G8H9I0J1K2L3M4N5O6P7/edit
                                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                                          Это и есть GOOGLE_SHEETS_ID
   ```

3. **Сохраните этот ID** - он понадобится в следующем шаге

## ⚙️ Шаг 7: Настройка переменных окружения

1. **Откройте файл `.env`** в корне проекта

2. **Добавьте следующие переменные:**
   ```bash
   # Google Sheets Configuration
   GOOGLE_SHEETS_ID=1A2B3C4D5E6F7G8H9I0J1K2L3M4N5O6P7
   GOOGLE_SHEET_NAME=Sheet1
   GOOGLE_COLUMN=1
   SYNC_INTERVAL_MINUTES=5
   ```

3. **Описание переменных:**
   - `GOOGLE_SHEETS_ID` - ID таблицы из URL (см. Шаг 6)
   - `GOOGLE_SHEET_NAME` - Название листа (по умолчанию: `Sheet1`)
   - `GOOGLE_COLUMN` - Номер колонки с каналами (1 = колонка A)
   - `SYNC_INTERVAL_MINUTES` - Интервал синхронизации в минутах (по умолчанию: 5)

## 🚀 Шаг 8: Запуск

1. **Убедитесь, что файл `credentials.json` находится в корне проекта:**
   ```bash
   ls -la credentials.json
   ```

2. **Пересоберите Docker контейнеры:**
   ```bash
   docker-compose build google-sheets-sync parser
   ```

3. **Перезапустите сервисы:**
   ```bash
   docker-compose up -d google-sheets-sync parser
   ```

4. **Проверьте логи синхронизации:**
   ```bash
   docker-compose logs -f google-sheets-sync
   ```

   Вы должны увидеть что-то вроде:
   ```
   [SYNC-SCHEDULER] Starting Google Sheets sync scheduler...
   [SYNC-SCHEDULER] Sync interval: 5 minutes
   [INFO] Connecting to Google Sheets: 1A2B3C4D5E6F7G8H9I0J1K2L3M4N5O6P7
   [INFO] Read 191 rows from Google Sheets
   [INFO] Extracted 191 valid channels from Google Sheets
   [INFO] ✓ channelname → 1234567890
   ```

5. **Проверьте логи парсера:**
   ```bash
   docker-compose logs -f parser
   ```

   Должны увидеть:
   ```
   [CHANNELS] Запуск периодической перезагрузки каналов каждые 5 минут
   [CHANNELS] Список каналов обновлен: 150 → 191
   ```

## 🔍 Проверка работы

### Тест 1: Добавление нового канала

1. Откройте Google Sheets таблицу
2. Добавьте новую ссылку на канал в конец списка
3. Подождите до 5 минут
4. Проверьте логи:
   ```bash
   docker-compose logs google-sheets-sync | grep "✓"
   ```

### Тест 2: Проверка базы данных

```bash
docker-compose exec db psql -U postgres -d filter -c "SELECT COUNT(*) FROM telegram_entities;"
```

Должно показать количество синхронизированных каналов.

## 📊 Структура Google Sheets таблицы

### Минимальная структура (одна колонка):
```
| A                              |
|--------------------------------|
| https://t.me/channel1          |
| https://t.me/channel2          |
| @channel3                      |
```

### Расширенная структура (с метаданными):
```
| A                      | B                | C           |
|------------------------|------------------|-------------|
| Channel URL            | Category         | Status      |
| https://t.me/channel1  | Cashback         | Active      |
| https://t.me/channel2  | Sales            | Active      |
| @channel3              | Deals            | Paused      |
```

⚠️ **Скрипт читает ТОЛЬКО колонку A** (или ту, что указана в `GOOGLE_COLUMN`)

## 🛠 Устранение неполадок

### Ошибка: "gspread not available"
```bash
# Пересоберите контейнер
docker-compose build google-sheets-sync
```

### Ошибка: "Google credentials file not found"
```bash
# Убедитесь, что файл существует
ls -la credentials.json

# Проверьте права доступа
chmod 644 credentials.json
```

### Ошибка: "Permission denied" при доступе к Google Sheets
1. Убедитесь, что вы дали доступ Service Account к таблице (Шаг 5)
2. Проверьте email в `credentials.json` (поле `client_email`)
3. Убедитесь, что роль - минимум "Viewer"

### Каналы не подхватываются парсером
1. Проверьте, что синхронизация работает:
   ```bash
   docker-compose logs google-sheets-sync
   ```
2. Проверьте, что парсер перезагружает каналы:
   ```bash
   docker-compose logs parser | grep CHANNELS
   ```
3. Проверьте базу данных:
   ```bash
   docker-compose exec db psql -U postgres -d filter -c "SELECT username FROM telegram_entities LIMIT 10;"
   ```

### Парсер не видит новые каналы
- Подождите до 5 минут (интервал перезагрузки)
- Или перезапустите парсер вручную:
  ```bash
  docker-compose restart parser
  ```

## 📝 Переменные окружения (справка)

| Переменная                 | Описание                           | По умолчанию | Обязательна |
|---------------------------|------------------------------------|--------------|-------------|
| `GOOGLE_SHEETS_ID`        | ID Google Sheets таблицы          | -            | ✅ Да       |
| `GOOGLE_CREDENTIALS_FILE` | Путь к JSON ключу                 | credentials.json | Нет   |
| `GOOGLE_SHEET_NAME`       | Название листа                    | Sheet1       | Нет         |
| `GOOGLE_COLUMN`           | Номер колонки (1 = A, 2 = B, ...) | 1           | Нет         |
| `SYNC_INTERVAL_MINUTES`   | Интервал синхронизации (минуты)   | 5            | Нет         |

## 🎓 Дополнительно

### Как изменить интервал синхронизации?

В `.env` файле измените:
```bash
SYNC_INTERVAL_MINUTES=10  # Синхронизация каждые 10 минут
```

И перезапустите:
```bash
docker-compose restart google-sheets-sync
```

### Как использовать несколько листов?

Измените `GOOGLE_SHEET_NAME` в `.env`:
```bash
GOOGLE_SHEET_NAME=ActiveChannels
```

### Как читать из другой колонки?

Измените `GOOGLE_COLUMN` в `.env`:
```bash
GOOGLE_COLUMN=2  # Читать из колонки B
```

### Как запустить синхронизацию вручную?

```bash
docker-compose exec google-sheets-sync python sync_google_sheets.py
```

## 📚 Полезные ссылки

- [Google Cloud Console](https://console.cloud.google.com/)
- [Google Sheets API Documentation](https://developers.google.com/sheets/api)
- [gspread Documentation](https://docs.gspread.org/)

## ✅ Готово!

Теперь ваш парсер автоматически синхронизирует список каналов из Google Sheets каждые 5 минут! 🎉

Любые изменения в таблице будут автоматически применены без необходимости перезапуска парсера.

