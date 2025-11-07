-- Миграция: удаление UNIQUE constraint с username
-- Причина: username может повторяться (канал пересоздан с тем же именем)

-- Удаляем UNIQUE constraint
ALTER TABLE telegram_entities DROP CONSTRAINT IF EXISTS telegram_entities_username_key;

-- Создаем обычный index для быстрого поиска (если еще не создан)
CREATE INDEX IF NOT EXISTS idx_telegram_entities_username ON telegram_entities(username);

-- Проверка: показать текущие constraints
-- SELECT conname, contype FROM pg_constraint WHERE conrelid = 'telegram_entities'::regclass;

