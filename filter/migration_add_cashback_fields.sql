-- Миграция для добавления полей процентов кэшбека в таблицу filters
-- Запустите этот SQL скрипт вручную, если у вас уже есть существующая база данных

ALTER TABLE filters ADD COLUMN IF NOT EXISTS min_cashback_percent FLOAT;
ALTER TABLE filters ADD COLUMN IF NOT EXISTS max_cashback_percent FLOAT;

-- Для проверки, что поля добавлены
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'filters' 
  AND column_name IN ('min_cashback_percent', 'max_cashback_percent');
