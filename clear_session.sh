#!/bin/bash
# Скрипт для полной очистки сессии Telegram

echo "⚠️  ВНИМАНИЕ: Это удалит текущую сессию Telegram!"
echo "После этого потребуется новая авторизация"
read -p "Продолжить? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Останавливаем контейнеры..."
    docker-compose down
    
    echo "Удаляем volume с сессией..."
    docker volume rm telegram-channels-parser_parser_session 2>/dev/null || echo "Volume не найден или уже удален"
    
    echo "Очищаем ключи в Redis..."
    docker-compose up -d redis
    sleep 3
    docker-compose exec redis redis-cli DEL tg:code:response tg:code:pending
    
    echo "✅ Сессия очищена!"
    echo ""
    echo "Теперь можно запустить парсер заново:"
    echo "docker-compose up -d"
else
    echo "Отменено"
fi

