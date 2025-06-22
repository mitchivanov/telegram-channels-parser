#!/bin/bash

# Log Rotator - создает почасовые логи с временными метками
# Сэр, этот скрипт обеспечит вам полный контроль над логированием

LOG_DIR="/app/logs"
DOCKER_LOGS_DIR="/var/lib/docker/containers"

# Создаем директорию для логов если её нет
mkdir -p "$LOG_DIR"

# Получаем список всех контейнеров и их ID
get_container_mapping() {
    echo "Mapping containers to their IDs..."
    
    # Создаем mapping файл
    cat > /tmp/container_mapping.json << 'EOF'
{
  "kafka": "",
  "redis": "", 
  "parser": "",
  "notifications-bot": "",
  "filter-backend": "",
  "filter-worker": "",
  "filter-frontend": "",
  "channels-bot": "",
  "channels-cleanup-worker": "",
  "db": "",
  "kafdrop": "",
  "filter-bot": ""
}
EOF
}

# Функция для получения ID контейнера по имени
get_container_id() {
    local container_name="$1"
    # Ищем в файлах конфигурации Docker
    for config_file in $(find "$DOCKER_LOGS_DIR" -name "config.v2.json" 2>/dev/null); do
        container_id=$(dirname "$config_file" | xargs basename)
        name=$(jq -r '.Name // empty' "$config_file" 2>/dev/null | sed 's/^\///')
        if [ "$name" = "$container_name" ]; then
            echo "$container_id"
            return
        fi
    done
}

# Функция для копирования и ротации логов
rotate_logs() {
    local service_name="$1"
    local container_id="$2"
    local timestamp=$(date +"%Y%m%d_%H0000")
    
    if [ -z "$container_id" ]; then
        echo "Warning: Container ID not found for $service_name"
        return
    fi
    
    local source_log="$DOCKER_LOGS_DIR/$container_id/${container_id}-json.log"
    local dest_log="$LOG_DIR/${service_name}_${timestamp}.log"
    
    if [ -f "$source_log" ]; then
        # Копируем текущий лог с временной меткой
        cp "$source_log" "$dest_log"
        echo "$(date): Rotated log for $service_name -> $dest_log"
        
        # Очищаем исходный файл (Docker пересоздаст его)
        echo "" > "$source_log"
    else
        echo "Warning: Log file not found for $service_name at $source_log"
    fi
}

# Основная функция ротации
main_rotation() {
    echo "=== Log Rotation Started at $(date) ==="
    
    # Список сервисов для ротации
    services=(
        "kafka"
        "redis" 
        "parser"
        "notifications-bot"
        "filter-backend"
        "filter-worker"
        "filter-frontend"
        "channels-bot"
        "channels-cleanup-worker"
        "db"
        "kafdrop"
        "filter-bot"
    )
    
    for service in "${services[@]}"; do
        container_id=$(get_container_id "$service")
        if [ ! -z "$container_id" ]; then
            rotate_logs "$service" "$container_id"
        fi
    done
    
    # Удаляем логи старше 7 дней
    find "$LOG_DIR" -name "*.log" -mtime +7 -delete
    
    echo "=== Log Rotation Completed at $(date) ==="
}

# Функция для мониторинга в реальном времени
monitor_logs() {
    echo "Starting log monitoring and hourly rotation..."
    
    while true; do
        # Проверяем, началась ли новый час
        current_minute=$(date +"%M")
        
        if [ "$current_minute" = "00" ]; then
            main_rotation
            # Ждем 61 секунду чтобы не запустить ротацию дважды в один час
            sleep 61
        fi
        
        # Проверяем каждую минуту
        sleep 60
    done
}

# Инициализация
echo "Starting Telegram Channels Parser Log Rotator"
echo "Log directory: $LOG_DIR"
echo "Docker logs directory: $DOCKER_LOGS_DIR"

# Создаем начальную структуру
get_container_mapping

# Делаем первоначальную ротацию
main_rotation

# Запускаем мониторинг
monitor_logs 