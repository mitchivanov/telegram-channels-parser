import json
import os
import logging
import asyncio

class StateManager:
    def __init__(self, state_file=None):
        # Используем переменную окружения STATE_FILE или значение по умолчанию
        self.state_file = state_file or os.environ.get('STATE_FILE', '/app/data/state.json')
        self.lock = asyncio.Lock()
        self.state = self._load()
        self.logger = logging.getLogger("parser.state")
        if 'processed_albums' not in self.state:
            self.state['processed_albums'] = []
        
        # Проверяем существование директории
        state_dir = os.path.dirname(self.state_file)
        if state_dir and not os.path.exists(state_dir):
            try:
                os.makedirs(state_dir)
                self.logger.info(f"Создана директория для state_file: {state_dir}")
            except Exception as e:
                self.logger.error(f"Ошибка при создании директории {state_dir}: {e}")
        
        self.logger.info(f"Инициализация StateManager. Путь к файлу состояния: {self.state_file}")

    def _load(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.error(f"Ошибка загрузки state файла: {e}")
                return {'last_ids': {}, 'activity': {}}
        return {'last_ids': {}, 'activity': {}}

    async def save(self):
        self.logger.debug("[LOCK] Ожидание lock для save()")
        async with self.lock:
            self.logger.debug("[LOCK] Захвачен lock для save()")
            try:
                # Используем асинхронную запись через asyncio.to_thread
                await asyncio.to_thread(self._save_sync)
                self.logger.debug(f"Состояние успешно сохранено в {self.state_file}")
            except Exception as e:
                self.logger.error(f"Ошибка при сохранении состояния: {e}")
            self.logger.debug("[LOCK] Освобождаю lock для save()")
    
    def _save_sync(self):
        # Создаем временный файл и атомарно перемещаем его
        temp_file = f"{self.state_file}.tmp"
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f)
            # Атомарное переименование
            os.replace(temp_file, self.state_file)
        except Exception as e:
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass
            raise e

    def get_last_id(self, channel):
        last_id = self.state['last_ids'].get(channel, 0)
        self.logger.info(f"[STATE] Получение last_id для {channel}: {last_id}")
        return last_id

    async def set_last_id(self, channel, msg_id):
        self.logger.debug(f"[LOCK] Ожидание lock для set_last_id({channel})")
        async with self.lock:
            self.logger.debug(f"[LOCK] Захвачен lock для set_last_id({channel})")
            self.logger.info(f"[STATE] Установка last_id для {channel}: {msg_id}")
            self.state['last_ids'][channel] = msg_id
            await self.save()
            self.logger.debug(f"[LOCK] Освобождаю lock для set_last_id({channel})")

    def get_activity(self, channel):
        return self.state['activity'].get(channel)

    async def set_activity(self, channel, data):
        self.logger.debug(f"[LOCK] Ожидание lock для set_activity({channel})")
        async with self.lock:
            self.logger.debug(f"[LOCK] Захвачен lock для set_activity({channel})")
            self.state['activity'][channel] = data
            await self.save()
            self.logger.debug(f"[LOCK] Освобождаю lock для set_activity({channel})")

    def is_album_processed(self, grouped_id):
        return grouped_id in self.state['processed_albums']

    async def mark_album_processed(self, grouped_id):
        self.logger.debug(f"[LOCK] Ожидание lock для mark_album_processed({grouped_id})")
        async with self.lock:
            self.logger.debug(f"[LOCK] Захвачен lock для mark_album_processed({grouped_id})")
            if grouped_id not in self.state['processed_albums']:
                self.state['processed_albums'].append(grouped_id)
                await self.save()
            self.logger.debug(f"[LOCK] Освобождаю lock для mark_album_processed({grouped_id})") 