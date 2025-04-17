import logging
import asyncio
import inspect

def filter_message(text, include=None, exclude=None):
    logger = logging.getLogger("parser.utils")
    if not text:
        return False
    text = text.lower()
    if include:
        for inc in include:
            if inc in text:
                logger.info(f"[UTILS] Сообщение прошло include-фильтр: '{inc}' найдено в '{text[:30]}...'")
                break
        else:
            logger.info(f"[UTILS] Сообщение не прошло include-фильтр: '{text[:30]}...'")
            return False
    if exclude:
        for exc in exclude:
            if exc in text:
                logger.info(f"[UTILS] Сообщение не прошло exclude-фильтр: '{exc}' найдено в '{text[:30]}...'")
                return False
    logger.info(f"[UTILS] Сообщение прошло фильтрацию: '{text[:30]}...'")
    return True

async def async_backoff(
    func,
    *args,
    max_attempts=5,
    base_delay=1,
    max_delay=30,
    logger=None,
    **kwargs
):
    attempt = 0
    func_name = getattr(func, '__name__', str(func))
    
    if logger:
        logger.debug(f"[BACKOFF] Начало выполнения {func_name} (макс. попыток: {max_attempts})")
    
    last_exception = None
    while attempt < max_attempts:
        try:
            # Проверяем случай когда передан результат вызова client(...)
            # например self.client(GetHistoryRequest)
            if inspect.iscoroutine(func):
                if logger:
                    logger.debug(f"[BACKOFF] Выполнение корутины {func_name}")
                result = await func
                if logger:
                    logger.debug(f"[BACKOFF] Успешно выполнена корутина {func_name}")
                return result
            
            # Если передана функция и первый аргумент - тип запроса Telethon
            # например self.client, GetHistoryRequest, peer=...
            elif hasattr(func, '__call__') and args and inspect.isclass(args[0]):
                request_type = args[0]
                remaining_args = args[1:]
                # Создаем запрос и выполняем его
                if logger:
                    logger.debug(f"[BACKOFF] Создание запроса {request_type.__name__} и его выполнение через {func_name}")
                request = request_type(*remaining_args, **kwargs)
                result = await func(request)
                if logger:
                    logger.debug(f"[BACKOFF] Успешно выполнен запрос {request_type.__name__}")
                return result
                
            # Обычная асинхронная функция
            elif inspect.iscoroutinefunction(func):
                if logger:
                    logger.debug(f"[BACKOFF] Вызов асинхронной функции {func_name}")
                result = await func(*args, **kwargs)
                if logger:
                    logger.debug(f"[BACKOFF] Успешно выполнена асинхронная функция {func_name}")
                return result
                
            # Объект с методом __call__
            elif hasattr(func, '__call__') and not inspect.isfunction(func) and not inspect.ismethod(func):
                if logger:
                    logger.debug(f"[BACKOFF] Вызов объекта с методом __call__ {func_name}")
                result = func(*args, **kwargs)
                if inspect.iscoroutine(result):
                    if logger:
                        logger.debug(f"[BACKOFF] Получена корутина от вызова {func_name}, выполняю")
                    result = await result
                if logger:
                    logger.debug(f"[BACKOFF] Успешно выполнен вызов {func_name}")
                return result
                
            # Другие случаи
            else:
                if logger:
                    logger.debug(f"[BACKOFF] Вызов функции {func_name} (общий случай)")
                result = await func(*args, **kwargs)
                if logger:
                    logger.debug(f"[BACKOFF] Успешно выполнен вызов {func_name} (общий случай)")
                return result
                
        except Exception as e:
            last_exception = e
            if logger:
                logger.warning(f"[BACKOFF] Ошибка на попытке {attempt+1}/{max_attempts} для {func_name}: {e}")
            delay = min(base_delay * (2 ** attempt), max_delay)
            if logger:
                logger.info(f"[BACKOFF] Пауза {delay:.1f} сек перед следующей попыткой для {func_name}")
            await asyncio.sleep(delay)
            attempt += 1
    
    if logger:
        logger.error(f"[BACKOFF] Не удалось выполнить {func_name} после {max_attempts} попыток")
    
    if last_exception:
        raise last_exception
    
    raise RuntimeError(f"Backoff failed for {func_name}") 