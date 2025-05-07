import re
from typing import List, Optional

def extract_cashback_percent(text: str) -> List[float]:
    """
    Извлекает все проценты кэшбэка из текста. Если встречается 'бесплатно' — добавляет 100.
    Если есть цены и кэшбек в рублях — вычисляет процент. Если только цены — тоже вычисляет.
    Возвращает список всех найденных процентов (float).
    """
    if not text:
        return []
    text = text.lower().replace(',', '.')
    result = set()
    # 1. Бесплатно
    free_patterns = [r'бесплатно', r'free', r'0 ?руб', r'0 ?₽', r'0 ?р', r'0р', r'0₽']
    if any(re.search(p, text) for p in free_patterns):
        result.add(100.0)
    # 2. Явные проценты
    percent_patterns = [
        r'(\d{1,3})\s*%\s*(?:кэшбек|кешбек|кэшбэк|кешбэк|cashback)?',
        r'(кэшбек|кешбек|кэшбэк|кешбэк|cashback)[^\d]{0,10}(\d{1,3})\s*%',
        r'(\d{1,3})\s*процент(?:а|ов)?',
    ]
    for pat in percent_patterns:
        for m in re.findall(pat, text):
            if isinstance(m, tuple):
                for v in m:
                    if v.isdigit():
                        val = float(v)
                        if 0 < val <= 100:
                            result.add(val)
            elif m.isdigit():
                val = float(m)
                if 0 < val <= 100:
                    result.add(val)
    # 3. Кэшбек в рублях + цены
    # Ищем "кэшбек XXX руб", "кэшбек XXX₽"
    cashback_rub = None
    cashback_rub_match = re.search(r'кэшбек[^\d]{0,10}(\d{2,6})\s*(?:руб|₽|р)\b', text)
    if cashback_rub_match:
        cashback_rub = float(cashback_rub_match.group(1))
    # Ищем "цена на сайте" и "цена для вас"
    price_site = None
    price_you = None
    price_site_match = re.search(r'цена на [^\d\n]{0,20}(\d{2,6}(?:\.\d{1,2})?)', text)
    if price_site_match:
        price_site = float(price_site_match.group(1))
    price_you_match = re.search(r'цена для вас[^\d\n]{0,20}(\d{2,6}(?:\.\d{1,2})?)', text)
    if price_you_match:
        price_you = float(price_you_match.group(1))
    if cashback_rub and price_site:
        percent = round(100 * cashback_rub / price_site, 2)
        if 0 < percent <= 100:
            result.add(percent)
    # 4. Просто цены (если нет кэшбэка в рублях)
    if not result and price_site and price_you:
        percent = round(100 * (price_site - price_you) / price_site, 2)
        if 0 < percent <= 100:
            result.add(percent)
    return sorted(result, reverse=True) 