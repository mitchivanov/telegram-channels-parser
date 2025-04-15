def filter_message(text, include=None, exclude=None):
    if not text:
        return False
    text = text.lower()
    if include:
        has_include = any(kw.lower() in text for kw in include)
    else:
        has_include = True
    has_exclude = any(kw.lower() in text for kw in (exclude or []))
    return has_include and not has_exclude 