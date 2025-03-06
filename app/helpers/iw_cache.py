cache = {}

def initialize_cache():
    global cache

def get_value(key: str):
    return cache.get(key, None)

def set_value(key: str, value: any):
    cache[key] = value