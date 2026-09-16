"""Simple task registry: maps task_name -> handler function."""

TASK_REGISTRY = {}


def task(name: str):
    def decorator(fn):
        TASK_REGISTRY[name] = fn
        return fn
    return decorator
