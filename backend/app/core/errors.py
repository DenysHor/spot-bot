def describe_exception(exc: BaseException) -> str:
    message = str(exc).strip() or repr(exc)
    return f"{type(exc).__name__}: {message}"
