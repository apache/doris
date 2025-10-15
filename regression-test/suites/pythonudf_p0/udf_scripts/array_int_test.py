def evaluate(res):
    value = 0
    for data in res:
        if data is not None:
            value += data
    return value