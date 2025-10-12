def evaluate(res):
    value = ""
    for data in res:
        if data is not None:
            value += data
    result = []
    result.append(value)
    return result