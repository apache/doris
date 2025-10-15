def evaluate(arg1, a, b):
    return arg1[:a] + "*" * (len(arg1) - a - b) + arg1[-b:]