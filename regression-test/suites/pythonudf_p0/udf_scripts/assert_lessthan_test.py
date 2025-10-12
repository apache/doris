def evaluate(smaller, bigger):
    if smaller is None or bigger is None:
        raise RuntimeError("Null values found :: " + str(smaller) + " < " + str(bigger))
    if not (smaller < bigger):
        raise RuntimeError("Assertion Not Met :: ! ( " + str(smaller) + " < " + str(bigger) + " )")
    else:
        return str(smaller) + " < " + str(bigger)