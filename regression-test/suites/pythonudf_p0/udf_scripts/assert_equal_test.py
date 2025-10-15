def evaluate(val1, val2):
    if val1 != val2:
        raise RuntimeError("Assertion Not Met :: ! ( " + str(val1) + " == " + str(val2) + " )")
    else:
        return str(val1) + " == " + str(val2)