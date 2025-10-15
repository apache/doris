def evaluate(hashMap):
    mul = 0
    for key, value in hashMap.items():
        mul += key * value
    return mul