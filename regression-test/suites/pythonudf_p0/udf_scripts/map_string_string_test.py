def evaluate(hashMap):
    sb = []
    sortSet = set()

    for key, value in hashMap.items():
        sortSet.add(key + value)

    for item in sorted(sortSet):
        sb.append(item)

    ans = ''.join(sb)
    return ans