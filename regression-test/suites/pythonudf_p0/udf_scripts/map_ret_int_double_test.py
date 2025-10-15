def evaluate(mid):
    ans = {}
    for key, value in mid.items():
        ans[key * 10] = value * 10
    return ans