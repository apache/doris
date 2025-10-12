def evaluate(mii):
    ans = {}
    for key, value in mii.items():
        ans[key * 10] = value * 10
    return ans