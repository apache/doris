# PERCENTILE_APPROX

## Syntax

PERCENTILE_APPROX(DOUBLE col, p [, B]) 

## Description
求近似第p个百分位数，p的值介于0到1之间，参数B控制近似的精确值，B值越大，近似度越高，默认值为10000，
当列中非重复的数量小于B时，返回精确的百分数。