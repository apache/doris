# HLL_UNION_AGG

## Syntax

`HLL_UNION_AGG(hll)`

## Description

HLL是基于HyperLogLog算法的工程实现，用于保存HyperLogLog计算过程的中间结果

它只能作为表的value列类型、通过聚合来不断的减少数据量，以此来实现加快查询的目的

基于它得到的是一个估算结果，误差大概在1%左右，hll列是通过其它列或者导入数据里面的数据生成的

导入的时候通过hll_hash函数来指定数据中哪一列用于生成hll列，它常用于替代count distinct，通过结合rollup在业务上用于快速计算uv等

## Examples
```
MySQL > select HLL_UNION_AGG(uv_set) from test_uv;;
+-------------------------+
| HLL_UNION_AGG(`uv_set`) |
+-------------------------+
| 17721                   |
+-------------------------+
```
