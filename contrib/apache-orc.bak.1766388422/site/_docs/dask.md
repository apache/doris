---
layout: docs
title: Dask
permalink: /docs/dask.html
---

## How to install

[Dask](https://dask.org) also supports Apache ORC.

```
pip3 install "dask[dataframe]==2022.2.0"
pip3 install pandas
```

## How to write and read an ORC file

```
In [1]: import pandas as pd

In [2]: import dask.dataframe as dd

In [3]: pf = pd.DataFrame(data={"col1": [1, 2, 3]})

In [4]: dd.to_orc(dd.from_pandas(pf, npartitions=2), path="/tmp/orc")
Out[4]: (None,)

In [5]: dd.read_orc(path="/tmp/orc").compute()
Out[5]:
   col1
0     1
1     2
2     3
```
