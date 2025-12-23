---
layout: docs
title: Spark Configuration
permalink: /docs/spark-config.html
---

## Table properties

Tables stored as ORC files use table properties to control their behavior. By
using table properties, the table owner ensures that all clients store data
with the same options.

Key                      | Default     | Notes
:----------------------- | :---------- | :------------------------
orc.compress             | ZLIB        | high level compression = {NONE, ZLIB, SNAPPY, ZSTD}
orc.compress.size        | 262,144     | compression chunk size
orc.stripe.size          | 67,108,864  | memory buffer in bytes for writing
orc.row.index.stride     | 10,000      | number of rows between index entries
orc.create.index         | true        | whether the ORC writer create indexes as part of the file or not
orc.bloom.filter.columns | ""          | comma separated list of column names
orc.bloom.filter.fpp     | 0.05        | bloom filter false positive rate
orc.key.provider         | "hadoop"    | key provider
orc.encrypt              | ""          | list of keys and columns to encrypt with
orc.mask                 | ""          | masks to apply to the encrypted columns

For example, to create an ORC table with Zstandard compression:

```
CREATE TABLE encrypted (
  ssn STRING,
  email STRING,
  name STRING
)
USING ORC
OPTIONS (
  hadoop.security.key.provider.path "kms://http@localhost:9600/kms",
  orc.key.provider "hadoop",
  orc.encrypt "pii:ssn,email",
  orc.mask "nullify:ssn;sha256:email"
)
```

## Configuration properties

There are more Spark configuration properties related to ORC files:

Key                                  | Default  | Notes
:----------------------------------- | :------- | :------------------------
spark.sql.orc.impl                   | native   | The name of ORC implementation. It can be one of `native` or `hive`. `native` means the native ORC support. `hive` means the ORC library in Hive.
spark.sql.orc.enableVectorizedReader | true     | Enables vectorized orc decoding in `native` implementation.
spark.sql.orc.mergeSchema            | false    | When true, the ORC data source merges schemas collected from all data files, otherwise the schema is picked from a random data file.
spark.sql.hive.convertMetastoreOrc   | true     | Spark SQL will use the Hive SerDe for ORC tables instead of the built-in support.
