---
layout: docs
title: Spark DDL
permalink: /docs/spark-ddl.html
---

ORC is well integrated into Spark, so storing your `istari` table as ORC
is done by adding `USING ORC`.

```
CREATE TABLE istari (
  name STRING,
  color STRING
) USING ORC;
```

To get information about an ORC file, use the `orc-tools` command.

```
% orc-tools meta <path_to_file>
```

To display the data in the ORC file, use:

```
% orc-tools data <path_to_file>
```
