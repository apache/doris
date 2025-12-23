---
layout: docs
title: Hive DDL
permalink: /docs/hive-ddl.html
---

ORC is well integrated into Hive, so storing your istari table as ORC
is done by adding "STORED AS ORC".

```
CREATE TABLE istari (
  name STRING,
  color STRING
) STORED AS ORC;
```

To modify a table so that new partitions of the istari table are
stored as ORC files:

```
ALTER TABLE istari SET FILEFORMAT ORC;
```

As of Hive 0.14, users can request an efficient merge of small ORC files
together by issuing a CONCATENATE command on their table or partition. The
files will be merged at the stripe level without reserialization.

```
ALTER TABLE istari [PARTITION partition_spec] CONCATENATE;
```

To get information about an ORC file, use the orcfiledump command.

```
% hive --orcfiledump <path_to_file>
```

As of Hive 1.1, to display the data in the ORC file, use:

```
% hive --orcfiledump -d <path_to_file>
```