---
layout: docs
title: ACID support
permalink: /docs/acid.html
---

Historically, the only way to atomically add data to a table in Hive
was to add a new partition. Updating or deleting data in partition
required removing the old partition and adding it back with the new
data and it wasn't possible to do atomically.

However, user's data is continually changing and as Hive matured,
users required reliability guarantees despite the churning data
lake. Thus, we needed to implement ACID transactions that guarantee
atomicity, consistency, isolation, and durability. Although we support
ACID transactions, they are not designed to support OLTP requirements.
It can support millions of rows updated per a transaction, but it can
not support millions of transactions an hour.

Additionally, we wanted to support streaming ingest in to Hive tables where
streaming applications like Flume or Storm could write data into Hive and
have transactions commit once a minute and queries would either see all of
a transaction or none of it.

HDFS is a write once file system and ORC is a write-once file format, so edits
were implemented using base files and delta files where insert, update, and
delete operations are recorded.

Hive tables without ACID enabled have each partition in HDFS look like:

Filename | Contents
:------- | :--------
00000_0  | Bucket 0
00001_0  | Bucket 1

With ACID enabled, the system will add delta directories:

Filename | Contents
:------- | :--------
00000_0  | Bucket 0 base
00001_0  | Bucket 1 base
delta_0000005_0000005/bucket_00000 | Transaction 5 to 5, bucket 0 delta
delta_0000005_0000005/bucket_00001 | Transaction 5 to 5, bucket 1 delta

When too many deltas have been created, a minor compaction will automatically
run and merge a set of transactions into a single delta:

Filename | Contents
:------- | :--------
00000_0  | Bucket 0 base
00001_0  | Bucket 1 base
delta_0000005_0000010/bucket_00000 | Transaction 5 to 10, bucket 0 delta
delta_0000005_0000010/bucket_00001 | Transaction 5 to 10, bucket 1 delta

When the deltas get large enough, major compaction will re-write the base
to incorporate the deltas.

Filename | Contents
:------- | :--------
base_0000010/bucket_00000 | Transactions upto 10, bucket 0 base
base_0000010/bucket_00001 | Transactions upto 10, bucket 1 base

Reads and compactions do not require locks and thus compactions can
not destructively modify their inputs, but rather write new
directories.

All rows are given an automatic assigned row id, which is the triple of
original transaction id, bucket, and row id, that is guaranteed to be unique.
All update and delete operations refer to that triple.

The ORC files in an ACID table are extended with several column. They
are the operation (insert, update, or delete), the triple that
uniquely identifies the row (originalTransaction, bucket, rowId), and
the current transaction.

```
struct<
  operation: int,
  originalTransaction: bigInt,
  bucket: int,
  rowId: bigInt,
  currentTransaction: bigInt,
  row: struct<...>
>
```

The serialization for the operation codes is:

Operation | Serialization
:-------- | :------------
INSERT    | 0
UPDATE    | 1
DELETE    | 2

When a application or query reads the ACID table, the reader provides
the list of committed transactions to include. This list is produced
by the Hive metastore when a query starts. The task does a merge
sort. Each of the files is sorted by (originalTransaction ascending,
bucket ascending, rowId ascending, and currentTransaction
descending). Only the first record with a currentTransaction that is
in the list of transactions to read is returned, which corresponds to
the last visible update to a row.

To support streaming ingest, we add two additional features. ORC files
may have additional footers written in to their body that is parsable
as a complete ORC file that only includes the records already
written. As the file is later extended the preliminary file footer
becomes dead space within the file. Secondly, a side file named
"*_flush_length" is a small file that contains a set of 8 byte
values. The last complete 8 byte value is the end of the last
preliminary footer.

Two properties are added to the metadata for ORC files to speed up the
processing of the ACID tables. In particular, when a task is reading
part of the base file for a bucket, it will use the first and last
rowIds to find the corresponding spots in the delta files. The
hive.acid.key.index lets the reader skip over stripes in the delta
file that don't need to be read in this task.

Key                 | Meaning
:------------------ | :-----------
hive.acid.stats     | Number of inserts, updates, and deletes comma separated
hive.acid.key.index | The last originalTransaction, bucket, rowId for each stripe
