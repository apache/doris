create table arr_t (
    `id` int null,
    `kaint` array<int> null
)
engine=OLAP
duplicate key(id)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1"
);