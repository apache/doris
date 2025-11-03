create table json_t (
    `id` int null,
    `kjson` json null
)
engine=OLAP
duplicate key(id)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1"
);