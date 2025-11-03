create table map_t (
    `id` int null,
    `km_int_int` map<int, int> null
)
engine=OLAP
duplicate key(id)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1"
);