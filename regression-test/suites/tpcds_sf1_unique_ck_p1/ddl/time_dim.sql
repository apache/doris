CREATE TABLE IF NOT EXISTS time_dim (
    t_time_sk bigint,
    t_time_id char(16),
    t_time integer,
    t_hour integer,
    t_minute integer,
    t_second integer,
    t_am_pm char(2),
    t_shift char(20),
    t_sub_shift char(20),
    t_meal_time char(20)
)
UNIQUE KEY(t_time_sk)
CLUSTER BY(t_time_id)
DISTRIBUTED BY HASH(t_time_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
)
