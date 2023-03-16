CREATE TABLE IF NOT EXISTS logs1 (
 `log_time`      DATETIME NOT NULL,
 `machine_name`  VARCHAR(128) NOT NULL,
 `machine_group` VARCHAR(128) NOT NULL,
 `cpu_idle`      FLOAT,
 `cpu_nice`      FLOAT,
 `cpu_system`    FLOAT,
 `cpu_user`      FLOAT,
 `cpu_wio`       FLOAT,
 `disk_free`     FLOAT,
 `disk_total`    FLOAT,
 `part_max_used` FLOAT,
 `load_fifteen`  FLOAT,
 `load_five`     FLOAT,
 `load_one`      FLOAT,
 `mem_buffers`   FLOAT,
 `mem_cached`    FLOAT,
 `mem_free`      FLOAT,
 `mem_shared`    FLOAT,
 `swap_free`     FLOAT,
 `bytes_in`      FLOAT,
 `bytes_out`     FLOAT
)
ENGINE = olap
DUPLICATE KEY( `log_time`, `machine_name`, `machine_group`)
DISTRIBUTED BY HASH(machine_name) BUCKETS 10
PROPERTIES("replication_num" = "1");
