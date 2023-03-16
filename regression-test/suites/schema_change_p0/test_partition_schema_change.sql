DROP TABLE IF EXISTS example_range_tbl;
CREATE TABLE IF NOT EXISTS example_range_tbl
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
    `city` VARCHAR(20) COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
)
ENGINE=OLAP
AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
PARTITION BY RANGE(`date`)
(
    PARTITION `p201701` VALUES LESS THAN ("2017-02-01"),
    PARTITION `p201702` VALUES LESS THAN ("2017-03-01"),
    PARTITION `p201703` VALUES LESS THAN ("2017-04-01")
)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES
(
    "replication_num" = "1", "light_schema_change" = "true"
);

INSERT INTO example_range_tbl VALUES
    (1, '2017-01-02', 'Beijing', 10, 1, "2017-01-02 00:00:00", 1, 30, 20);

INSERT INTO example_range_tbl VALUES
    (1, '2017-02-02', 'Beijing', 10, 1, "2017-02-02 00:00:00", 1, 30, 20);

INSERT INTO example_range_tbl VALUES
    (1, '2017-03-02', 'Beijing', 10, 1, "2017-03-02 00:00:00", 1, 30, 20);

select * from example_range_tbl order by `date`;

ALTER table example_range_tbl ADD COLUMN new_column INT MAX default "1";

INSERT INTO example_range_tbl VALUES
    (2, '2017-02-03', 'Beijing', 10, 1, "2017-02-02 00:00:00", 1, 30, 20, 2);

select * from example_range_tbl order by `date`;