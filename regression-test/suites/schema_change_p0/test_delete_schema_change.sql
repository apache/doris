DROP TABLE IF EXISTS schema_change_delete_regression_test;

CREATE TABLE IF NOT EXISTS schema_change_delete_regression_test (
                `user_id` LARGEINT NOT NULL COMMENT "用户id",
                `date` DATE NOT NULL COMMENT "数据灌入日期时间",
                `city` VARCHAR(20) COMMENT "用户所在城市",
                `age` SMALLINT COMMENT "用户年龄",
                `sex` TINYINT COMMENT "用户性别",
                `last_visit_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                `last_update_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次更新时间",
                `last_visit_date_not_null` DATETIME NOT NULL DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                `cost` BIGINT DEFAULT "0" COMMENT "用户总消费",
                `max_dwell_time` INT DEFAULT "0" COMMENT "用户最大停留时间",
                `min_dwell_time` INT DEFAULT "99999" COMMENT "用户最小停留时间")
            DUPLICATE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
            PROPERTIES ( "replication_num" = "1", "light_schema_change" = "true" );

INSERT INTO schema_change_delete_regression_test VALUES
             (1, '2017-10-01', 'Beijing', 10, 1, '2020-01-01', '2020-01-01', '2020-01-01', 1, 30, 20);

INSERT INTO schema_change_delete_regression_test VALUES
             (2, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2020-01-02', 1, 31, 21);

SELECT * FROM schema_change_delete_regression_test order by user_id ASC, last_visit_date;

ALTER table schema_change_delete_regression_test ADD COLUMN new_column INT default "1";

SELECT * FROM schema_change_delete_regression_test order by user_id DESC, last_visit_date;

INSERT INTO schema_change_delete_regression_test VALUES
             (1, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2020-01-02', 1, 31, 19, 2);

INSERT INTO schema_change_delete_regression_test VALUES
             (2, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, 2);

INSERT INTO schema_change_delete_regression_test VALUES
             (3, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, 1);

INSERT INTO schema_change_delete_regression_test VALUES
             (3, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, 2);
             
DELETE FROM schema_change_delete_regression_test where new_column = 1;

SELECT * FROM schema_change_delete_regression_test order by user_id DESC, last_visit_date;