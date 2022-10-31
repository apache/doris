DROP TABLE IF EXISTS running_difference_test;
CREATE TABLE running_difference_test (
                 `id` int NULL COMMENT 'id' ,
                `day` date COMMENT 'day', 
	`time_val` datetime COMMENT 'time_val',
 	`doublenum` double NULL COMMENT 'doublenum'
                )
DUPLICATE KEY(id) 
DISTRIBUTED BY HASH(id) BUCKETS 3 
PROPERTIES ( 
    "replication_num" = "1"
); 
INSERT into running_difference_test (id,day, time_val,doublenum) values ('1', '2022-11-08', '2022-03-12 11:05:04', 4.7),
                                                   ('1', '2022-10-31', '2022-03-12 10:42:01', 3.3),
                                                    ('1', '2022-10-27', '2022-03-12 10:41:02', 2.6),
                                                   ('1', '2022-10-28', '2022-03-12 10:41:00', 1.4); 
SELECT * from running_difference_test;

SELECT
    id,
    day,
    time_val,
    doublenum,
    running_difference(id) AS delta
FROM
running_difference_test;

SELECT
    id,
    day,
    time_val,
    doublenum,
    running_difference(day) AS delta
FROM
running_difference_test;

SELECT
    id,
    day,
    time_val,
    doublenum,
    running_difference(time_val) AS delta
FROM
running_difference_test;

SELECT
    id,
    day,
    time_val,
    doublenum,
    running_difference(doublenum) AS delta
FROM
running_difference_test;