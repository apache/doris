DROP TABLE IF EXISTS running_difference_test;

CREATE TABLE running_difference_test (
                 `id` int NOT NULL COMMENT 'id' ,
                `day` date COMMENT 'day', 
	`time_val` datetime COMMENT 'time_val',
 	`doublenum` double NULL COMMENT 'doublenum'
                )
DUPLICATE KEY(id) 
DISTRIBUTED BY HASH(id) BUCKETS 3 
PROPERTIES ( 
    "replication_num" = "1"
); 
                                                  
INSERT into running_difference_test (id,day, time_val,doublenum) values ('1', '2022-10-28', '2022-03-12 10:41:00', null),
                                                   ('2','2022-10-27', '2022-03-12 10:41:02', 2.6),
                                                   ('3','2022-10-28', '2022-03-12 10:41:03', 2.5),
                                                   ('4','2022-9-29', '2022-03-12 10:41:03', null),
                                                   ('5','2022-10-31', '2022-03-12 10:42:01', 3.3),
                                                   ('6', '2022-11-08', '2022-03-12 11:05:04', 4.7); 
SELECT * from running_difference_test ORDER BY id ASC;

SELECT
    id,
    running_difference(id) AS delta
FROM
(
    SELECT
        id,
        day,
        time_val,
        doublenum
    FROM running_difference_test
)as runningDifference ORDER BY id ASC;

SELECT
    day,
    running_difference(day) AS delta
FROM
(
    SELECT
        id,
        day,
        time_val,
        doublenum
    FROM running_difference_test
)as runningDifference ORDER BY id ASC;

SELECT
    time_val,
    running_difference(time_val) AS delta
FROM
(
    SELECT
        id,
        day,
        time_val,
        doublenum
    FROM running_difference_test
)as runningDifference ORDER BY id ASC;

SELECT
    doublenum,
    running_difference(doublenum) AS delta
FROM
(
    SELECT
        id,
        day,
        time_val,
        doublenum
    FROM running_difference_test
)as runningDifference ORDER BY id ASC;