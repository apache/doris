DROP TABLE IF EXISTS retention_test;

CREATE TABLE retention_test(
                `uid` int COMMENT 'user id', 
                `date` datetime COMMENT 'date time' 
                )
DUPLICATE KEY(uid) 
DISTRIBUTED BY HASH(uid) BUCKETS 3 
PROPERTIES ( 
    "replication_num" = "1"
); 

INSERT into retention_test (uid, date) values (0, '2022-10-12'),
                                        (0, '2022-10-13'),
                                        (0, '2022-10-14'),
                                        (1, '2022-10-12'),
                                        (1, '2022-10-13'),
                                        (2, '2022-10-12'); 

SELECT * from retention_test ORDER BY uid;

SELECT 
    uid,     
    retention(date = '2022-10-12')
        AS r 
            FROM retention_test 
            GROUP BY uid 
            ORDER BY uid ASC;

SELECT 
    uid,     
    retention(date = '2022-10-12', date = '2022-10-13')
        AS r 
            FROM retention_test 
            GROUP BY uid 
            ORDER BY uid ASC;

SELECT 
    uid,     
    retention(date = '2022-10-12', date = '2022-10-13', date = '2022-10-14')
        AS r 
            FROM retention_test 
            GROUP BY uid 
            ORDER BY uid ASC;

SET parallel_fragment_exec_instance_num=4;

SELECT * from retention_test ORDER BY uid;

SELECT 
    uid,     
    retention(date = '2022-10-12')
        AS r 
            FROM retention_test 
            GROUP BY uid 
            ORDER BY uid ASC;

SELECT 
    uid,     
    retention(date = '2022-10-12', date = '2022-10-13')
        AS r 
            FROM retention_test 
            GROUP BY uid 
            ORDER BY uid ASC;

SELECT 
    uid,     
    retention(date = '2022-10-12', date = '2022-10-13', date = '2022-10-14')
        AS r 
            FROM retention_test 
            GROUP BY uid 
            ORDER BY uid ASC;