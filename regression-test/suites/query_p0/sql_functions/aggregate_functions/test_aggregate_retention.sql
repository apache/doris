DROP TABLE IF EXISTS retention_test;

CREATE TABLE IF NOT EXISTS retention_test(
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

SELECT
    sum(a.r[1])
        AS s1, 
    sum(a.r[2])
        AS s2,
    sum(a.r[3])
        AS s3  
            FROM(
                SELECT
                    uid,
                    retention(date = '2022-10-12', date = '2022-10-13', date = '2022-10-14') 
                        AS r 
                            FROM retention_test
                            GROUP BY uid 
                ) a;


DROP TABLE IF EXISTS retention_test_many_params;

CREATE TABLE IF NOT EXISTS retention_test_many_params(
                `uid` int COMMENT 'user id', 
                `date` datetime COMMENT 'date time' 
                )
DUPLICATE KEY(uid) 
DISTRIBUTED BY HASH(uid) BUCKETS 1 
PROPERTIES ( 
    "replication_num" = "1"
); 

INSERT into retention_test_many_params (uid, date) values (0, '2022-10-12'),
                                        (0, '2022-10-13'),
                                        (0, '2022-10-14'),
                                        (0, '2022-10-15'),
                                        (0, '2022-10-16'),
                                        (0, '2022-10-17'),
                                        (0, '2022-10-18'),
                                        (0, '2022-10-19'),
                                        (0, '2022-10-20'),
                                        (0, '2022-10-21'),
                                        (0, '2022-10-22'),
                                        (0, '2022-10-23');

SELECT * from retention_test_many_params ORDER BY date;

SELECT 
    uid,     
    retention(date = '2022-10-12', date = '2022-10-13', date = '2022-10-14', 
            date = '2022-10-15', date = '2022-10-16', date = '2022-10-17',
            date = '2022-10-18', date = '2022-10-19', date = '2022-10-20',
            date = '2022-10-21', date = '2022-10-22', date = '2022-10-23'
            )
        AS r 
            FROM retention_test_many_params 
            GROUP BY uid 
            ORDER BY uid ASC;
