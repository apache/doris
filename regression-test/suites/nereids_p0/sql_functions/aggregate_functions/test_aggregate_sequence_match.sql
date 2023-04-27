-- test no match
DROP TABLE IF EXISTS sequence_match_test1;

CREATE TABLE sequence_match_test1(
                `uid` int COMMENT 'user id',
                `date` datetime COMMENT 'date time', 
                `number` int NULL COMMENT 'number' 
                )
DUPLICATE KEY(uid) 
DISTRIBUTED BY HASH(uid) BUCKETS 3 
PROPERTIES ( 
    "replication_num" = "1"
); 

INSERT INTO sequence_match_test1(uid, date, number) values (1, '2022-11-02 10:41:00', 1),
                                                   (2, '2022-11-02 11:41:00', 7),
                                                   (3, '2022-11-02 16:15:01', 3),
                                                   (4, '2022-11-02 19:05:04', 4),
                                                   (5, '2022-11-02 21:24:12', 5);

SELECT * FROM sequence_match_test1 ORDER BY date;

SELECT sequence_match('(?1)(?2)', date, number = 1, number = 2) FROM sequence_match_test1;
SELECT sequence_count('(?1)(?2)', date, number = 1, number = 2) FROM sequence_match_test1;

SELECT sequence_match('(?1)(?2).*', date, number = 1, number = 2) FROM sequence_match_test1;
SELECT sequence_count('(?1)(?2).*', date, number = 1, number = 2) FROM sequence_match_test1;

--not match
SELECT sequence_match('(?1)(?t>3600)(?2)', date, number = 1, number = 7) FROM sequence_match_test1;
--match
SELECT sequence_match('(?1)(?t>=3600)(?2)', date, number = 1, number = 7) FROM sequence_match_test1;

--not match
SELECT sequence_count('(?1)(?t>3600)(?2)', date, number = 1, number = 7) FROM sequence_match_test1;
--match
SELECT sequence_count('(?1)(?t>=3600)(?2)', date, number = 1, number = 7) FROM sequence_match_test1;

SELECT sequence_match('(?1)(?2)', date, number = 1, number = 4, number = 3) FROM sequence_match_test1;
SELECT sequence_count('(?1)(?2)', date, number = 1, number = 4, number = 3) FROM sequence_match_test1;


--test match

DROP TABLE IF EXISTS sequence_match_test2;

CREATE TABLE sequence_match_test2(
                `uid` int COMMENT 'user id',
                `date` datetime COMMENT 'date time', 
                `number` int NULL COMMENT 'number' 
                )
DUPLICATE KEY(uid) 
DISTRIBUTED BY HASH(uid) BUCKETS 3 
PROPERTIES ( 
    "replication_num" = "1"
); 

INSERT INTO sequence_match_test2(uid, date, number) values (1, '2022-11-02 10:41:00', 1),
                                                   (2, '2022-11-02 13:28:02', 2),
                                                   (3, '2022-11-02 16:15:01', 1),
                                                   (4, '2022-11-02 19:05:04', 2),
                                                   (5, '2022-11-02 20:08:44', 3); 

SELECT * FROM sequence_match_test2 ORDER BY date;

SELECT sequence_match('(?1)(?2)', date, number = 1, number = 3) FROM sequence_match_test2;
SELECT sequence_count('(?1)(?2)', date, number = 1, number = 3) FROM sequence_match_test2;

SELECT sequence_match('(?1)(?2)', date, number = 1, number = 2) FROM sequence_match_test2;
SELECT sequence_count('(?1)(?2)', date, number = 1, number = 2) FROM sequence_match_test2;

SELECT sequence_match('(?1)(?2)', date, number = 1, number = 3, number = 4) FROM sequence_match_test2;
SELECT sequence_count('(?1)(?2)', date, number = 1, number = 3, number = 4) FROM sequence_match_test2;

--test reverse

DROP TABLE IF EXISTS sequence_match_test3;

CREATE TABLE sequence_match_test3(
                `uid` int COMMENT 'user id',
                `date` datetime COMMENT 'date time', 
                `number` int NULL COMMENT 'number' 
                )
DUPLICATE KEY(uid) 
DISTRIBUTED BY HASH(uid) BUCKETS 3 
PROPERTIES ( 
    "replication_num" = "1"
); 

INSERT INTO sequence_match_test3(uid, date, number) values (5, '2022-11-02 20:08:44', 3),
                                                   (4, '2022-11-02 19:05:04', 2),
                                                   (3, '2022-11-02 16:15:01', 1),
                                                   (2, '2022-11-02 13:28:02', 2),
                                                   (1, '2022-11-02 10:41:00', 1);

SELECT * FROM sequence_match_test3 ORDER BY date;

SELECT sequence_match('(?1)(?2)', date, number = 1, number = 3) FROM sequence_match_test3;
SELECT sequence_count('(?1)(?2)', date, number = 1, number = 3) FROM sequence_match_test3;

SELECT sequence_match('(?1)(?2)', date, number = 1, number = 2) FROM sequence_match_test3;
SELECT sequence_count('(?1)(?2)', date, number = 1, number = 2) FROM sequence_match_test3;

SELECT sequence_match('(?1)(?2)', date, number = 1, number = 3, number = 4) FROM sequence_match_test3;
SELECT sequence_count('(?1)(?2)', date, number = 1, number = 3, number = 4) FROM sequence_match_test3;
