SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table orc_full_acid_empty (id INT, value STRING)
CLUSTERED BY (id) INTO 3 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional' = 'true');

create table orc_full_acid_par_empty (id INT, value STRING)
PARTITIONED BY (part_col INT)
CLUSTERED BY (id) INTO 3 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional' = 'true');

create table orc_full_acid (id INT, value STRING)
CLUSTERED BY (id) INTO 3 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional' = 'true');

insert into orc_full_acid values
(1, 'A'),
(2, 'B'),
(3, 'C');

update orc_full_acid set value = 'CC' where id = 3;

create table orc_full_acid_par (id INT, value STRING)
PARTITIONED BY (part_col INT)
CLUSTERED BY (id) INTO 3 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional' = 'true');

insert into orc_full_acid_par PARTITION(part_col=20230101) values
(1, 'A'),
(2, 'B'),
(3, 'C');

insert into orc_full_acid_par PARTITION(part_col=20230102) values
(4, 'D'),
(5, 'E'),
(6, 'F');

update orc_full_acid_par set value = 'BB' where id = 2;


CREATE TABLE acid_orc_special_name(                            
   `operation` int,
   `originalTransaction` int ,
   `bucket` int,
   `rowId` int,
   `currentTransaction` int        ,                         
   `row` struct<rowid:int,`bucket`:int> )
PARTITIONED BY (`data_id` int)
stored as orc     TBLPROPERTIES ( 'orc.compress'='snappy','transactional'='true','transactional_properties'='default');

INSERT INTO acid_orc_special_name PARTITION (data_id = 1) VALUES (1, 101, 10, 1001, 102, named_struct('rowid',1401,'bucket', 140));
INSERT INTO acid_orc_special_name PARTITION (data_id = 1) VALUES (2, 102, 20, 1002, 103, named_struct('rowid',1402,'bucket', 240));
INSERT INTO acid_orc_special_name PARTITION (data_id = 3) VALUES  (3, 103, 30, 1003, 104, named_struct('rowid',1403,'bucket', 340)),(4, 104, 40, 1004, 105, named_struct('rowid',1404,'bucket', 440));
UPDATE acid_orc_special_name SET `operation` = 6, `bucket` = 60 WHERE `rowId` = 1003 AND `data_id` = 3;
DELETE FROM acid_orc_special_name WHERE `rowId` = 1001 AND `data_id` = 1;
INSERT INTO acid_orc_special_name PARTITION (data_id = 1) VALUES (9, 909, 90, 9009, 902, named_struct('rowid',1079,'bucket', 95));