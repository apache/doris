-- Currently docker is hive 2.x version. Hive 2.x versioned full-acid tables need to run major compaction.
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

alter table orc_full_acid compact 'major';

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

alter table orc_full_acid_par PARTITION(part_col=20230101) compact 'major';
alter table orc_full_acid_par PARTITION(part_col=20230102) compact 'major';

