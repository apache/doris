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




create table orc_to_acid_tb (id INT, value STRING)
PARTITIONED BY (part_col INT)
CLUSTERED BY (id) INTO 3 BUCKETS
STORED AS ORC;
INSERT INTO TABLE orc_to_acid_tb PARTITION (part_col=101) VALUES (1, 'A'), (3, 'C');
INSERT INTO TABLE orc_to_acid_tb PARTITION (part_col=102) VALUES (2, 'B');
ALTER TABLE orc_to_acid_tb SET TBLPROPERTIES ('transactional'='true');


create table orc_to_acid_compacted_tb (id INT, value STRING)
PARTITIONED BY (part_col INT)
CLUSTERED BY (id) INTO 3 BUCKETS
STORED AS ORC;
INSERT INTO TABLE orc_to_acid_compacted_tb PARTITION (part_col=101) VALUES (1, 'A'), (3, 'C');
INSERT INTO TABLE orc_to_acid_compacted_tb PARTITION (part_col=102) VALUES (2, 'B');
ALTER TABLE orc_to_acid_compacted_tb SET TBLPROPERTIES ('transactional'='true');
ALTER TABLE orc_to_acid_compacted_tb partition(part_col='101') COMPACT 'major' and wait;
ALTER TABLE orc_to_acid_compacted_tb partition(part_col='102') COMPACT 'major' and wait;
INSERT INTO TABLE orc_to_acid_compacted_tb PARTITION (part_col=102) VALUES (4, 'D');
update orc_to_acid_compacted_tb set value = "CC" where id = 3; 
update orc_to_acid_compacted_tb set value = "BB" where id = 2; 


create table orc_acid_minor (id INT, value STRING)
CLUSTERED BY (id) INTO 3 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional' = 'true');
insert into orc_acid_minor values (1, 'A');
insert into orc_acid_minor values (2, 'B');
insert into orc_acid_minor values (3, 'C');
update orc_acid_minor set value = "BB" where id = 2; 
ALTER TABLE orc_acid_minor COMPACT 'minor' and wait;
insert into orc_acid_minor values (4, 'D');
update orc_acid_minor set value = "DD" where id = 4; 
DELETE FROM orc_acid_minor WHERE id = 3;


create table orc_acid_major (id INT, value STRING)
CLUSTERED BY (id) INTO 3 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional' = 'true');
insert into orc_acid_major values (1, 'A');
insert into orc_acid_major values (2, 'B');
insert into orc_acid_major values (3, 'C');
update orc_acid_major set value = "BB" where id = 2; 
ALTER TABLE orc_acid_major COMPACT 'minor' and wait;
insert into orc_acid_major values (4, 'D');
update orc_acid_major set value = "DD" where id = 4; 
DELETE FROM orc_acid_major WHERE id = 3;
