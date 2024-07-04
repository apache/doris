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

