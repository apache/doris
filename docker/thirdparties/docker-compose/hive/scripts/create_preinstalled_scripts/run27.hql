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

