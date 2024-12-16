create table mtmv_base1 (id INT, value STRING)
    PARTITIONED BY (part_col INT)
    CLUSTERED BY (id) INTO 3 BUCKETS
    STORED AS ORC;

insert into mtmv_base1 PARTITION(part_col=20230101) values
(1, 'A'),
(2, 'B'),
(3, 'C');

insert into mtmv_base1 PARTITION(part_col=20230102) values
(4, 'D'),
(5, 'E'),
(6, 'F');


