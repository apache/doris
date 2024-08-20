
use demo.test_db;

drop table  if exists complex_parquet_v1_schema_change;
CREATE TABLE complex_parquet_v1_schema_change (
    id int,
    col1 array<int>,
    col2 array<float>,
    col3 array<decimal(12,4)>,
    col4 map<int,int>,
    col5 map<int,float>,
    col6 map<int,decimal(12,5)>,
    col7 struct<x:int,y:float,z:decimal(12,5)>,
    col8 int,
    col9 float,
    col10 decimal(12,5),
    col_del int
)USING iceberg
TBLPROPERTIES(
  'write.format.default' = 'parquet',
  'format-version'='1');

INSERT INTO complex_parquet_v1_schema_change
VALUES
  (1, array(1, 2, 3), array(1.1, 2.2, 3.3), array(10.1234, 20.5678, 30.9876),map(1, 100, 2, 200), map(1, 1.1, 2, 2.2), map(1, 10.12345, 2, 20.98765),named_struct('x', 1, 'y', 1.1, 'z', 10.12345),10, 10.5, 100.56789, 999),
  (2, array(4, 5), array(4.4, 5.5), array(40.1234, 50.5678),map(3, 300, 4, 400), map(3, 3.3, 4, 4.4), map(3, 30.12345, 4, 40.98765),named_struct('x', 2, 'y', 2.2, 'z', 20.98765),20, 20.5, 200.56789, 888),
  (3, array(6), array(6.6), array(60.1234),map(5, 500), map(5, 5.5), map(5, 50.12345),named_struct('x', 3, 'y', 3.3, 'z', 30.12345),30, 30.5, 300.56789, 777),
  (4, array(7,7,7), array(7.1,7.2,7.3), array(10,30),map(2,4), map(6,7), map(8,9),named_struct('x', 4, 'y', 4.4, 'z', 40.98765),40, 40.5, 400.56789, 666),
  (5, NULL, NULL, NULL,NULL, NULL, NULL,named_struct('x', 5, 'y', 5.5, 'z', 50.12345),50, 50.5, 500.56789, 555),
  (6, array(7, 8), array(7.7, 8.8), array(70.1234, 80.5678),map(6, 600, 7, 700), map(6, 6.6, 7, 7.7), map(6, 60.12345, 7, 70.98765),named_struct('x', 6, 'y', 6.6, 'z', 60.12345),60, 60.5, 600.56789, 444),
  (7, array(9, 10), array(9.9, 10.10), array(90.1234, 100.5678),map(8, 800, 9, 900), map(8, 8.8, 9, 9.9), map(8, 80.12345, 9, 90.98765),named_struct('x', 7, 'y', 7.7, 'z', 70.98765),70, 70.5, 700.56789, 333),
  (8, array(11, 12), array(11.11, 12.12), array(110.1234, 120.5678),map(10, 1000, 11, 1100), map(10, 10.10, 11, 11.11), map(10, 100.12345, 11, 110.98765),named_struct('x', 8, 'y', 8.8, 'z', 80.12345),80, 80.5, 800.56789, 222),
  (9, array(13, 14), array(13.13, 14.14), array(130.1234, 140.5678),map(12, 1200, 13, 1300), map(12, 12.12, 13, 13.13), map(12, 120.12345, 13, 130.98765),named_struct('x', 9, 'y', 9.9, 'z', 90.12345),90, 90.5, 900.56789, 111),
  (10, array(15, 16), array(15.15, 16.16), array(150.1234, 160.5678),map(14, 1400, 15, 1500), map(14, 14.14, 15, 15.15), map(14, 140.12345, 15, 150.98765),named_struct('x', 10, 'y', 10.10, 'z', 100.12345),100, 100.5, 1000.56789, 0);


ALTER TABLE complex_parquet_v1_schema_change CHANGE COLUMN col1.element type bigint;
ALTER TABLE complex_parquet_v1_schema_change CHANGE COLUMN col2.element type double;
ALTER TABLE complex_parquet_v1_schema_change CHANGE COLUMN col3.element type decimal(20,4);
ALTER TABLE complex_parquet_v1_schema_change CHANGE COLUMN col4.value type bigint;
ALTER TABLE complex_parquet_v1_schema_change CHANGE COLUMN col5.value type double;
ALTER TABLE complex_parquet_v1_schema_change CHANGE COLUMN col6.value type decimal(20,5);
ALTER TABLE complex_parquet_v1_schema_change CHANGE COLUMN col7.x type bigint;
ALTER TABLE complex_parquet_v1_schema_change CHANGE COLUMN col7.y type double;
ALTER TABLE complex_parquet_v1_schema_change CHANGE COLUMN col7.z type decimal(20,5);
alter table complex_parquet_v1_schema_change CHANGE COLUMN col8 col8 bigint;
alter table complex_parquet_v1_schema_change CHANGE COLUMN col9 col9 double;
alter table complex_parquet_v1_schema_change CHANGE COLUMN col10 col10 decimal(20,5);
alter table complex_parquet_v1_schema_change drop column col7.z;
alter table complex_parquet_v1_schema_change add column col7.add double;
alter table complex_parquet_v1_schema_change change column col7.add first;
alter table complex_parquet_v1_schema_change rename COLUMN col1 to rename_col1;
alter table complex_parquet_v1_schema_change rename COLUMN col2 to rename_col2;
alter table complex_parquet_v1_schema_change rename COLUMN col3 to rename_col3;
alter table complex_parquet_v1_schema_change rename COLUMN col4 to rename_col4;
alter table complex_parquet_v1_schema_change rename COLUMN col5 to rename_col5;
alter table complex_parquet_v1_schema_change rename COLUMN col6 to rename_col6;
alter table complex_parquet_v1_schema_change rename COLUMN col7 to rename_col7;
alter table complex_parquet_v1_schema_change rename COLUMN col8 to rename_col8;
alter table complex_parquet_v1_schema_change rename COLUMN col9 to rename_col9;
alter table complex_parquet_v1_schema_change rename COLUMN col10 to rename_col10;
alter table complex_parquet_v1_schema_change drop column col_del;
alter table complex_parquet_v1_schema_change CHANGE COLUMN rename_col8 first;
alter table complex_parquet_v1_schema_change CHANGE COLUMN rename_col9 after rename_col8;
alter table complex_parquet_v1_schema_change CHANGE COLUMN rename_col10 after rename_col9;
alter table complex_parquet_v1_schema_change add column col_add int;
alter table complex_parquet_v1_schema_change add column col_add2 int;


INSERT INTO complex_parquet_v1_schema_change (id, rename_col8, rename_col9, rename_col10,rename_col1, rename_col2, rename_col3,rename_col4, rename_col5, rename_col6,rename_col7, col_add, col_add2)
VALUES
  (11,100, 11.1, 110.12345,array(11, 12, 13), array(11.1, 12.2, 13.3), array(110.1234, 120.5678, 130.9876),map(11, 1100, 12, 1200), map(11, 11.1, 12, 12.2), map(11, 110.12345, 12, 120.98765),named_struct('add', 11.1, 'x', 11, 'y', 11.1),110, 120),
  (12,200, 22.2, 220.12345,array(14, 15), array(14.4, 15.5), array(140.1234, 150.5678),map(13, 1300, 14, 1400), map(13, 13.3, 14, 14.4), map(13, 130.12345, 14, 140.98765),named_struct('add', 22.2, 'x', 12, 'y', 12.2),130, 140),
  (13,300, 33.3, 330.12345,array(16), array(16.6), array(160.1234),map(15, 1500), map(15, 15.5), map(15, 150.12345),named_struct('add', 33.3, 'x', 13, 'y', 13.3),150, 160),
  (14,400, 44.4, 440.12345,array(), array(), array(),map(), map(), map(),named_struct('add', 44.4, 'x', 14, 'y', 14.4),170, 180),
  (15,500, 55.5, 550.12345,NULL, NULL, NULL,NULL, NULL, NULL,named_struct('add', 55.5, 'x', 15, 'y', 15.5),190, 200);




