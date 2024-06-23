create database if not exists demo.test_db;
drop table if exists demo.test_db.location_s3a_table;
create table demo.test_db.location_s3a_table (
    id int,
    val string
) using iceberg 
location 's3a://warehouse/wh/test_db/location_s3a_table'
tblproperties (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
);
insert into demo.test_db.location_s3a_table values (1,'a');
update demo.test_db.location_s3a_table set val='b' where id=1;

drop table if exists demo.test_db.location_s3_table;
create table demo.test_db.location_s3_table (
    id int,
    val string
) using iceberg 
location 's3://warehouse/wh/test_db/location_s3_table'
tblproperties (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
);
insert into demo.test_db.location_s3_table values (1,'a');
update demo.test_db.location_s3_table set val='b' where id=1;

create table demo.test_db.tb_ts_ntz_filter (ts timestamp_ntz) using iceberg;
insert into demo.test_db.tb_ts_ntz_filter values (timestamp_ntz '2024-06-11 12:34:56.123456');