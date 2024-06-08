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