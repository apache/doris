use paimon;
create database if not exists test_paimon_spark;
use test_paimon_spark;

drop table if exists test_tb_mix_format;
create table test_tb_mix_format (
    id int,
    value int,
    par string
) PARTITIONED BY (par) TBLPROPERTIES (
    'primary-key' = 'id, par',
    'bucket'=1000,
    'file.format'='orc'
);
-- orc format in partition a
insert into test_tb_mix_format values (1,1,'a'),(2,1,'a'),(3,1,'a'),(4,1,'a'),(5,1,'a'),(6,1,'a'),(7,1,'a'),(8,1,'a'),(9,1,'a'),(10,1,'a');
-- update some data, these splits will be readed by jni
insert into test_tb_mix_format values (1,2,'a'),(2,2,'a'),(3,2,'a'),(4,2,'a'),(5,2,'a');
-- parquet format in partition b
alter table test_tb_mix_format set TBLPROPERTIES ('file.format'='parquet');
insert into test_tb_mix_format values (1,1,'b'),(2,1,'b'),(3,1,'b'),(4,1,'b'),(5,1,'b'),(6,1,'b'),(7,1,'b'),(8,1,'b'),(9,1,'b'),(10,1,'b');
-- update some data, these splits will be readed by jni
insert into test_tb_mix_format values (1,2,'b'),(2,2,'b'),(3,2,'b'),(4,2,'b'),(5,2,'b');
-- delete foramt in table properties, doris should get format by file name
alter table test_tb_mix_format unset TBLPROPERTIES ('file.format');