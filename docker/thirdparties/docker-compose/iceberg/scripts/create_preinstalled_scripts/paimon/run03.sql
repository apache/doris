use paimon;

create database if not exists test_paimon_spark;
use test_paimon_spark;

drop table if exists test_varchar_char_type;

create table test_varchar_char_type (
    c1 int,
    c2 char(1),
    c3 char(255),
    c4 char(256),
    c5 char(2147483647),
    c6 varchar(1),
    c7 varchar(65533),
    c8 varchar(2147483646),
    c9 varchar(2147483647),
    c10 string
);
