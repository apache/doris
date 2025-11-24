use paimon;

create database if not exists test_paimon_spark;
use test_paimon_spark;

drop table if exists test_varchar_char_type;

create table test_varchar_char_type (
    c1 int,
    c2 char(1),
    c3 char(254),
    c4 char(2147483647),
    c5 varchar(1),
    c6 varchar(65533),
    c7 varchar(2147483646),
    c8 varchar(2147483647),
    c9 string
);
