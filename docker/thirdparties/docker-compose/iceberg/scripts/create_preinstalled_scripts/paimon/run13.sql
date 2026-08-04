use paimon;
create database if not exists test_paimon_spark;
use test_paimon_spark;

drop table if exists variant_smoke;
create table variant_smoke (
    id BIGINT,
    payload VARIANT
) using paimon
tblproperties (
    'file.format' = 'parquet'
);

insert into variant_smoke values
    (1, parse_json('{"name":"alice","age":18,"active":true,"score":98.5,"tags":["flink","paimon"],"profile":{"city":"beijing","zip":100000},"missing":null}')),
    (2, parse_json('{"name":"bob","age":30,"active":false,"tags":["doris"],"profile":{"city":"shanghai"},"extra":{"levels":[1,2,3]}}')),
    (3, parse_json('[1,"mixed",false,null,{"k":"v"}]'));
