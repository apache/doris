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

drop table if exists variant_shredded;
create table variant_shredded (
    id BIGINT,
    event_date DATE,
    payload VARIANT
) using paimon
partitioned by (event_date)
tblproperties (
    'file.format' = 'parquet',
    'parquet.variant.shreddingSchema' = '{"type":"ROW","fields":[{"name":"payload","type":{"type":"ROW","fields":[{"name":"name","type":"STRING"},{"name":"age","type":"INT"}]}}]}'
);

insert into variant_shredded values
    (1, date '2026-06-01', parse_json('{"name":"alice","age":18,"extra":"shredded"}')),
    (2, date '2026-06-01', parse_json('{"name":"bob","age":30}'));

drop table if exists variant_mixed_us;
create table variant_mixed_us (
    id BIGINT,
    event_date DATE,
    payload VARIANT
) using paimon
partitioned by (event_date)
tblproperties ('file.format' = 'parquet');

insert into variant_mixed_us values
    (1, date '2026-06-01', parse_json('{"name":"alice","age":18,"layout":"unshredded"}'));
alter table variant_mixed_us set tblproperties (
    'parquet.variant.shreddingSchema' = '{"type":"ROW","fields":[{"name":"payload","type":{"type":"ROW","fields":[{"name":"name","type":"STRING"},{"name":"age","type":"INT"}]}}]}'
);
insert into variant_mixed_us values
    (2, date '2026-07-01', parse_json('{"name":"bob","age":30,"layout":"shredded"}'));

drop table if exists variant_mixed_su;
create table variant_mixed_su (
    id BIGINT,
    event_date DATE,
    payload VARIANT
) using paimon
partitioned by (event_date)
tblproperties (
    'file.format' = 'parquet',
    'parquet.variant.shreddingSchema' = '{"type":"ROW","fields":[{"name":"payload","type":{"type":"ROW","fields":[{"name":"name","type":"STRING"},{"name":"age","type":"INT"}]}}]}'
);

insert into variant_mixed_su values
    (1, date '2026-06-01', parse_json('{"name":"alice","age":18,"layout":"shredded"}'));
alter table variant_mixed_su set tblproperties (
    'parquet.variant.shreddingSchema' = ''
);
insert into variant_mixed_su values
    (2, date '2026-07-01', parse_json('{"name":"bob","age":30,"layout":"unshredded"}'));
