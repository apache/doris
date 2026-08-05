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
-- Paimon 1.4.2 parses every present shredding schema as JSON, so remove the property
-- before writing unshredded files.
alter table variant_mixed_su unset tblproperties (
    'parquet.variant.shreddingSchema'
);
insert into variant_mixed_su values
    (2, date '2026-07-01', parse_json('{"name":"bob","age":30,"layout":"unshredded"}'));

-- Keep a primary-key table separate from variant_smoke so the regression covers both Paimon
-- append-only reads and deduplicate merge reads with Variant payloads.
drop table if exists variant_primary_key_smoke;
create table variant_primary_key_smoke (
    id BIGINT,
    payload VARIANT
) using paimon
tblproperties (
    'primary-key' = 'id',
    'bucket' = '1',
    'merge-engine' = 'deduplicate',
    'file.format' = 'parquet'
);

insert into variant_primary_key_smoke values
    (1, parse_json('{"name":"alice","version":1,"active":false,"profile":{"city":"beijing"}}')),
    (2, parse_json('{"name":"bob","version":1,"tags":["old"]}')),
    (3, parse_json('[10,"original"]'));

-- A second snapshot updates two existing keys and inserts a new key. Doris should expose only the
-- latest logical row for ids 1 and 2.
insert into variant_primary_key_smoke values
    (1, parse_json('{"name":"alice-updated","version":2,"active":true,"profile":{"city":"hangzhou"}}')),
    (2, parse_json('{"name":"bob","version":2,"tags":["new","primary-key"]}')),
    (4, parse_json('{"name":"carol","version":1,"active":true}'));
