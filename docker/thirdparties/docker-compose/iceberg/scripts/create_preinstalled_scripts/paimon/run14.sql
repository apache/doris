use paimon;
create database if not exists test_paimon_spark;
use test_paimon_spark;

-- Dedicated append-only table for Doris Paimon JNI nested-column pruning regressions.
drop table if exists jni_complex_column_pruning;
create table jni_complex_column_pruning (
    id BIGINT,
    profile STRUCT<city: STRING, zip: INT, street: STRING>,
    events ARRAY<STRUCT<name: STRING, score: INT, note: STRING>>,
    attributes MAP<STRING, STRUCT<code: INT, label: STRING, note: STRING>>
) using paimon
tblproperties (
    'file.format' = 'parquet'
);

insert into jni_complex_column_pruning values
    (
        1,
        struct('beijing', 100000, 'road-a'),
        array(struct('login', 90, 'web'), struct('purchase', 70, 'app')),
        map(
            'primary', struct(10, 'alpha', 'keep-primary'),
            'backup', struct(11, 'alpha-backup', 'keep-backup')
        )
    ),
    (
        2,
        struct('shanghai', 200000, 'road-b'),
        array(struct('purchase', 95, 'app'), struct('logout', 60, 'web')),
        map(
            'primary', struct(20, 'beta', 'keep-primary'),
            'backup', struct(21, 'beta-backup', 'keep-backup')
        )
    );
