use paimon;
create database if not exists test_partition_legacy;
use test_partition_legacy;

drop table if exists test_partition_legacy_true;
CREATE TABLE test_partition_legacy_true (
    dt DATE,
    user_id BIGINT,
    event_name STRING,
    event_value DOUBLE
) USING paimon
PARTITIONED BY (dt)
TBLPROPERTIES (
    'primary-key' = 'dt, user_id',
    'bucket' = '1',
    'merge-engine' = 'deduplicate',
    'partition.legacy-name' = 'true'
);

INSERT INTO test_partition_legacy_true (dt, user_id, event_name, event_value) VALUES
    (CAST('2026-02-13' AS DATE), CAST(1001 AS BIGINT), 'click', CAST(1.5 AS DOUBLE)),
    (CAST('2026-02-13' AS DATE), CAST(1002 AS BIGINT), 'view', CAST(2.0 AS DOUBLE)),
    (CAST('2026-02-13' AS DATE), CAST(1003 AS BIGINT), 'purchase', CAST(99.9 AS DOUBLE)),
    (CAST('2026-03-01' AS DATE), CAST(2001 AS BIGINT), 'click', CAST(3.0 AS DOUBLE)),
    (CAST('2026-03-01' AS DATE), CAST(2002 AS BIGINT), 'view', CAST(4.5 AS DOUBLE)),
    (CAST('2026-03-02' AS DATE), CAST(3001 AS BIGINT), 'click', CAST(5.0 AS DOUBLE)),
    (CAST('2026-03-02' AS DATE), CAST(3002 AS BIGINT), 'purchase', CAST(188.0 AS DOUBLE)),
    (CAST('2026-03-02' AS DATE), CAST(3003 AS BIGINT), 'view', CAST(6.5 AS DOUBLE));


drop table if exists test_partition_legacy_false;
CREATE TABLE test_partition_legacy_false (
    dt DATE,
    user_id BIGINT,
    event_name STRING,
    event_value DOUBLE
) USING paimon
PARTITIONED BY (dt)
TBLPROPERTIES (
    'primary-key' = 'dt, user_id',
    'bucket' = '1',
    'merge-engine' = 'deduplicate',
    'partition.legacy-name' = 'false'
);

INSERT INTO test_partition_legacy_false (dt, user_id, event_name, event_value) VALUES
    (CAST('2026-02-13' AS DATE), CAST(1001 AS BIGINT), 'click', CAST(1.5 AS DOUBLE)),
    (CAST('2026-02-13' AS DATE), CAST(1002 AS BIGINT), 'view', CAST(2.0 AS DOUBLE)),
    (CAST('2026-02-13' AS DATE), CAST(1003 AS BIGINT), 'purchase', CAST(99.9 AS DOUBLE)),
    (CAST('2026-03-01' AS DATE), CAST(2001 AS BIGINT), 'click', CAST(3.0 AS DOUBLE)),
    (CAST('2026-03-01' AS DATE), CAST(2002 AS BIGINT), 'view', CAST(4.5 AS DOUBLE)),
    (CAST('2026-03-02' AS DATE), CAST(3001 AS BIGINT), 'click', CAST(5.0 AS DOUBLE)),
    (CAST('2026-03-02' AS DATE), CAST(3002 AS BIGINT), 'purchase', CAST(188.0 AS DOUBLE)),
    (CAST('2026-03-02' AS DATE), CAST(3003 AS BIGINT), 'view', CAST(6.5 AS DOUBLE));
