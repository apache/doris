-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

use paimon;
create database if not exists test_paimon_spark_blob;
use test_paimon_spark_blob;

-- Default blob storage fixtures for Doris FileScannerV2 native/JNI reads.
-- Spark exposes BLOB as BINARY; blob-field converts the named columns to Paimon BLOB.
-- Do not use blob-descriptor-field or blob-external-storage-field here.
-- See blob_fixtures.md for the expected results and suggested Groovy assertions.
-- Binary values use hex/UTF-8 encoding explicitly; no CHAR/VARCHAR casts can truncate them.
create or replace temporary view paimon_blob_seed as
select id, case_name, image, length(image) as expected_size, hex(image) as expected_hex
from values
    (1,  'null_first',          cast(null as binary)),
    (2,  'empty',               unhex('')),
    (3,  'one_zero',            unhex('00')),
    (4,  'one_ascii_max',       unhex('7F')),
    (5,  'one_high_bit',        unhex('80')),
    (6,  'one_ff',              unhex('FF')),
    (7,  'embedded_zero',       unhex('004100FF')),
    (8,  'all_byte_values',     unhex(concat(
        '000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F',
        '202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F',
        '404142434445464748494A4B4C4D4E4F505152535455565758595A5B5C5D5E5F',
        '606162636465666768696A6B6C6D6E6F707172737475767778797A7B7C7D7E7F',
        '808182838485868788898A8B8C8D8E8F909192939495969798999A9B9C9D9E9F',
        'A0A1A2A3A4A5A6A7A8A9AAABACADAEAFB0B1B2B3B4B5B6B7B8B9BABBBCBDBEBF',
        'C0C1C2C3C4C5C6C7C8C9CACBCCCDCECFD0D1D2D3D4D5D6D7D8D9DADBDCDDDEDF',
        'E0E1E2E3E4E5E6E7E8E9EAEBECEDEEEFF0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF'))),
    (9,  'utf8',                encode('你好Doris🌍', 'UTF-8')),
    (10, 'hello_world',         unhex('68656C6C6F20776F726C64')),
    (11, 'limit_minus_one',     encode('1234567', 'UTF-8')),
    (12, 'limit_exact',         encode('12345678', 'UTF-8')),
    (13, 'limit_plus_one',      encode('123456789', 'UTF-8')),
    (14, 'json',               encode('{"id":1,"name":"blob","active":true}', 'UTF-8')),
    (15, 'uri_is_payload',     encode('s3://not-a-reference/image.bin', 'UTF-8')),
    (16, 'control_bytes',      unhex('00090A0D1B7FFF')),
    (17, 'repeated_payload',   unhex('68656C6C6F20776F726C64')),
    (18, 'null_middle',        cast(null as binary)),
    (19, 'empty_after_null',   unhex('')),
    (20, 'length_1',           encode('a', 'UTF-8')),
    (21, 'length_47',          encode(repeat('a', 47), 'UTF-8')),
    (22, 'length_48',          encode(repeat('b', 48), 'UTF-8')),
    (23, 'length_49',          encode(repeat('c', 49), 'UTF-8')),
    (24, 'length_111',         encode(repeat('a', 111), 'UTF-8')),
    (25, 'length_112',         encode(repeat('b', 112), 'UTF-8')),
    (26, 'length_113',         encode(repeat('c', 113), 'UTF-8')),
    (27, 'length_127',         encode(repeat('a', 127), 'UTF-8')),
    (28, 'length_128',         encode(repeat('b', 128), 'UTF-8')),
    (29, 'length_129',         encode(repeat('c', 129), 'UTF-8')),
    (30, 'length_255',         encode(repeat('a', 255), 'UTF-8')),
    (31, 'length_256',         encode(repeat('b', 256), 'UTF-8')),
    (32, 'length_257',         encode(repeat('c', 257), 'UTF-8')),
    (33, 'length_8175',        encode(repeat('a', 8175), 'UTF-8')),
    (34, 'empty_after_large',  unhex('')),
    (35, 'length_8176',        encode(repeat('b', 8176), 'UTF-8')),
    (36, 'null_after_large',   cast(null as binary)),
    (37, 'length_8177',        encode(repeat('c', 8177), 'UTF-8')),
    (38, 'small_after_large',  unhex('78')),
    (39, 'null_last',          cast(null as binary))
as seed(id, case_name, image);

-- One ordered writer keeps NULL/empty/large/small transitions in the same .blob file.
-- The 8176-byte transitions also exercise three-byte zigzag varints without large objects.
drop table if exists blob_basic;
create table blob_basic (
    id int, case_name string, image binary, expected_size int, expected_hex string
) using paimon tblproperties (
    'file.format' = 'parquet', 'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true', 'blob-field' = 'image', 'write-only' = 'true'
);
insert into blob_basic
select /*+ COALESCE(1) */ * from paimon_blob_seed order by id;

drop table if exists blob_empty;
create table blob_empty (id int, image binary) using paimon tblproperties (
    'file.format' = 'parquet', 'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true', 'blob-field' = 'image', 'write-only' = 'true'
);

-- Nonempty table, but the BLOB has no non-NULL records: index/footer-only .blob files.
drop table if exists blob_all_null;
create table blob_all_null (id int, image binary) using paimon tblproperties (
    'file.format' = 'parquet', 'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true', 'blob-field' = 'image', 'write-only' = 'true'
);
insert into blob_all_null
select /*+ COALESCE(1) */ cast(id as int), cast(null as binary) from range(1, 5, 1, 1);

-- A small, isolated fixture for paimon_blob_max_size=8: NULL, 0, 7, 8 and 9 bytes.
-- Test inline failures separately from descriptor output and from projections of id alone.
drop table if exists blob_size_limit;
create table blob_size_limit (
    id int, case_name string, image binary, expected_size int, expected_hex string
) using paimon tblproperties (
    'file.format' = 'parquet', 'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true', 'blob-field' = 'image', 'write-only' = 'true'
);
insert into blob_size_limit
select /*+ COALESCE(1) */ * from paimon_blob_seed where id in (1, 2, 11, 12, 13) order by id;

-- Real payloads at and above Doris' default 10 MiB inline limit. Keep them separate
-- from the small fixtures so ordinary payload scans do not fail with the default limit.
-- Store an MD5 instead of a hex copy of each multi-megabyte payload.
drop table if exists blob_large_payload;
create table blob_large_payload (
    id int, case_name string, image binary, expected_size int, expected_md5 string
) using paimon tblproperties (
    'file.format' = 'parquet', 'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true', 'blob-field' = 'image', 'write-only' = 'true'
);
insert into blob_large_payload
select /*+ COALESCE(1) */ id, case_name, image, expected_size, md5(image)
from (
    select id, case_name, encode(repeat('A', expected_size), 'UTF-8') as image, expected_size
    from values
        (1, 'limit_10m_exact',    10485760),
        (2, 'limit_10m_plus_one', 10485761),
        (3, 'payload_11m',       11534336)
    as seed(id, case_name, expected_size)
) payloads
order by id;

-- Two BLOB fields separated by an ordinary column. NULL maps and sizes are independent.
create or replace temporary view paimon_blob_pair_seed as
select id, image, note, thumbnail,
       length(image) as expected_size, hex(image) as expected_hex,
       length(thumbnail) as expected_thumbnail_size, hex(thumbnail) as expected_thumbnail_hex
from values
    (1, cast(null as binary), 'both_null',  cast(null as binary)),
    (2, unhex(''),            'both_empty', unhex('')),
    (3, unhex('68656C6C6F'),  'right_null', cast(null as binary)),
    (4, cast(null as binary), 'left_null',  unhex('776F726C64')),
    (5, unhex('00FF'),        'different',  unhex('68656C6C6F20776F726C64')),
    (6, unhex(''),            'left_empty', unhex('FF0080')),
    (7, unhex('FF0080'),      'right_empty', unhex('')),
    (8, encode(repeat('x', 257), 'UTF-8'), 'reuse_buffer', unhex('01'))
as seed(id, image, note, thumbnail);

drop table if exists blob_multi_columns;
create table blob_multi_columns (
    id int, image binary, note string, thumbnail binary,
    expected_size int, expected_hex string,
    expected_thumbnail_size int, expected_thumbnail_hex string
) using paimon tblproperties (
    'file.format' = 'parquet', 'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true', 'blob-field' = 'image,thumbnail', 'write-only' = 'true'
);
insert into blob_multi_columns
select /*+ COALESCE(1) */ * from paimon_blob_pair_seed order by id;

-- Partition predicates must retain the right original row positions, including NULL partitions.
drop table if exists blob_partitioned;
create table blob_partitioned (
    id int, case_name string, image binary, expected_size int, expected_hex string, region string
) using paimon partitioned by (region) tblproperties (
    'file.format' = 'parquet', 'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true', 'blob-field' = 'image', 'write-only' = 'true'
);
insert into blob_partitioned
select /*+ COALESCE(1) */ *,
       case when id <= 6 then 'a' when id <= 12 then 'b' else cast(null as string) end
from paimon_blob_seed where id <= 13 order by id;

-- Three commits produce nonzero anchor origins. Duplicate SQL ids remain separate append rows.
drop table if exists blob_multi_snapshot;
create table blob_multi_snapshot (
    id int, case_name string, image binary, expected_size int, expected_hex string
) using paimon tblproperties (
    'file.format' = 'parquet', 'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true', 'blob-field' = 'image', 'write-only' = 'true'
);
insert into blob_multi_snapshot
select /*+ COALESCE(1) */ * from paimon_blob_seed where id in (1, 2, 10) order by id;
insert into blob_multi_snapshot
select /*+ COALESCE(1) */ * from paimon_blob_seed where id in (7, 12) order by id;
insert into blob_multi_snapshot values
    (10, 'duplicate_id_new_payload', unhex('6E6577'), 3, '6E6577');

-- More than a default scanner batch, with independently rolling companion files for two fields.
-- Each payload is at most 106 bytes. A single Parquet anchor has a much larger target than BLOBs.
-- Do not assert a fixed file count: verify rollover through $files and descriptor URI changes.
create or replace temporary view paimon_blob_rollover_seed as
select cast(id as int) as id,
       case when id % 17 = 0 then cast(null as binary)
            when id % 17 = 1 then unhex('')
            else encode(concat('row-', lpad(cast(id as string), 5, '0'), ':',
                               repeat('a', cast(id % 97 as int))), 'UTF-8') end as image,
       case when id % 13 = 0 then cast(null as binary)
            else encode(concat('thumb-', cast(id as string)), 'UTF-8') end as thumbnail
from range(0, 4097, 1, 1);

drop table if exists blob_rollover;
create table blob_rollover (
    id int, image binary, thumbnail binary,
    expected_size int, expected_hex string,
    expected_thumbnail_size int, expected_thumbnail_hex string
) using paimon tblproperties (
    'file.format' = 'parquet', 'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true', 'blob-field' = 'image,thumbnail',
    'target-file-size' = '64 mb', 'blob.target-file-size' = '16 kb', 'write-only' = 'true'
);
insert into blob_rollover
select /*+ COALESCE(1) */ id, image, thumbnail,
       length(image), hex(image), length(thumbnail), hex(thumbnail)
from paimon_blob_rollover_seed order by id;

-- Old/new files use different ordinary-column names and BLOB ordinals; field IDs remain stable.
-- Adding an ordinary column also covers NULL defaults in historical Parquet anchors.
drop table if exists blob_schema_evolution;
create table blob_schema_evolution (
    id int, case_name string, image binary, expected_size int, expected_hex string
) using paimon tblproperties (
    'file.format' = 'parquet', 'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true', 'blob-field' = 'image', 'write-only' = 'true'
);
insert into blob_schema_evolution
select /*+ COALESCE(1) */ * from paimon_blob_seed where id in (1, 10) order by id;
-- Paimon 1.4.2 rejects renaming a BLOB column, so rename an ordinary column instead.
alter table blob_schema_evolution rename column case_name to label;
alter table blob_schema_evolution add columns (extra string);
alter table blob_schema_evolution alter column image first;
insert into blob_schema_evolution (image, id, label, expected_size, expected_hex, extra)
select /*+ COALESCE(1) */ image, id, case_name, expected_size, expected_hex, 'after-schema-change'
from paimon_blob_seed where id in (2, 12) order by id;

-- Compaction rewrites ordinary anchors and can leave older BLOB companions in the row-range group.
-- Latest field-bunch selection must not duplicate or lose rows. File counts are not an oracle.
drop table if exists blob_compacted;
create table blob_compacted (
    id int, case_name string, image binary, expected_size int, expected_hex string
) using paimon tblproperties (
    'file.format' = 'parquet', 'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true', 'blob-field' = 'image'
);
insert into blob_compacted
select /*+ COALESCE(1) */ * from paimon_blob_seed where id in (1, 2, 10) order by id;
insert into blob_compacted
select /*+ COALESCE(1) */ * from paimon_blob_seed where id in (7, 12, 13) order by id;
call sys.compact(table => 'blob_compacted');

-- Managed BLOB with an ORC ordinary anchor: native BLOB planning must fall back to JNI.
drop table if exists blob_orc_fallback;
create table blob_orc_fallback (
    id int, case_name string, image binary, expected_size int, expected_hex string
) using paimon tblproperties (
    'file.format' = 'orc', 'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true', 'blob-field' = 'image', 'write-only' = 'true'
);
insert into blob_orc_fallback
select /*+ COALESCE(1) */ * from paimon_blob_seed where id in (1, 2, 7, 10, 12, 13) order by id;

-- Change the READ default only after writing ordinary payload bytes. A relation-level
-- blob-as-descriptor=false override must still restore inline output and its size limit.
drop table if exists blob_descriptor_default;
create table blob_descriptor_default (
    id int, case_name string, image binary, expected_size int, expected_hex string
) using paimon tblproperties (
    'file.format' = 'parquet', 'row-tracking.enabled' = 'true',
    'data-evolution.enabled' = 'true', 'blob-field' = 'image', 'write-only' = 'true'
);
insert into blob_descriptor_default
select /*+ COALESCE(1) */ * from paimon_blob_seed where id in (1, 2, 10) order by id;
alter table blob_descriptor_default set tblproperties ('blob-as-descriptor' = 'true');

drop view paimon_blob_seed;
drop view paimon_blob_pair_seed;
drop view paimon_blob_rollover_seed;
