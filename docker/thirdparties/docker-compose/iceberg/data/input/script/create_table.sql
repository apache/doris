
CREATE TABLE demo.format_v1.sample_parquet (
    id bigint COMMENT 'unique id',
    col_boolean boolean,
    col_short short,
    col_byte byte,
    col_integer integer,
    col_long long,
    col_float float,
    col_double double,
    col_date date,
    col_timestamp timestamp,
    col_timestamp_ntz timestamp_ntz,
    col_char char(20),
    col_varchar varchar(128),
    col_string string,
    col_binary binary,
    col_decimal decimal(12, 6),
    city string
    )
USING iceberg
PARTITIONED BY (days(col_timestamp), city, bucket(2, id))
TBLPROPERTIES (
    'format-version' = '1',
    'write.format.default' = 'parquet'
);

CREATE TABLE demo.format_v1.sample_orc (
    id bigint COMMENT 'unique id',
    col_boolean boolean,
    col_short short,
    col_byte byte,
    col_integer integer,
    col_long long,
    col_float float,
    col_double double,
    col_date date,
    col_timestamp timestamp,
    col_timestamp_ntz timestamp_ntz,
    col_char char(20),
    col_varchar varchar(128),
    col_string string,
    col_binary binary,
    col_decimal decimal(12, 6),
    city string
    )
USING iceberg
PARTITIONED BY (days(col_timestamp), city, bucket(2, id))
TBLPROPERTIES (
    'format-version' = '1',
    'write.format.default' = 'orc'
);



CREATE TABLE demo.format_v2.sample_cow_parquet (
    id bigint COMMENT 'unique id',
    col_boolean boolean,
    col_short short,
    col_byte byte,
    col_integer integer,
    col_long long,
    col_float float,
    col_double double,
    col_date date,
    col_timestamp timestamp,
    col_timestamp_ntz timestamp_ntz,
    col_char char(20),
    col_varchar varchar(128),
    col_string string,
    col_binary binary,
    col_decimal decimal(12, 6),
    city string
    )
USING iceberg
PARTITIONED BY (days(col_timestamp), city, bucket(2, id))
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write',
    'write.format.default' = 'parquet'
);


CREATE TABLE demo.format_v2.sample_cow_orc (
    id bigint COMMENT 'unique id',
    col_boolean boolean,
    col_short short,
    col_byte byte,
    col_integer integer,
    col_long long,
    col_float float,
    col_double double,
    col_date date,
    col_timestamp timestamp,
    col_timestamp_ntz timestamp_ntz,
    col_char char(20),
    col_varchar varchar(128),
    col_string string,
    col_binary binary,
    col_decimal decimal(12, 6),
    city string
    )
USING iceberg
PARTITIONED BY (days(col_timestamp), city, bucket(2, id))
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write',
    'write.format.default' = 'orc'
);


CREATE TABLE demo.format_v2.sample_mor_parquet (
    id bigint COMMENT 'unique id',
    col_boolean boolean,
    col_short short,
    col_byte byte,
    col_integer integer,
    col_long long,
    col_float float,
    col_double double,
    col_date date,
    col_timestamp timestamp,
    col_timestamp_ntz timestamp_ntz,
    col_char char(20),
    col_varchar varchar(128),
    col_string string,
    col_binary binary,
    col_decimal decimal(12, 6),
    city string
    )
USING iceberg
PARTITIONED BY (days(col_timestamp), city, bucket(2, id))
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read',
    'write.format.default' = 'parquet'
);


CREATE TABLE demo.format_v2.sample_mor_orc (
    id bigint COMMENT 'unique id',
    col_boolean boolean,
    col_short short,
    col_byte byte,
    col_integer integer,
    col_long long,
    col_float float,
    col_double double,
    col_date date,
    col_timestamp timestamp,
    col_timestamp_ntz timestamp_ntz,
    col_char char(20),
    col_varchar varchar(128),
    col_string string,
    col_binary binary,
    col_decimal decimal(12, 6),
    city string
    )
USING iceberg
PARTITIONED BY (days(col_timestamp), city, bucket(2, id))
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read',
    'write.format.default' = 'orc'
);

