create database if not exists schema_change;
use schema_change;

CREATE TABLE IF NOT EXISTS parquet_primitive_types_to_boolean (
    id INT,
    bool_col BOOLEAN,
    int_col BOOLEAN,
    smallint_col BOOLEAN,
    tinyint_col BOOLEAN,
    bigint_col BOOLEAN,
    float_col BOOLEAN,
    double_col BOOLEAN,
    string_col BOOLEAN,
    char1_col BOOLEAN,
    char2_col BOOLEAN,
    varchar_col BOOLEAN,
    date_col BOOLEAN,
    timestamp_col BOOLEAN,
    decimal1_col BOOLEAN,
    decimal2_col BOOLEAN
) STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/parquet_table/parquet_schema_change';

CREATE TABLE IF NOT EXISTS parquet_primitive_types_to_bigint (
    id INT,
    bool_col BIGINT,
    int_col BIGINT,
    smallint_col BIGINT,
    tinyint_col BIGINT,
    bigint_col BIGINT,
    float_col BIGINT,
    double_col BIGINT,
    string_col BIGINT,
    char1_col BIGINT,
    char2_col BIGINT,
    varchar_col BIGINT,
    date_col BIGINT,
    timestamp_col BIGINT,
    decimal1_col BIGINT,
    decimal2_col BIGINT
) STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/parquet_table/parquet_schema_change';

CREATE TABLE IF NOT EXISTS parquet_primitive_types_to_int (
    id INT,
    bool_col INT,
    int_col INT,
    smallint_col INT,
    tinyint_col INT,
    bigint_col INT,
    float_col INT,
    double_col INT,
    string_col INT,
    char1_col INT,
    char2_col INT,
    varchar_col INT,
    date_col INT,
    timestamp_col INT,
    decimal1_col INT,
    decimal2_col INT
) STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/parquet_table/parquet_schema_change';

CREATE TABLE IF NOT EXISTS parquet_primitive_types_to_smallint (
    id INT,
    bool_col SMALLINT,
    int_col SMALLINT,
    smallint_col SMALLINT,
    tinyint_col SMALLINT,
    bigint_col SMALLINT,
    float_col SMALLINT,
    double_col SMALLINT,
    string_col SMALLINT,
    char1_col SMALLINT,
    char2_col SMALLINT,
    varchar_col SMALLINT,
    date_col SMALLINT,
    timestamp_col SMALLINT,
    decimal1_col SMALLINT,
    decimal2_col SMALLINT
) STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/parquet_table/parquet_schema_change';

CREATE TABLE IF NOT EXISTS parquet_primitive_types_to_tinyint (
    id INT,
    bool_col TINYINT,
    int_col TINYINT,
    smallint_col TINYINT,
    tinyint_col TINYINT,
    bigint_col TINYINT,
    float_col TINYINT,
    double_col TINYINT,
    string_col TINYINT,
    char1_col TINYINT,
    char2_col TINYINT,
    varchar_col TINYINT,
    date_col TINYINT,
    timestamp_col TINYINT,
    decimal1_col TINYINT,
    decimal2_col TINYINT
) STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/parquet_table/parquet_schema_change';

CREATE TABLE IF NOT EXISTS parquet_primitive_types_to_float (
    id INT,
    bool_col FLOAT,
    int_col FLOAT,
    smallint_col FLOAT,
    tinyint_col FLOAT,
    bigint_col FLOAT,
    float_col FLOAT,
    double_col FLOAT,
    string_col FLOAT,
    char1_col FLOAT,
    char2_col FLOAT,
    varchar_col FLOAT,
    date_col FLOAT,
    timestamp_col FLOAT,
    decimal1_col FLOAT,
    decimal2_col FLOAT
) STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/parquet_table/parquet_schema_change';

CREATE TABLE IF NOT EXISTS parquet_primitive_types_to_double (
    id INT,
    bool_col DOUBLE,
    int_col DOUBLE,
    smallint_col DOUBLE,
    tinyint_col DOUBLE,
    bigint_col DOUBLE,
    float_col DOUBLE,
    double_col DOUBLE,
    string_col DOUBLE,
    char1_col DOUBLE,
    char2_col DOUBLE,
    varchar_col DOUBLE,
    date_col DOUBLE,
    timestamp_col DOUBLE,
    decimal1_col DOUBLE,
    decimal2_col DOUBLE
) STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/parquet_table/parquet_schema_change';

CREATE TABLE IF NOT EXISTS parquet_primitive_types_to_string (
    id INT,
    bool_col STRING,
    int_col STRING,
    smallint_col STRING,
    tinyint_col STRING,
    bigint_col STRING,
    float_col STRING,
    double_col STRING,
    string_col STRING,
    char1_col STRING,
    char2_col STRING,
    varchar_col STRING,
    date_col STRING,
    timestamp_col STRING,
    decimal1_col STRING,
    decimal2_col STRING
) STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/parquet_table/parquet_schema_change';

CREATE TABLE IF NOT EXISTS parquet_primitive_types_to_date (
    id INT,
    bool_col DATE,
    int_col DATE,
    smallint_col DATE,
    tinyint_col DATE,
    bigint_col DATE,
    float_col DATE,
    double_col DATE,
    string_col DATE,
    char1_col DATE,
    char2_col DATE,
    varchar_col DATE,
    date_col DATE,
    timestamp_col DATE,
    decimal1_col DATE,
    decimal2_col DATE
) STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/parquet_table/parquet_schema_change';

CREATE TABLE IF NOT EXISTS parquet_primitive_types_to_timestamp (
    id INT,
    bool_col TIMESTAMP,
    int_col TIMESTAMP,
    smallint_col TIMESTAMP,
    tinyint_col TIMESTAMP,
    bigint_col TIMESTAMP,
    float_col TIMESTAMP,
    double_col TIMESTAMP,
    string_col TIMESTAMP,
    char1_col TIMESTAMP,
    char2_col TIMESTAMP,
    varchar_col TIMESTAMP,
    date_col TIMESTAMP,
    timestamp_col TIMESTAMP,
    decimal1_col TIMESTAMP,
    decimal2_col TIMESTAMP
) STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/parquet_table/parquet_schema_change';


CREATE TABLE IF NOT EXISTS parquet_primitive_types_to_decimal1 (
    id INT,
    bool_col DECIMAL(20,5),
    int_col DECIMAL(20,5),
    smallint_col DECIMAL(20,5),
    tinyint_col DECIMAL(20,5),
    bigint_col DECIMAL(20,5),
    float_col DECIMAL(20,5),
    double_col DECIMAL(20,5),
    string_col DECIMAL(20,5),
    char1_col DECIMAL(20,5),
    char2_col DECIMAL(20,5),
    varchar_col DECIMAL(20,5),
    date_col DECIMAL(20,5),
    timestamp_col DECIMAL(20,5),
    decimal1_col DECIMAL(20,5),
    decimal2_col DECIMAL(20,5)
) STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/parquet_table/parquet_schema_change';

CREATE TABLE IF NOT EXISTS parquet_primitive_types_to_decimal2 (
    id INT,
    bool_col DECIMAL(7,1),
    int_col DECIMAL(7,1),
    smallint_col DECIMAL(7,1),
    tinyint_col DECIMAL(7,1),
    bigint_col DECIMAL(7,1),
    float_col DECIMAL(7,1),
    double_col DECIMAL(7,1),
    string_col DECIMAL(7,1),
    char1_col DECIMAL(7,1),
    char2_col DECIMAL(7,1),
    varchar_col DECIMAL(7,1),
    date_col DECIMAL(7,1),
    timestamp_col DECIMAL(7,1),
    decimal1_col DECIMAL(7,1),
    decimal2_col DECIMAL(7,1)
) STORED AS PARQUET
LOCATION '/user/doris/preinstalled_data/parquet_table/parquet_schema_change';




CREATE TABLE IF NOT EXISTS orc_primitive_types_to_boolean (
    id INT,
    bool_col BOOLEAN,
    int_col BOOLEAN,
    smallint_col BOOLEAN,
    tinyint_col BOOLEAN,
    bigint_col BOOLEAN,
    float_col BOOLEAN,
    double_col BOOLEAN,
    string_col BOOLEAN,
    char1_col BOOLEAN,
    char2_col BOOLEAN,
    varchar_col BOOLEAN,
    date_col BOOLEAN,
    timestamp_col BOOLEAN,
    decimal1_col BOOLEAN,
    decimal2_col BOOLEAN
) STORED AS orc
LOCATION '/user/doris/preinstalled_data/orc_table/orc_schema_change';


CREATE TABLE IF NOT EXISTS orc_primitive_types_to_bigint (
    id INT,
    bool_col BIGINT,
    int_col BIGINT,
    smallint_col BIGINT,
    tinyint_col BIGINT,
    bigint_col BIGINT,
    float_col BIGINT,
    double_col BIGINT,
    string_col BIGINT,
    char1_col BIGINT,
    char2_col BIGINT,
    varchar_col BIGINT,
    date_col BIGINT,
    timestamp_col BIGINT,
    decimal1_col BIGINT,
    decimal2_col BIGINT
) STORED AS orc
LOCATION '/user/doris/preinstalled_data/orc_table/orc_schema_change';

CREATE TABLE IF NOT EXISTS orc_primitive_types_to_int (
    id INT,
    bool_col INT,
    int_col INT,
    smallint_col INT,
    tinyint_col INT,
    bigint_col INT,
    float_col INT,
    double_col INT,
    string_col INT,
    char1_col INT,
    char2_col INT,
    varchar_col INT,
    date_col INT,
    timestamp_col INT,
    decimal1_col INT,
    decimal2_col INT
) STORED AS orc
LOCATION '/user/doris/preinstalled_data/orc_table/orc_schema_change';

CREATE TABLE IF NOT EXISTS orc_primitive_types_to_smallint (
    id INT,
    bool_col SMALLINT,
    int_col SMALLINT,
    smallint_col SMALLINT,
    tinyint_col SMALLINT,
    bigint_col SMALLINT,
    float_col SMALLINT,
    double_col SMALLINT,
    string_col SMALLINT,
    char1_col SMALLINT,
    char2_col SMALLINT,
    varchar_col SMALLINT,
    date_col SMALLINT,
    timestamp_col SMALLINT,
    decimal1_col SMALLINT,
    decimal2_col SMALLINT
) STORED AS orc
LOCATION '/user/doris/preinstalled_data/orc_table/orc_schema_change';

CREATE TABLE IF NOT EXISTS orc_primitive_types_to_tinyint (
    id INT,
    bool_col TINYINT,
    int_col TINYINT,
    smallint_col TINYINT,
    tinyint_col TINYINT,
    bigint_col TINYINT,
    float_col TINYINT,
    double_col TINYINT,
    string_col TINYINT,
    char1_col TINYINT,
    char2_col TINYINT,
    varchar_col TINYINT,
    date_col TINYINT,
    timestamp_col TINYINT,
    decimal1_col TINYINT,
    decimal2_col TINYINT
) STORED AS orc
LOCATION '/user/doris/preinstalled_data/orc_table/orc_schema_change';

CREATE TABLE IF NOT EXISTS orc_primitive_types_to_float (
    id INT,
    bool_col FLOAT,
    int_col FLOAT,
    smallint_col FLOAT,
    tinyint_col FLOAT,
    bigint_col FLOAT,
    float_col FLOAT,
    double_col FLOAT,
    string_col FLOAT,
    char1_col FLOAT,
    char2_col FLOAT,
    varchar_col FLOAT,
    date_col FLOAT,
    timestamp_col FLOAT,
    decimal1_col FLOAT,
    decimal2_col FLOAT
) STORED AS orc
LOCATION '/user/doris/preinstalled_data/orc_table/orc_schema_change';

CREATE TABLE IF NOT EXISTS orc_primitive_types_to_double (
    id INT,
    bool_col DOUBLE,
    int_col DOUBLE,
    smallint_col DOUBLE,
    tinyint_col DOUBLE,
    bigint_col DOUBLE,
    float_col DOUBLE,
    double_col DOUBLE,
    string_col DOUBLE,
    char1_col DOUBLE,
    char2_col DOUBLE,
    varchar_col DOUBLE,
    date_col DOUBLE,
    timestamp_col DOUBLE,
    decimal1_col DOUBLE,
    decimal2_col DOUBLE
) STORED AS orc
LOCATION '/user/doris/preinstalled_data/orc_table/orc_schema_change';

CREATE TABLE IF NOT EXISTS orc_primitive_types_to_string (
    id INT,
    bool_col STRING,
    int_col STRING,
    smallint_col STRING,
    tinyint_col STRING,
    bigint_col STRING,
    float_col STRING,
    double_col STRING,
    string_col STRING,
    char1_col STRING,
    char2_col STRING,
    varchar_col STRING,
    date_col STRING,
    timestamp_col STRING,
    decimal1_col STRING,
    decimal2_col STRING
) STORED AS orc
LOCATION '/user/doris/preinstalled_data/orc_table/orc_schema_change';

CREATE TABLE IF NOT EXISTS orc_primitive_types_to_date (
    id INT,
    bool_col DATE,
    int_col DATE,
    smallint_col DATE,
    tinyint_col DATE,
    bigint_col DATE,
    float_col DATE,
    double_col DATE,
    string_col DATE,
    char1_col DATE,
    char2_col DATE,
    varchar_col DATE,
    date_col DATE,
    timestamp_col DATE,
    decimal1_col DATE,
    decimal2_col DATE
) STORED AS orc
LOCATION '/user/doris/preinstalled_data/orc_table/orc_schema_change';

CREATE TABLE IF NOT EXISTS orc_primitive_types_to_timestamp (
    id INT,
    bool_col TIMESTAMP,
    int_col TIMESTAMP,
    smallint_col TIMESTAMP,
    tinyint_col TIMESTAMP,
    bigint_col TIMESTAMP,
    float_col TIMESTAMP,
    double_col TIMESTAMP,
    string_col TIMESTAMP,
    char1_col TIMESTAMP,
    char2_col TIMESTAMP,
    varchar_col TIMESTAMP,
    date_col TIMESTAMP,
    timestamp_col TIMESTAMP,
    decimal1_col TIMESTAMP,
    decimal2_col TIMESTAMP
) STORED AS orc
LOCATION '/user/doris/preinstalled_data/orc_table/orc_schema_change';


CREATE TABLE IF NOT EXISTS orc_primitive_types_to_decimal1 (
    id INT,
    bool_col DECIMAL(20,5),
    int_col DECIMAL(20,5),
    smallint_col DECIMAL(20,5),
    tinyint_col DECIMAL(20,5),
    bigint_col DECIMAL(20,5),
    float_col DECIMAL(20,5),
    double_col DECIMAL(20,5),
    string_col DECIMAL(20,5),
    char1_col DECIMAL(20,5),
    char2_col DECIMAL(20,5),
    varchar_col DECIMAL(20,5),
    date_col DECIMAL(20,5),
    timestamp_col DECIMAL(20,5),
    decimal1_col DECIMAL(20,5),
    decimal2_col DECIMAL(20,5)
) STORED AS orc
LOCATION '/user/doris/preinstalled_data/orc_table/orc_schema_change';

CREATE TABLE IF NOT EXISTS orc_primitive_types_to_decimal2 (
    id INT,
    bool_col DECIMAL(7,1),
    int_col DECIMAL(7,1),
    smallint_col DECIMAL(7,1),
    tinyint_col DECIMAL(7,1),
    bigint_col DECIMAL(7,1),
    float_col DECIMAL(7,1),
    double_col DECIMAL(7,1),
    string_col DECIMAL(7,1),
    char1_col DECIMAL(7,1),
    char2_col DECIMAL(7,1),
    varchar_col DECIMAL(7,1),
    date_col DECIMAL(7,1),
    timestamp_col DECIMAL(7,1),
    decimal1_col DECIMAL(7,1),
    decimal2_col DECIMAL(7,1)
) STORED AS orc
LOCATION '/user/doris/preinstalled_data/orc_table/orc_schema_change';


MSCK REPAIR TABLE parquet_primitive_types_to_boolean;
MSCK REPAIR TABLE parquet_primitive_types_to_bigint;
MSCK REPAIR TABLE parquet_primitive_types_to_int;
MSCK REPAIR TABLE parquet_primitive_types_to_smallint;
MSCK REPAIR TABLE parquet_primitive_types_to_tinyint;
MSCK REPAIR TABLE parquet_primitive_types_to_float;
MSCK REPAIR TABLE parquet_primitive_types_to_double;
MSCK REPAIR TABLE parquet_primitive_types_to_string;
MSCK REPAIR TABLE parquet_primitive_types_to_date;
MSCK REPAIR TABLE parquet_primitive_types_to_timestamp;
MSCK REPAIR TABLE parquet_primitive_types_to_decimal1;
MSCK REPAIR TABLE parquet_primitive_types_to_decimal2;

MSCK REPAIR TABLE orc_primitive_types_to_boolean;
MSCK REPAIR TABLE orc_primitive_types_to_bigint;
MSCK REPAIR TABLE orc_primitive_types_to_int;
MSCK REPAIR TABLE orc_primitive_types_to_smallint;
MSCK REPAIR TABLE orc_primitive_types_to_tinyint;
MSCK REPAIR TABLE orc_primitive_types_to_float;
MSCK REPAIR TABLE orc_primitive_types_to_double;
MSCK REPAIR TABLE orc_primitive_types_to_string;
MSCK REPAIR TABLE orc_primitive_types_to_date;
MSCK REPAIR TABLE orc_primitive_types_to_timestamp;
MSCK REPAIR TABLE orc_primitive_types_to_decimal1;
MSCK REPAIR TABLE orc_primitive_types_to_decimal2;