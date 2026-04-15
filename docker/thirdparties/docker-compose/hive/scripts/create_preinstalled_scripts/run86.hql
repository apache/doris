
use `multi_catalog`;

-- LzoTextInputFormat: primary LZO text InputFormat from hadoop-lzo
CREATE TABLE text_lzo_format (
    id      INT,
    value   INT,
    name    STRING,
    score   DOUBLE,
    dt      DATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS
    INPUTFORMAT  'com.hadoop.compression.lzo.LzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/preinstalled_data/text_lzo';

-- DeprecatedLzoTextInputFormat: legacy mapred-API wrapper, same file format
CREATE TABLE text_deprecated_lzo_format (
    id      INT,
    value   INT,
    name    STRING,
    score   DOUBLE,
    dt      DATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS
    INPUTFORMAT  'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/preinstalled_data/text_lzo';
