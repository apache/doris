
use `multi_catalog`;

-- lzo-hadoop (org.anarres) mapreduce-API LZO text InputFormat
-- Class is provided by auxlib/lzo-hadoop-1.0.6.jar (cp to /opt/hive/lib on boot)
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
    INPUTFORMAT  'com.hadoop.mapreduce.LzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/preinstalled_data/text_lzo';

-- lzo-hadoop (org.anarres) legacy mapred-API wrapper, same file format
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
