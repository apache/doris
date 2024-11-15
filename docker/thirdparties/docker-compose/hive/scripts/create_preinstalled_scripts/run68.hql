use `default`;

-- user case for a table with en empty lzo file
CREATE TABLE `user_empty_lzo`(
  `object_id` string COMMENT '对象ID',
  `s_info_compname` string COMMENT '公司中文名称',
  `ann_dt` string COMMENT '评级日期',
  `b_rate_style` string COMMENT 'null',
  `b_info_creditrating` string COMMENT 'null',
  `b_rate_ratingoutlook` bigint COMMENT 'null',
  `b_info_creditratingagency` string COMMENT '评级机构代码',
  `s_info_compcode` string COMMENT '债券主体公司id',
  `b_info_creditratingexplain` string COMMENT '信用评级说明',
  `b_info_precreditrating` string COMMENT '前次信用评级',
  `b_creditrating_change` string COMMENT '评级变动方向',
  `b_info_issuerratetype` bigint COMMENT '评级对象类型代码',
  `ann_dt2` string COMMENT '公告日期',
  `opdate` timestamp COMMENT 'null',
  `opmode` string COMMENT 'null')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/preinstalled_data/user_empty_lzo';

msck repair table user_empty_lzo;
