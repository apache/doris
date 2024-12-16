create database if not exists multi_catalog;
use multi_catalog;

CREATE TABLE IF NOT EXISTS `test_csv_format_error`(
  `device_id` string COMMENT '设备唯一识别ID ',
  `user_id` bigint COMMENT '设备唯一识别ID  HASH DEVICE_ID ',
  `user_app_id` int COMMENT '使用样本应用的用户Id ',
  `standard_app_id` int COMMENT '标准应用ID ',
  `standard_app_name` string COMMENT '标准应用名称 ',
  `standard_package_name` string COMMENT '标准应用包名 ',
  `device_brand_id` int COMMENT '设备品牌ID ',
  `device_brand_name` string COMMENT '设备品牌名称 ',
  `device_eqp_id` int COMMENT '设备型号ID ',
  `device_eqp_name` string COMMENT '设备型号名称 ',
  `os_version_id` int COMMENT '系统版本ID ',
  `os_version_name` string COMMENT '系统版本名称 ',
  `os_type_id` int COMMENT '操作系统类型ID 0 安卓 1 IOS  ',
  `os_type_name` string COMMENT '操作系统类型名称 0 安卓 1 IOS  ',
  `os_name` string COMMENT '操作系统名称 ',
  `oem_os_version` string COMMENT '厂商封装后的操作系统版本 ',
  `oem_os_name` string COMMENT '厂商封装后的操作系统名称 ',
  `app_version` string COMMENT '样本应用版本 ',
  `app_key` string COMMENT '样本应用key ',
  `app_channel` string COMMENT '推广渠道 ',
  `package_name` string COMMENT '宿主APP包名 ',
  `app_name` string COMMENT '宿主APP名称',
  `sdk_version` string COMMENT 'SDK版本 ',
  `api_level` string COMMENT 'API等级 ',
  `carrier_id` int COMMENT '运营商ID ',
  `carrier_name` string COMMENT '运营商名称 ',
  `phone_num` string COMMENT '手机号码 ',
  `ip` string COMMENT 'IP地址 ',
  `country_id` int COMMENT '国家id',
  `country_name` string COMMENT '国家name',
  `province_id` int COMMENT '省份ID ',
  `province_name` string COMMENT '省份名称 ',
  `city_id` int COMMENT '地级市ID ',
  `city_name` string COMMENT '地级市名称 ',
  `county_id` int COMMENT '县级市ID ',
  `county_name` string COMMENT '县级市名称 ',
  `mac_address` string COMMENT 'MAC地址 ',
  `network_id` int COMMENT '网络类型ID ',
  `network_name` string COMMENT '网络类型 ',
  `org_package_name` string COMMENT '原始应用包名 ',
  `org_app_name` string COMMENT '原始应用程序名 ',
  `org_app_version` string COMMENT '原始应用版本 ',
  `app_flag` int COMMENT '安装、更新、还是卸载',
  `action_time` string COMMENT '行为发生的时间',
  `day_realy` string COMMENT '行为发生的日期',
  `memo` map<string,string> COMMENT '备注')
COMMENT 'ods-App_Installed'
PARTITIONED BY (
  `day` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'colelction.delim'=',',
  'field.delim'='\t',
  'mapkey.delim'=':',
  'serialization.format'='\t')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/test_csv_format_error';

msck repair table test_csv_format_error;