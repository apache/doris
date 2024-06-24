create database if not exists multi_catalog;
use multi_catalog;

CREATE TABLE `test_compress_partitioned`(
  `watchid` string,
  `javaenable` smallint,
  `title` string,
  `goodevent` smallint,
  `eventtime` timestamp,
  `eventdate` date,
  `counterid` bigint,
  `clientip` bigint,
  `clientip6` char(50),
  `regionid` bigint,
  `userid` string,
  `counterclass` tinyint,
  `os` smallint,
  `useragent` smallint,
  `url` string,
  `referer` string,
  `urldomain` string,
  `refererdomain` string,
  `refresh` smallint,
  `isrobot` smallint,
  `referercategories` string,
  `urlcategories` string,
  `urlregions` string,
  `refererregions` string,
  `resolutionwidth` int,
  `resolutionheight` int,
  `resolutiondepth` smallint,
  `flashmajor` smallint,
  `flashminor` smallint,
  `flashminor2` string,
  `netmajor` smallint,
  `netminor` smallint,
  `useragentmajor` int,
  `useragentminor` char(4),
  `cookieenable` smallint,
  `javascriptenable` smallint,
  `ismobile` smallint,
  `mobilephone` smallint,
  `mobilephonemodel` string,
  `params` string,
  `ipnetworkid` bigint,
  `traficsourceid` tinyint,
  `searchengineid` int,
  `searchphrase` string,
  `advengineid` smallint,
  `isartifical` smallint,
  `windowclientwidth` int,
  `windowclientheight` int,
  `clienttimezone` smallint,
  `clienteventtime` timestamp,
  `silverlightversion1` smallint,
  `silverlightversion2` smallint,
  `silverlightversion3` bigint,
  `silverlightversion4` int,
  `pagecharset` string,
  `codeversion` bigint,
  `islink` smallint,
  `isdownload` smallint,
  `isnotbounce` smallint,
  `funiqid` string,
  `hid` bigint,
  `isoldcounter` smallint,
  `isevent` smallint,
  `isparameter` smallint,
  `dontcounthits` smallint,
  `withhash` smallint,
  `hitcolor` char(2),
  `utceventtime` timestamp,
  `age` smallint,
  `sex` smallint,
  `income` smallint,
  `interests` int,
  `robotness` smallint,
  `generalinterests` string,
  `remoteip` bigint,
  `remoteip6` char(50),
  `windowname` int,
  `openername` int,
  `historylength` smallint,
  `browserlanguage` char(4),
  `browsercountry` char(4),
  `socialnetwork` string,
  `socialaction` string,
  `httperror` int,
  `sendtiming` int,
  `dnstiming` int,
  `connecttiming` int,
  `responsestarttiming` int,
  `responseendtiming` int,
  `fetchtiming` int,
  `redirecttiming` int,
  `dominteractivetiming` int,
  `domcontentloadedtiming` int,
  `domcompletetiming` int,
  `loadeventstarttiming` int,
  `loadeventendtiming` int,
  `nstodomcontentloadedtiming` int,
  `firstpainttiming` int,
  `redirectcount` tinyint,
  `socialsourcenetworkid` smallint,
  `socialsourcepage` string,
  `paramprice` bigint,
  `paramorderid` string,
  `paramcurrency` char(6),
  `paramcurrencyid` int,
  `goalsreached` string,
  `openstatservicename` string,
  `openstatcampaignid` string,
  `openstatadid` string,
  `openstatsourceid` string,
  `utmsource` string,
  `utmmedium` string,
  `utmcampaign` string,
  `utmcontent` string,
  `utmterm` string,
  `fromtag` string,
  `hasgclid` smallint,
  `refererhash` string,
  `urlhash` string,
  `clid` bigint,
  `yclid` string,
  `shareservice` string,
  `shareurl` string,
  `sharetitle` string,
  `parsedparamskey1` string,
  `parsedparamskey2` string,
  `parsedparamskey3` string,
  `parsedparamskey4` string,
  `parsedparamskey5` string,
  `parsedparamsvaluedouble` double,
  `islandid` char(40),
  `requestnum` bigint,
  `requesttry` smallint)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/doris/suites/test_hive_compress_type/test_compress_partitioned'
TBLPROPERTIES (
  'transient_lastDdlTime'='1692589095');

CREATE TABLE `parquet_lz4_compression`(
  `col_int` int,
  `col_smallint` smallint,
  `col_tinyint` tinyint,
  `col_bigint` bigint,
  `col_float` float,
  `col_double` double,
  `col_boolean` boolean,
  `col_string` string,
  `col_char` char(10),
  `col_varchar` varchar(25),
  `col_date` date,
  `col_timestamp` timestamp,
  `col_decimal` decimal(10,2))
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/suites/test_hive_compress_type/parquet_lz4_compression'
TBLPROPERTIES (
  'parquet.compression'='LZ4',
  'transient_lastDdlTime'='1700723950');

CREATE TABLE `parquet_lzo_compression`(
  `col_int` int,
  `col_smallint` smallint,
  `col_tinyint` tinyint,
  `col_bigint` bigint,
  `col_float` float,
  `col_double` double,
  `col_boolean` boolean,
  `col_string` string,
  `col_char` char(10),
  `col_varchar` varchar(25),
  `col_date` date,
  `col_timestamp` timestamp,
  `col_decimal` decimal(10,2))
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/suites/test_hive_compress_type/parquet_lzo_compression'
TBLPROPERTIES (
  'parquet.compression'='LZO',
  'transient_lastDdlTime'='1701173147');

msck repair table test_compress_partitioned;
msck repair table parquet_lz4_compression;
msck repair table parquet_lzo_compression;

