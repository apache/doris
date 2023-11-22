-- https://github.com/apache/incubator-doris/issues/5138
DROP DATABASE IF EXISTS issue_5138;
CREATE DATABASE issue_5138;
USE issue_5138

CREATE TABLE `test` ( `date` date, `userid` varchar(128)) ENGINE=OLAP DUPLICATE KEY(`date`, `userid`) PARTITION BY RANGE(`date`)() DISTRIBUTED BY HASH(`userid`) BUCKETS 8 PROPERTIES ( "colocate_with" = "test");

DROP DATABASE IF EXISTS issue_5138;
