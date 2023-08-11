-- https://github.com/apache/incubator-doris/issues/4313
DROP DATABASE IF EXISTS issue_4313;
CREATE DATABASE issue_4313;
USE issue_4313

CREATE TABLE `click_show_window` ( `event_day` datetime NULL COMMENT "",  `title` varchar(600) NULL COMMENT "",  `report_value` varchar(50) replace NULL COMMENT "") ENGINE=OLAP DISTRIBUTED BY HASH(`event_day`) BUCKETS 5;

SELECT * FROM (SELECT DISTINCT event_day, title FROM click_show_window) a WHERE a.title IS NOT NULL;

DROP DATABASE IF EXISTS issue_4313;
