-- https://github.com/apache/incubator-doris/issues/4040
DROP DATABASE IF EXISTS issue_4040;
CREATE DATABASE issue_4040;
USE issue_4040

CREATE TABLE `a` ( `col1` tinyint(4) NULL COMMENT "",  `col2` char(6) NULL COMMENT "",  `col3` char(11) NULL COMMENT "",  `col4` int NULL COMMENT "", pv int sum NULL) ENGINE=OLAP DISTRIBUTED BY HASH(`col1`) BUCKETS 5;

insert into a values(1, 'ALL', 'foo', 1, 1), (1, null, 'foo', 1, 1);

SELECT IF(col2 IS NULL, 'ALL', col2) AS col2, IF(col3 IS NULL, 'ALL', col3) AS col3, pv FROM ( SELECT col1, col2, col3, SUM(pv) AS pv FROM  a WHERE col1 = 1 AND col4 = 1 AND col3 = 'foo'  GROUP BY GROUPING SETS ((col1), (col1, col2), (col1, col3), (col1, col2, col3))) t WHERE IF(col2 IS NULL, 'ALL', col2) = 'ALL' order by col2, col3, pv;

DROP DATABASE IF EXISTS issue_4040;
