--https://github.com/apache/incubator-doris/issues/5583
drop database if exists issue_5583
create database issue_5583
use issue_5583
create table test(col1 int, col2 int) distributed by hash(col1);
insert into test values(1, 2);
SELECT col1, IF(GROUPING(`col1`) = 1, COUNT(col1), 0) AS class_count, col2, IF(GROUPING(`col2`) = 1, COUNT(col2), 0) AS school_count FROM test GROUP BY GROUPING SETS((col1, col2), (col1));
SELECT col1, IF(GROUPING(`col1`) = 1, COUNT(col1), 0) AS class_count, col2, IF(GROUPING(`col2`) = 1, COUNT(col2), 0) AS school_count FROM test GROUP BY GROUPING SETS((col1, col2), (col1)) order by 1, 2, 3;
drop database issue_5583;
