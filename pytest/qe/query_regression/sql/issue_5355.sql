-- https://github.com/apache/incubator-doris/issues/5355
DROP DATABASE IF EXISTS issue_5355;
CREATE DATABASE issue_5355;
USE issue_5355

create view test_view as SELECT CASE substr(a, 4, 2) WHEN '00' THEN 1 ELSE 2 END AS ts FROM (select 1 as a) t;

DROP DATABASE IF EXISTS issue_5355;
