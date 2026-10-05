-- https://github.com/apache/incubator-doris/issues/4241
drop database if exists issue_4241;

create database issue_4241;
use issue_4241
CREATE VIEW test_view AS SELECT NULL AS k UNION ALL SELECT NULL AS k;
EXPLAIN SELECT v1.k FROM test_view v1 LEFT JOIN test_view v2 ON v1.k = v2.k; #IGNORE CHECK
EXPLAIN WITH test_view (k) AS ( SELECT NULL AS k UNION ALL SELECT NULL AS k ) SELECT v1.k FROM test_view v1 LEFT JOIN test_view v2 ON v1.k = v2.k; #IGNORE CHECK
SELECT v1.k FROM test_view v1 LEFT JOIN test_view v2 ON v1.k = v2.k;
WITH test_view (k) AS ( SELECT NULL AS k UNION ALL SELECT NULL AS k ) SELECT v1.k FROM test_view v1 LEFT JOIN test_view v2 ON v1.k = v2.k;

drop database issue_4241;
