-- https://github.com/apache/incubator-doris/issues/7875
DROP DATABASE IF EXISTS issue_7875
CREATE DATABASE issue_7875;
use issue_7875
CREATE TABLE dt(k datetime) DISTRIBUTED BY HASH(k);
INSERT INTO dt VALUES("2022-01-03 10:22:33");
SELECT * FROM dt;
DROP DATABASE issue_7875;
