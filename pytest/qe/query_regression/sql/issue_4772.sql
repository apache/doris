-- https://github.com/apache/incubator-doris/issues/4772
drop database if exists issue_4772

create database issue_4772
use issue_4772
-- create table
DROP TABLE IF EXISTS `test_t`
CREATE TABLE `test_t` (`name` VARCHAR(1000) NULL COMMENT "string") DUPLICATE KEY(`name`) DISTRIBUTED BY HASH(`name`)  PROPERTIES ("replication_num" = "1","in_memory" = "false","storage_format" = "DEFAULT")

-- insert data
INSERT INTO test_t (`name`) VALUES ('aaaaa'), ('ccccc')

SELECT `t1`.`name` FROM ( SELECT `name` FROM `test_t` GROUP BY `name` ) `t1` UNION ALL SELECT `t2`.`name` FROM ( SELECT `name`, -1 AS `bool` FROM `test_t` GROUP BY `name` ) `t2` WHERE `t2`.`bool` = 1

SELECT `t2`.`name` FROM ( SELECT `name`, -1 AS `bool` FROM `test_t` GROUP BY `name` ) `t2` WHERE `t2`.`bool` = 1 UNION ALL SELECT `t1`.`name` FROM ( SELECT `name` FROM `test_t` GROUP BY `name` ) `t1`

drop database issue_4772
