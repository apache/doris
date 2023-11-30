-- https://github.com/apache/incubator-doris/issues/4806
drop database if exists issue_4806

create database issue_4806
use issue_4806
-- create table
DROP TABLE IF EXISTS `invalid_date`

CREATE TABLE invalid_date(date datetime NULL COMMENT "", day date NULL COMMENT "", site_id int(11) NULL COMMENT "") DUPLICATE KEY(date, day, site_id) COMMENT "OLAP" PARTITION BY RANGE(day) (PARTITION p20201030 VALUES [('2020-10-30'), ('2020-10-31'))) DISTRIBUTED BY HASH(site_id) PROPERTIES ("replication_num" = "1", "in_memory" = "false", "storage_format" = "V2" )

insert into invalid_date values ('2020-06-25 00:16:23', '2020-06-25', 1)
select date from invalid_date where day in (20201030)

insert into invalid_date values ('2020-10-30 00:19:23', '2020-10-30', 1)
select date from invalid_date where day in (20201030)

drop database issue_4806
