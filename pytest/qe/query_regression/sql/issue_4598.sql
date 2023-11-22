-- https://github.com/apache/incubator-doris/issues/4598
drop database if exists issue_4598
drop database if exists issue_4598_bak

create database issue_4598
use issue_4598
CREATE TABLE `afo_app_gpu_resource` ( `collect_day` date NOT NULL COMMENT "", `collect_min` char(20) NOT NULL COMMENT "", `app_id` varchar(50) NOT NULL COMMENT "", `group_name` varchar(100) NOT NULL COMMENT "", `queue_name` varchar(200) NOT NULL COMMENT "", `app_name` varchar(300) NULL COMMENT "", `app_type` varchar(50) NULL COMMENT "", `app_mode` varchar(20) NULL COMMENT "", `res_type` varchar(20) NULL , `res_schedule` double SUM NULL COMMENT "", `res_used` double SUM NULL COMMENT "", `res_util` double SUM NULL COMMENT "") ENGINE=OLAP AGGREGATE KEY(`collect_day`, `collect_min`, `app_id`, `group_name`, `queue_name`, `app_name`, `app_type`, `app_mode`, `res_type`) COMMENT "OLAP" PARTITION BY RANGE(`collect_day`) (PARTITION p20200215 VALUES  LESS THAN  ('2020-02-15'), PARTITION p20200216 VALUES  LESS THAN  ('2020-02-16'), PARTITION p20200217 VALUES  LESS THAN  ('2020-02-17')) DISTRIBUTED BY HASH(`group_name`) BUCKETS 30 PROPERTIES (  "replication_num" = "1");
-- insert
insert into afo_app_gpu_resource(`collect_day`, `collect_min`, `app_id`, `group_name`, `queue_name`, `app_name`, `app_type`, `app_mode`, `res_type`, res_schedule, res_used, res_util) values ('2020-02-16', '2020-02-16 00:00', 'application_1547876776012_46509900', 'root.serving.hadoop-peisong', 'root.serving.hadoop-peisong.p40prod', 'TFS','AFO-Serving', 'NULL', 'unknown', 2, 2,0)

CREATE TABLE `yarn_groups_gpu_resource` ( `dt` date NULL COMMENT "日期", `scenes` varchar(20) NULL COMMENT "", `yarn_cluster` varchar(30) NULL COMMENT "", `tenant` varchar(50) NULL COMMENT "", `group_id` varchar(10) NULL COMMENT "", `group_code` varchar(200) NULL COMMENT "", `res_type` varchar(20) NULL COMMENT "", `res_min` int(11) SUM NULL COMMENT "") ENGINE=OLAP AGGREGATE KEY(`dt`, `scenes`, `yarn_cluster`, `tenant`, `group_id`, `group_code`, `res_type`) PARTITION BY RANGE(`dt`) (PARTITION p20200215 VALUES  LESS THAN  ('2020-02-15'), PARTITION p20200216 VALUES  LESS THAN  ('2020-02-16'), PARTITION p20200217 VALUES  LESS THAN  ('2020-02-17')) DISTRIBUTED BY HASH(`tenant`) BUCKETS 10 PROPERTIES (  "replication_num" = "1")

insert into yarn_groups_gpu_resource(`dt`, `scenes`, `yarn_cluster`, `tenant`, `group_id`, `group_code`, `res_type`, res_min) values ('2020-02-16', 'Serving', 'gh_serving', 'hadoop-poistar', '3143', 'root.gh_serving.hadoop-poistar','gcores', 13)


create database issue_4598_bak
use issue_4598_bak
-- query
select date_format(dt, '%Y%m%d') as dt from issue_4598.afo_app_gpu_resource as app left join ( select  dt from issue_4598.yarn_groups_gpu_resource ) g on app.collect_day = g.dt group by dt

select dt from issue_4598.afo_app_gpu_resource as app left join (select  dt from issue_4598.yarn_groups_gpu_resource) g on app.collect_day = g.dt group by dt
drop database issue_4598
drop database issue_4598_bak
