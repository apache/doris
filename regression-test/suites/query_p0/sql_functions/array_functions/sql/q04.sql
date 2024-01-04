DROP TABLE IF EXISTS ads_pi_cuser_all_info;
CREATE TABLE IF NOT EXISTS ads_pi_cuser_all_info (
                                       `corp_id` varchar(64) NOT NULL COMMENT '机构ID',
                                       `staff_id` varchar(64) NOT NULL COMMENT '客户经理ID',
                                       `external_user_id` varchar(64) NOT NULL COMMENT '外部联系人ID',
                                       `is_deleted` int(11) REPLACE_IF_NOT_NULL NULL COMMENT '删除好友标识',
                                       `main_id` largeint(40) REPLACE_IF_NOT_NULL NULL COMMENT '用户main_id',
                                       `birthday` varchar(32) REPLACE_IF_NOT_NULL NULL COMMENT '客户生日',
                                       `gender` tinyint(4) REPLACE_IF_NOT_NULL NULL COMMENT '用户性别',
                                       `avater` text REPLACE_IF_NOT_NULL NULL COMMENT '用户头像地址',
                                       `name` text REPLACE_IF_NOT_NULL NULL COMMENT '用户姓名',
                                       `remark_name` text REPLACE_IF_NOT_NULL NULL COMMENT '客户经理备注姓名',
                                       `type` tinyint(4) REPLACE_IF_NOT_NULL NULL,
                                       `client_number` varchar(64) REPLACE_IF_NOT_NULL NULL COMMENT '上一次行为码',
                                       `M0000001` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000002` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000003` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000004` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000005` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000006` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000007` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000008` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000009` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000010` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000011` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000012` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000013` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000014` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000015` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000016` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000017` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000018` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000019` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `M0000020` int(11) SUM NOT NULL DEFAULT "0" COMMENT '预设用例分值，1000000取余',
                                       `product_reach_times` int(11) SUM NOT NULL DEFAULT "0" COMMENT '本周产品触达次数，每自然周自动清零',
                                       `common_reach_times` int(11) SUM NOT NULL DEFAULT "0" COMMENT '本周通用触达次数，每自然周自动清零',
                                       `last_action` varchar(64) REPLACE_IF_NOT_NULL NULL COMMENT '上一次行为码',
                                       `qw_tag_ids` text REPLACE_IF_NOT_NULL NULL COMMENT '企微标签，使用逗号分隔',
                                       `stgy_tag_ids` text REPLACE_IF_NOT_NULL NULL COMMENT '企微标签，使用逗号分隔',
                                       `last_reached_task_id` largeint(40) REPLACE_IF_NOT_NULL NULL COMMENT '上一次触达任务ID',
                                       `last_reached_task_score` int(11) REPLACE_IF_NOT_NULL NULL COMMENT '上一次触达阶段分值',
                                       `current_max_score_mode` text REPLACE_IF_NOT_NULL NULL COMMENT '分值最高的用例字段名，逗号分隔'
) ENGINE=OLAP
AGGREGATE KEY(`corp_id`, `staff_id`, `external_user_id`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`staff_id`) BUCKETS 3
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"is_being_synced" = "false",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false"
);

INSERT INTO ads_pi_cuser_all_info
(corp_id, staff_id, external_user_id, is_deleted, main_id, birthday, gender, avater, name, remark_name, `type`, client_number, M0000001, M0000002, M0000003, M0000004, M0000005, M0000006, M0000007, M0000008, M0000009, M0000010, M0000011, M0000012, M0000013, M0000014, M0000015, M0000016, M0000017, M0000018, M0000019, M0000020, product_reach_times, common_reach_times, last_action, qw_tag_ids, stgy_tag_ids, last_reached_task_id, last_reached_task_score, current_max_score_mode)
VALUES('ww36b98e83f52f6bcc', '0af73fc236bf444aadc801cd4c416539', 'wmfvPXDAAA7OrmzTSkEVRkphuGx3hSVA', 0, '1604732822185627669', '', 2, 'http://wx.qlogo.cn/mmhead/9M0PhLTmTIeHGOibG2yxg90drr4nhu6NuJ5O4J9bskXicNShwsiaukk6g/0', 'mate20.0410q', 'mate20.0410q', 1, '', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NULL, '45,34,22', NULL, '275', 0, NULL);
INSERT INTO ads_pi_cuser_all_info
(corp_id, staff_id, external_user_id, is_deleted, main_id, birthday, gender, avater, name, remark_name, `type`, client_number, M0000001, M0000002, M0000003, M0000004, M0000005, M0000006, M0000007, M0000008, M0000009, M0000010, M0000011, M0000012, M0000013, M0000014, M0000015, M0000016, M0000017, M0000018, M0000019, M0000020, product_reach_times, common_reach_times, last_action, qw_tag_ids, stgy_tag_ids, last_reached_task_id, last_reached_task_score, current_max_score_mode)
VALUES('ww36b98e83f52f6bcc', '0af73fc236bf444aadc801cd4c416539', 'wmfvPXDAAAHCcW-cFR5U2yPG5zfAS4rg', 0, '1600777478614724671', '', 0, 'http://wx.qlogo.cn/mmhead/Q3auHgzwzM43qyI9vM4Q8jYrdl7ia8FakbibeTWnSmTVu7QjtxHLJib2g/0', 'iPhoneXS', 'iPhoneXS', 1, '', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NULL, NULL, '56,34,35', NULL, NULL, NULL);
INSERT INTO ads_pi_cuser_all_info
(corp_id, staff_id, external_user_id, is_deleted, main_id, birthday, gender, avater, name, remark_name, `type`, client_number, M0000001, M0000002, M0000003, M0000004, M0000005, M0000006, M0000007, M0000008, M0000009, M0000010, M0000011, M0000012, M0000013, M0000014, M0000015, M0000016, M0000017, M0000018, M0000019, M0000020, product_reach_times, common_reach_times, last_action, qw_tag_ids, stgy_tag_ids, last_reached_task_id, last_reached_task_score, current_max_score_mode)
VALUES('ww36b98e83f52f6bcc1', '0af73fc236bf444aadc801cd4c416539', 'wmfvPXDAAA7OrmzTSkEVRkphuGx3hSVA', 0, '1604732822185627669', '', 2, 'http://wx.qlogo.cn/mmhead/9M0PhLTmTIeHGOibG2yxg90drr4nhu6NuJ5O4J9bskXicNShwsiaukk6g/0', 'mate20.0410q', 'mate20.0410q', 1, '', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NULL, NULL, '45,34', '275', 0, NULL);
INSERT INTO ads_pi_cuser_all_info
(corp_id, staff_id, external_user_id, is_deleted, main_id, birthday, gender, avater, name, remark_name, `type`, client_number, M0000001, M0000002, M0000003, M0000004, M0000005, M0000006, M0000007, M0000008, M0000009, M0000010, M0000011, M0000012, M0000013, M0000014, M0000015, M0000016, M0000017, M0000018, M0000019, M0000020, product_reach_times, common_reach_times, last_action, qw_tag_ids, stgy_tag_ids, last_reached_task_id, last_reached_task_score, current_max_score_mode)
VALUES('ww36b98e83f52f6bcc2', '0af73fc236bf444aadc801cd4c416539', 'wmfvPXDAAAHCcW-cFR5U2yPG5zfAS4rg', 0, '1600777478614724671', '', 0, 'http://wx.qlogo.cn/mmhead/Q3auHgzwzM43qyI9vM4Q8jYrdl7ia8FakbibeTWnSmTVu7QjtxHLJib2g/0', 'iPhoneXS', 'iPhoneXS', 1, '', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NULL, NULL, '22,25,34', NULL, NULL, NULL);


select stgy_tag_ids from ads_pi_cuser_all_info WHERE arrays_overlap(split_by_string(stgy_tag_ids,','),['23','22']);

select * from ads_pi_cuser_all_info WHERE arrays_overlap(split_by_string(stgy_tag_ids,','),['23','22']) and arrays_overlap(split_by_string(stgy_tag_ids,','),['35']);