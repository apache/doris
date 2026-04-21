// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite('test_repeat_no_stackflow') {
    multi_sql '''
       SET disable_nereids_rules="PRUNE_EMPTY_PARTITION";

       DROP TABLE IF EXISTS test_repeat_no_stackflow_t1 FORCE;
       DROP TABLE IF EXISTS test_repeat_no_stackflow_t2 FORCE;
       DROP TABLE IF EXISTS test_repeat_no_stackflow_t3 FORCE;
       DROP TABLE IF EXISTS test_repeat_no_stackflow_t4 FORCE;
       DROP TABLE IF EXISTS test_repeat_no_stackflow_t5 FORCE;
       DROP TABLE IF EXISTS test_repeat_no_stackflow_t6 FORCE;

       CREATE TABLE `test_repeat_no_stackflow_t1` (
  `id` int NOT NULL,
  `agent_id` int NOT NULL,
  `site_id` int NULL ,
  `tdate` varchar(255) NOT NULL ,
  `money` decimal(32,3) NOT NULL DEFAULT "0.000" ,
  `monies` decimal(13,3) NOT NULL DEFAULT "0.000" ,
  `account` decimal(13,3) NOT NULL DEFAULT "0.000" ,
  `reward_cost` decimal(13,3) NOT NULL DEFAULT "0.000" ,
  `finance_cost` decimal(13,3) NOT NULL DEFAULT "0.000" ,
  `ad_type` tinyint NOT NULL ,
  `agent_type` tinyint NULL ,
  `pay_type` tinyint NOT NULL ,
  `pay_date` varchar(255) NOT NULL,
  `memo` text NULL,
  `aorder` varchar(20) NULL,
  `plat_id` tinyint NOT NULL DEFAULT "1",
  `game_id` int NOT NULL DEFAULT "0",
  `add_type` tinyint NOT NULL DEFAULT "0" ,
  `company_id` tinyint NOT NULL DEFAULT "0",
  `nature` tinyint NOT NULL DEFAULT "0" ,
  `s_date` varchar(255) NOT NULL ,
  `e_date` varchar(255) NOT NULL ,
  `author` varchar(50) NULL ,
  `game_sign` varchar(64) NOT NULL DEFAULT "" ,
  `agent` varchar(64) NOT NULL DEFAULT "" ,
  `media` varchar(64) NOT NULL DEFAULT "" ,
  `company` varchar(16) NOT NULL DEFAULT "" ,
  `company_type` varchar(16) NOT NULL DEFAULT "self" ,
  `other` tinyint NOT NULL DEFAULT "0" ,
  `is_delete` tinyint NOT NULL DEFAULT "0" ,
  `created_at` varchar(255) NOT NULL,
  `updated_at` varchar(255) NOT NULL
) ENGINE=OLAP
UNIQUE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"replication_num" = "1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728",
"enable_mow_light_delete" = "false"
);

CREATE TABLE `test_repeat_no_stackflow_t2` (
  `uid` varchar(255) NULL ,
  `game_sign` varchar(255) NULL ,
  `register_date` date NOT NULL ,
  `agent_id` varchar(255) NULL ,
  `site_id` varchar(255) NULL ,
  `game_id` varchar(255) NULL ,
  `guid` varchar(255) NOT NULL ,
  `openid` text NULL,
  `mobile_phone` varchar(255) NULL ,
  `user_name` varchar(255) NULL ,
  `imei` text NULL ,
  `long_id` varchar(255) NULL ,
  `mtype` varchar(30) NULL ,
  `package` varchar(255) NULL ,
  `reg_time` varchar(255) NULL ,
  `logined` varchar(30) NULL ,
  `reg_type` varchar(30) NULL ,
  `os` varchar(20) NULL ,
  `game_aweme_id` varchar(255) NULL ,
  `match_type` varchar(30) NULL ,
  `network` varchar(255) NULL ,
  `idfv` text NULL ,
  `system` varchar(255) NULL ,
  `model` varchar(255) NULL ,
  `come_back_user` varchar(10) NULL ,
  `ip` varchar(255) NULL ,
  `ipv6` varchar(255) NULL ,
  `oaid` varchar(255) NULL ,
  `adinfo` text NULL ,
  `ad_device` text NULL ,
  `wx_platform` varchar(255) NULL ,
  `original_imei` varchar(255) NULL ,
  `total_reg_num` varchar(255) NULL ,
  `reg_hour` varchar(10) NOT NULL ,
  `reg_date_hour` varchar(128) NOT NULL
) ENGINE=OLAP
UNIQUE KEY(`uid`, `game_sign`, `register_date`)
DISTRIBUTED BY HASH(`uid`) BUCKETS 10
PROPERTIES (
"replication_num" = "1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728",
"enable_mow_light_delete" = "false"
);

CREATE TABLE `test_repeat_no_stackflow_t3` (
  `id` int NOT NULL,
  `site_id` int NOT NULL DEFAULT "0" ,
  `site_name` varchar(255) NOT NULL DEFAULT "" ,
  `agent_id` int NOT NULL,
  `plat` tinyint NOT NULL DEFAULT "0" ,
  `plat_id` tinyint NOT NULL DEFAULT "1" ,
  `chargeman` varchar(128) NOT NULL DEFAULT "" ,
  `site_url` varchar(128) NULL DEFAULT "" ,
  `https` tinyint NULL DEFAULT "0" ,
  `channel` varchar(60) NOT NULL DEFAULT "" ,
  `addtime` datetime NULL,
  `edittime` datetime NULL,
  `isdel` tinyint NOT NULL DEFAULT "0" ,
  `is_remote` tinyint NOT NULL DEFAULT "0" ,
  `convert_source_type` varchar(128) NOT NULL DEFAULT "" ,
  `convert_toolkit` varchar(128) NOT NULL DEFAULT "" ,
  `anchor_id` int NOT NULL DEFAULT "0" ,
  `anchor_account` varchar(128) NOT NULL DEFAULT "" ,
  `anchor_account_id` int NOT NULL DEFAULT "0" ,
  `create_user` varchar(128) NULL DEFAULT "" ,
  `update_user` varchar(128) NULL DEFAULT "" ,
  `action_track_type` int NULL DEFAULT "0"
) ENGINE=OLAP
UNIQUE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"replication_num" = "1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728",
"enable_mow_light_delete" = "false"
);

CREATE TABLE `test_repeat_no_stackflow_t4` (
  `id` varchar(255) NULL ,
  `agent_id` varchar(255) NULL ,
  `agent_name` varchar(255) NULL ,
  `group` varchar(255) NULL ,
  `media` varchar(255) NULL ,
  `agent` varchar(255) NULL ,
  `plat_id` varchar(10) NULL ,
  `reg_date` varchar(64) NULL ,
  `chargeman` varchar(128) NULL ,
  `agent_type` varchar(32) NULL ,
  `department_id` varchar(255) NULL ,
  `gr_agent_channel` varchar(64) NULL ,
  `channel_client` varchar(64) NULL ,
  `account_id` varchar(100) NULL ,
  `top` varchar(10) NULL ,
  `activity_at` varchar(64) NULL ,
  `updated_time` varchar(64) NULL ,
  `is_tw` varchar(10) NULL ,
  `system` varchar(32) NULL ,
  `subject` varchar(32) NULL ,
  `gr_agent_remarks` text NULL ,
  `agent_media_name` varchar(64) NULL ,
  `description` varchar(255) NULL ,
  `gr_agent_media_channel` varchar(32) NULL ,
  `callback` varchar(32) NULL ,
  `type` varchar(255) NULL ,
  `hot` varchar(10) NULL ,
  `default_media` varchar(3) NULL ,
  `agent_channel_name` varchar(32) NULL ,
  `gr_agent_channel_remarks` text NULL
) ENGINE=OLAP
UNIQUE KEY(`id`, `agent_id`)
DISTRIBUTED BY HASH(`agent_id`) BUCKETS 10
PROPERTIES (
"replication_num" = "1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728",
"enable_mow_light_delete" = "false"
);

CREATE TABLE `test_repeat_no_stackflow_t5` (
  `game_id` varchar(255) NULL ,
  `plat_id` varchar(20) NULL ,
  `game_app_name` varchar(255) NULL ,
  `game_sign` varchar(128) NULL ,
  `game_name` varchar(255) NULL ,
  `contract_name` varchar(255) NULL ,
  `company_qualification` varchar(255) NULL ,
  `system` varchar(64) NULL ,
  `sync` varchar(10) NULL ,
  `type` varchar(10) NULL ,
  `game_product_id` varchar(32) NULL ,
  `product_name` varchar(255) NULL ,
  `core_play` varchar(32) NULL ,
  `story_theme` varchar(32) NULL ,
  `art_style` varchar(32) NULL ,
  `contract_company` varchar(32) NULL ,
  `state` varchar(10) NULL ,
  `screen_type` varchar(10) NULL ,
  `float_position` varchar(20) NULL ,
  `is_open` varchar(10) NULL ,
  `is_sync` varchar(10) NULL ,
  `open_game` varchar(4) NULL ,
  `app_name` varchar(256) NULL ,
  `app_id` varchar(255) NULL ,
  `release_state` varchar(10) NULL ,
  `media_abbr` varchar(64) NULL ,
  `os` varchar(10) NULL ,
  `os_two` varchar(10) NULL ,
  `game_version` varchar(20) NULL ,
  `channel_show` varchar(10) NULL ,
  `autologin` varchar(10) NULL ,
  `relate_game` varchar(255) NULL ,
  `discount` varchar(32) NULL ,
  `disable_register` varchar(4) NULL ,
  `disable_unrelated_login` varchar(4) NULL ,
  `disable_related_back` varchar(4) NULL ,
  `disable_back` varchar(4) NULL ,
  `accept_related_game` varchar(20) NULL ,
  `disable_pay` varchar(4) NULL ,
  `tw_plat_id` varchar(4) NULL ,
  `tw_os` varchar(4) NULL ,
  `server_sign` varchar(32) NULL ,
  `objective_id` varchar(32) NULL ,
  `company_main` varchar(64) NULL ,
  `business_purpose` varchar(4) NULL ,
  `agent_sign` varchar(60) NULL ,
  `icon` varchar(128) NULL ,
  `package_name_id` varchar(30) NULL ,
  `server_group_id` varchar(40) NULL ,
  `client_type` varchar(10) NULL ,
  `platform` varchar(10) NULL
) ENGINE=OLAP
UNIQUE KEY(`game_id`, `plat_id`)
DISTRIBUTED BY HASH(`game_id`) BUCKETS 10
PROPERTIES (
"replication_num" = "1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728",
"enable_mow_light_delete" = "false"
);

CREATE TABLE `test_repeat_no_stackflow_t6` (
  `id` int NOT NULL ,
  `abbr` varchar(64) NOT NULL ,
  `name` varchar(64) NOT NULL ,
  `desc` varchar(255) NULL ,
  `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP ,
  `created_user` varchar(64) NULL ,
  `update_at` datetime NULL DEFAULT CURRENT_TIMESTAMP ,
  `update_user` varchar(64) NULL ,
  `hot` tinyint NULL ,
  `usage_status` tinyint NULL DEFAULT "1"
) ENGINE=OLAP
UNIQUE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"replication_num" = "1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728",
"enable_mow_light_delete" = "false"
);
    '''

    // the logical repeat had a long set intersection chain,
    // need convert set intersection's result SetView to Set,
    // otherwise it will cause stackoverflow
    sql """ explain
    SELECT
    agent_dims.game_id,
    cj.`system`,
    cj.game_sign,
    cj.os,
    cj.os_two,
    ci.account_id,
    ci.media,
    ci.gr_agent_media_channel,
    cj.game_product_id,
    ck.site_name,
    ci.chargeman,
    agent_dims.agent_id,
    agent_dims.site_id,
    ck.anchor_id,
    cl.abbr,
    SUM(agent_dims.total_money) AS total_cost,
    SUM(user_guid_reg_real.guid) AS total_reg_users,
    CASE
        WHEN SUM(user_guid_reg_real.guid) = 0 OR SUM(user_guid_reg_real.guid) IS NULL THEN 0
        ELSE ROUND(SUM(agent_dims.total_money) / NULLIF(SUM(user_guid_reg_real.guid), 0), 2)
    END AS reg_cost
FROM (
    SELECT
        `site_id`,
        `game_id`,
        `agent_id`,
        `game_sign`,
        tdate,
        COUNT(1),
        SUM(`money`) AS total_money
    FROM test_repeat_no_stackflow_t1
    WHERE tdate BETWEEN '2025-05-01' AND '2025-05-01'
    GROUP BY
        `site_id`,
        `game_id`,
        `agent_id`,
        `game_sign`,
        tdate
) AS agent_dims
LEFT JOIN (
    SELECT
        `site_id`,
        `game_id`,
        `agent_id`,
        `game_sign`,
        register_date,
        COUNT(DISTINCT guid) AS guid
    FROM test_repeat_no_stackflow_t2
    WHERE `register_date` BETWEEN '2025-05-01' AND '2025-05-01'
    GROUP BY
        `site_id`,
        `game_id`,
        `agent_id`,
        `game_sign`,
        register_date
) AS user_guid_reg_real
    ON agent_dims.game_id = user_guid_reg_real.game_id
    AND agent_dims.game_sign = user_guid_reg_real.game_sign
    AND agent_dims.agent_id = user_guid_reg_real.agent_id
    AND agent_dims.site_id = user_guid_reg_real.site_id
    AND agent_dims.tdate = user_guid_reg_real.register_date
LEFT JOIN `test_repeat_no_stackflow_t3` ck
    ON agent_dims.site_id = ck.site_id
LEFT JOIN `test_repeat_no_stackflow_t4` ci
    ON agent_dims.agent_id = ci.agent_id
LEFT JOIN `test_repeat_no_stackflow_t5` cj
    ON agent_dims.game_id = cj.game_id
LEFT JOIN `test_repeat_no_stackflow_t6` cl
    ON ci.agent = cl.abbr
WHERE
    agent_dims.game_id = '3706'
    AND agent_dims.site_id = '5100814'
GROUP BY
    CUBE(
        agent_dims.game_id,
        cj.`system`,
        cj.game_sign,
        cj.os,
        cj.os_two,
        ci.account_id,
        ci.media,
        ci.gr_agent_media_channel,
        cj.game_product_id,
        ck.site_name,
        ci.chargeman,
        agent_dims.agent_id,
        agent_dims.site_id,
        ck.anchor_id,
        cl.abbr
    );
    """

    multi_sql '''
       DROP TABLE IF EXISTS test_repeat_no_stackflow_t1 FORCE;
       DROP TABLE IF EXISTS test_repeat_no_stackflow_t2 FORCE;
       DROP TABLE IF EXISTS test_repeat_no_stackflow_t3 FORCE;
       DROP TABLE IF EXISTS test_repeat_no_stackflow_t4 FORCE;
       DROP TABLE IF EXISTS test_repeat_no_stackflow_t5 FORCE;
       DROP TABLE IF EXISTS test_repeat_no_stackflow_t6 FORCE;
    '''
}
