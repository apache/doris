import org.apache.commons.lang3.StringUtils

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

suite("test_special_buffer_before_nested", "p0") {
    // note: this column order cannot be changed which with data should trigger mysql row buffer exactly reach 4096 before
    // open_dynamic_mode to write array
    sql """ DROP TABLE IF EXISTS test_special_buffer_before_nested """
    sql """ CREATE TABLE `test_special_buffer_before_nested` (
              `account_id` bigint NOT NULL,
              `register_time` datetime NULL,
              `nickname` varchar(1024) NULL,
              `facebook_name` varchar(1024) NULL,
              `google_name` varchar(1024) NULL,
              `twitter_name` varchar(1024) NULL,
              `email` varchar(1024) NULL,
              `language` varchar(1024) NULL,
              `gender` int NULL,
              `country` varchar(1024) NULL,
              `phone` varchar(1024) NULL,
              `timezone` varchar(1024) NULL,
              `sponsor` varchar(1024) NULL,
              `avatar` varchar(65533) NULL,
              `certification` int NULL,
              `suspicious_user` int NULL,
              `audit_user` int NULL,
              `tt_audit_user` int NULL,
              `auto_invite_bind` int NULL,
              `third_party_payment` int NULL,
              `official_payment` int NULL,
              `user_security_level` int NULL,
              `vip_end_time` datetime NULL,
              `agent` int NULL,
              `user_group_level` int NULL,
              `nearby_toggle` int NULL,
              `open_msg_notification` int NULL,
              `recall_time` datetime NULL,
              `first_recharge_time` datetime NULL,
              `total_recharge_amount` double NULL,
              `user_rfm_level` varchar(1024) NULL,
              `recharge_credit_amount` double NULL,
              `recharge_credit_count` int NULL,
              `recharge_credit_no_callback_count` int NULL,
              `total_withdraw_amount` double NULL,
              `withdraw_list` array<int> NULL,
              `user_charm_level` int NULL,
              `user_charm_history_level` int NULL,
              `user_auto_greetings_status` int NULL,
              `channel` varchar(1024) NULL,
              `bundle_id` varchar(1024) NULL,
              `platform` varchar(1024) NULL,
              `campaign_group_name` varchar(1024) NULL,
              `similar_group_id` varchar(1024) NULL,
              `cancel_account_time` datetime NULL,
              `log_seq` varchar(1024) NULL,
              `date_time` datetime NULL,
              `time_millis` bigint NULL,
              `first_country` varchar(1024) NULL,
              `invite_time` datetime NULL,
              `certification_time` datetime NULL,
              `certification_url` varchar(65533) NULL,
              `login_type` varchar(1024) NULL,
              `vip_start_time` datetime NULL,
              `vip_type` int NULL,
              `agent_time` datetime NULL,
              `recall_count` int NULL,
              `wake_count` int NULL,
              `client_ips` array<varchar(1024)> NULL,
              `client_ip` varchar(1024) NULL,
              `first_client_ip` varchar(1024) NULL,
              `device_ids` array<varchar(1024)> NULL,
              `device_id` varchar(1024) NULL,
              `first_device_id` varchar(1024) NULL,
              `phone_model` varchar(1024) NULL,
              `os_version` varchar(1024) NULL,
              `mcc_mnc` varchar(1024) NULL,
              `sys_region` varchar(1024) NULL,
              `carrier_region` varchar(1024) NULL,
              `cloud_id` varchar(1024) NULL,
              `device_name` varchar(1024) NULL,
              `first_channel` varchar(1024) NULL,
              `version` varchar(1024) NULL,
              `first_version` varchar(1024) NULL,
              `app_name` varchar(1024) NULL,
              `first_app_name` varchar(1024) NULL,
              `ad_campaign` varchar(65533) NULL,
              `last_ad_campaign` varchar(65533) NULL,
              `ad_campaign_id` varchar(65533) NULL,
              `last_ad_campaign_id` varchar(65533) NULL,
              `campaign` varchar(65533) NULL,
              `last_campaign` varchar(65533) NULL,
              `campaign_time` datetime NULL,
              `last_campaign_time` datetime NULL,
              `ad_name` varchar(65533) NULL,
              `last_ad_name` varchar(65533) NULL,
              `ad_advertiser` varchar(1024) NULL,
              `last_ad_advertiser` varchar(1024) NULL,
              `ad_advertiser_team` varchar(1024) NULL,
              `last_ad_advertiser_team` varchar(1024) NULL,
              `ad_app` varchar(1024) NULL,
              `last_ad_app` varchar(1024) NULL,
              `ad_group` varchar(65533) NULL,
              `last_ad_group` varchar(65533) NULL,
              `adid` varchar(65533) NULL,
              `last_adid` varchar(65533) NULL,
              `creative` varchar(65533) NULL,
              `last_creative` varchar(65533) NULL,
              `match_type` varchar(1024) NULL,
              `last_match_type` varchar(1024) NULL,
              `network` varchar(1024) NULL,
              `last_network` varchar(1024) NULL,
              `ad_country` varchar(1024) NULL,
              `last_ad_country` varchar(1024) NULL,
              `last_platform` varchar(1024) NULL,
              `ad_platform` varchar(1024) NULL,
              `last_ad_platform` varchar(1024) NULL,
              `install_begin_time` datetime NULL,
              `last_install_begin_time` datetime NULL,
              `installed_at` datetime NULL,
              `last_installed_at` datetime NULL,
              `install_finish_time` datetime NULL,
              `last_install_finish_time` datetime NULL,
              `click_time` datetime NULL,
              `last_click_time` datetime NULL,
              `referral_time` datetime NULL,
              `last_referral_time` datetime NULL,
              `registration_cost` double NULL,
              `abtest_sub` array<int> NULL,
              `abtest_id` array<int> NULL,
              `block_level_upgrade` int NULL,
              `passcard_grade` int NULL,
              `passcard_grade_end_time` datetime NULL,
              `passcard_pay` array<datetime> NULL,
              `passcard_number` datetime NULL,
              `vcast_grade_charm` int NULL,
              `vcast_grade_wealth` int NULL,
              `im_chat_violation` int NULL,
              `process_time` datetime NULL DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=OLAP
            UNIQUE KEY(`account_id`)
            DISTRIBUTED BY HASH(`account_id`) BUCKETS 64
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "min_load_replica_num" = "-1",
            "is_being_synced" = "false",
            "storage_medium" = "hdd",
            "storage_format" = "V2",
            "inverted_index_storage_format" = "V1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "store_row_column" = "true",
            "row_store_page_size" = "16384",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false",
            "enable_mow_light_delete" = "false"
            );
            """
    sql """ truncate table test_special_buffer_before_nested """
    sql """ insert into test_special_buffer_before_nested values(71111112,'2024-06-22 00:55:50','prem',NULL,'Sarkar sultan',NULL,'amorynanwaas525@gmail.com','en',1,'PK',NULL,'Asia/Karachi',NULL,'https://aaa.dddddd.cccc/asgte/bbbccc/1997-11-18/abcdefghigklmnopk0f4c9cbff3b2aac.png',1
            ,NULL,NULL,NULL,NULL,1,NULL,21,NULL,NULL,NULL,NULL,1,NULL,NULL,NULL,'R+F-M-',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'wlg',NULL,'Google',NULL,NULL,NULL,'bb851bcb725b4fc8b2495bde4898e5f5','2024-12-01 11:17:42',NULL,'PK',NULL,'2024-06-24 12:52:56',
            NULL,'google',NULL,0,NULL,20,1,'["154.81.244.112", "154.80.91.113", "154.80.72.110", "154.80.73.110", "103.131.212.96", "154.80.64.213", "103.131.212.19", "154.80.116.29", "154.80.117.215", "103.131.212.45", "154.80.115.43", "154.80.78.149", "154.80.65.149", "154.80.83.110", "154.80.67.46", "154.81.243.198", "154.80.83.118", "154.81.244.140", "154.80.105.177", "154.80.123.254", "154.80.99.141", "154.80.109.50", "154.80.100.66", "103.131.213.174", "154.80.122.66", "103.131.213.12", "154.81.243.209", "154.80.106.40", "154.80.76.77", "154.80.97.51", "154.80.112.44", "103.131.212.26", "154.81.245.155", "154.81.244.250", "154.80.71.72", "154.80.87.41", "103.131.213.24", "154.80.110.133", "154.80.109.201", "154.80.111.49", "154.80.77.246", "154.80.81.80", "154.80.64.80", "103.131.212.59", "154.80.95.80", "154.81.243.224", "154.80.64.195", "103.131.213.221", "154.80.75.211", "154.81.244.45", "154.80.69.218", "154.80.110.153", "154.80.99.153", "154.80.106.79", "103.131.212.109", "154.80.88.1", "103.131.213.82", "154.81.241.197", "154.80.107.58", "154.80.81.173", "103.131.213.185", "154.80.66.84", "103.131.212.101", "154.80.120.21", "154.80.65.158", "103.131.213.117", "103.131.213.104", "103.131.213.215", "103.131.212.252", "103.131.213.228", "103.131.212.48", "154.81.247.69", "103.131.213.78", "103.131.212.54", "103.131.213.58", "103.131.212.170", "103.131.213.234", "103.131.212.83", "154.81.244.150", "103.131.213.218", "103.131.212.120", "103.131.212.32", "103.131.213.1", "103.131.213.246", "103.131.213.21", "103.131.212.35", "103.131.212.146", "103.131.213.26", "103.131.212.80", "103.131.212.115", "103.131.212.123", "103.131.213.255", "103.131.213.247", "103.131.213.92", "103.131.213.9", "103.131.213.136", "103.131.212.192", "103.131.212.97", "103.131.212.126", "154.80.84.103", "103.131.213.159", "103.131.213.68", "103.131.212.66", "103.131.212.10", "154.80.71.170", "154.80.80.122", "154.80.109.41", "154.80.103.59", "103.131.213.89", "103.131.213.143", "154.80.78.13", "103.131.213.147", "154.80.118.77", "103.131.212.135", "154.81.245.71", "154.80.84.246", "154.80.79.246", "154.80.103.50", "154.80.87.158", "103.131.213.177", "103.131.213.16", "154.80.117.48", "103.131.213.88", "154.80.77.88", "103.131.213.73", "154.80.72.8", "154.80.87.104", "103.131.212.175", "154.80.114.89", "103.131.212.183", "103.131.213.130", "154.80.64.173", "103.131.212.236", "103.131.212.8", "103.131.212.2", "103.131.212.107", "103.131.213.17", "103.131.212.70", "103.131.212.37", "154.80.106.117", "103.131.213.149", "103.131.212.232", "103.131.213.238", "154.80.80.62", "154.80.83.83", "103.131.213.55", "103.131.213.204", "154.80.112.54", "154.80.104.6", "103.131.212.229", "154.80.90.250", "103.131.212.145", "154.80.85.250", "103.131.213.112", "154.80.84.15", "103.131.212.223", "103.131.213.84", "154.80.67.207", "154.80.67.205", "103.131.212.187", "154.80.110.200", "103.131.212.221", "154.80.75.178", "103.131.212.200", "154.80.83.226", "103.131.213.13", "103.131.213.252", "103.131.212.234", "103.131.213.18", "154.80.106.193", "103.131.212.157", "154.80.107.227", "154.80.111.155", "154.80.107.27", "103.131.212.142", "154.80.101.91", "103.131.213.154", "103.131.212.63", "103.131.213.250", "154.80.94.62", "154.80.69.143", "154.80.74.47", "154.80.81.47", "154.80.109.147", "103.131.213.231", "154.80.84.236", "154.80.120.207", "154.81.247.207", "154.80.118.95"]',
            '154.80.118.95','154.81.244.112','["7e5007a7-09e3-4ffa-888b-4487a654c39b"]','7e5007a7-09e3-4ffa-888b-4487a654c39b','7e5007a7-09e3-4ffa-888b-4487a654c39b','vivo_vivo%2B1814','Android_Android_8.1.0','41001','US',NULL,NULL,NULL,'wlg','3.4.6','3.3.5','com.video.welive','com.video.welive',
            'EN_wlg_zt_ray_gg_ac_2.5_Sup_all_240409_vo',NULL,'21174044377',NULL,NULL,NULL,NULL,NULL,'b3202c4487fe9728e68d748350b84f86',NULL,'ray',NULL,'zt',NULL,'wlg',NULL,'mix-EN-0409',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'EN',NULL,NULL,'Google',
            NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'[59, 4]','[17, 1]',NULL,NULL,NULL,NULL,NULL,NULL,NULL,1,'2024-12-02 09:50:52'); """
    qt_sql """ select * from  test_special_buffer_before_nested """
}
