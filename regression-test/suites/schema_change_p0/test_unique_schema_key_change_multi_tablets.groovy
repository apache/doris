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

import org.apache.doris.regression.suite.ClusterOptions
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_unique_schema_key_change_multi_tablets","docker") {
     def options = new ClusterOptions()
     options.cloudMode = true
     options.setFeNum(1)
     options.setBeNum(1)
     options.enableDebugPoints()
     options.feConfigs.add("enable_workload_group=false")
     options.beConfigs.add('enable_java_support=false')
     options.beConfigs.add('meta_service_conflict_error_retry_times=1')
     options.beConfigs.add('alter_tablet_worker_count=100')
     options.msConfigs.add('delete_bitmap_lock_v2_white_list=*')
     options.msConfigs.add('enable_retry_txn_conflict=false')

     def tbName = "test_unique_schema_key_change_multi_tablets"
     def initTable = " CREATE TABLE `${tbName}` (\n" +
             "  `source` int NOT NULL,\n" +
             "  `data_region` varchar(6) NULL,\n" +
             "  `report_date` date NOT NULL,\n" +
             "  `report_hour` varchar(20),\n" +
             "  `affiliate_id` varchar(200),\n" +
             "  `ad_format` varchar(200) NULL,\n" +
             "  `ad_width` varchar(200) NULL,\n" +
             "  `ad_height` varchar(200) NULL,\n" +
             "  `os` varchar(200) NULL,\n" +
             "  `device_make` varchar(200) NULL,\n" +
             "  `bundle_id` varchar(200) NULL,\n" +
             "  `country` varchar(200) NULL,\n" +
             "  `country_a2` varchar(200) NULL,\n" +
             "  `connection_type` varchar(200) NULL,\n" +
             "  `campaign_id` int NULL,\n" +
             "  `ad_group_id` int NULL,\n" +
             "  `ad_id` int NULL,\n" +
             "  `creative_id` int NULL,\n" +
             "  `adv_id` int NULL,\n" +
             "  `offer_id` varchar(200) NULL,\n" +
             "  `bop_aff_id` varchar(200) NULL,\n" +
             "  `bid_strategy` int NULL,\n" +
             "  `p_ctr_version` varchar(200) NULL,\n" +
             "  `p_cvr_version` varchar(200) NULL,\n" +
             "  `p_d_cvr_version` varchar(200) NULL,\n" +
             "  `p_open_ivr_version` varchar(200) NULL,\n" +
             "  `feature_1` varchar(200) NULL,\n" +
             "  `response_type` int NULL,\n" +
             "  `pos` int NULL,\n" +
             "  `instl` int NULL,\n" +
             "  `domain` varchar(200) NULL,\n" +
             "  `video_placement` int NULL,\n" +
             "  `is_rewarded` int NULL,\n" +
             "  `bid_algorithm` int NULL,\n" +
             "  `first_ssp` varchar(200) NULL,\n" +
             "  `tag_id` varchar(512) NULL,\n" +
             "  `publisher_id` varchar(200) NULL,\n" +
             "  `fraud_type` varchar(200) NULL,\n" +
             "  `empty_device_id` int NULL,\n" +
             "  `audience` varchar(200) NULL,\n" +
             "  `opt_price` varchar(200) NULL,\n" +
             "  `package` varchar(512) NULL,\n" +
             "  `cold_boot_type` varchar(64) NULL,\n" +
             "  `bid_mode` varchar(64) NULL,\n" +
             "  `not_boot_cause` varchar(64) NULL,\n" +
             "  `traffic_source` int NULL,\n" +
             "  `traffic_type` int NULL,\n" +
             "  `bid_not_adjust_cause` int NULL,\n" +
             "  `win_rate_target_roi` varchar(200) NULL,\n" +
             "  `cpi_reduction_flag` int NULL,\n" +
             "  `companion_ad` int NULL,\n" +
             "  `companion_ad_w` int NULL,\n" +
             "  `companion_ad_h` int NULL,\n" +
             "  `parent_ad_creative_id` int NULL,\n" +
             "  `adjust_bid_version` varchar(200) NULL,\n" +
             "  `win_rate_version` varchar(200) NULL,\n" +
             "  `cpi_ad_cold_sort_flag` int NULL,\n" +
             "  `cpi_ad_cold_sort_rule` int NULL,\n" +
             "  `cps_cool_start_mode` varchar(32) NULL,\n" +
             "  `deeplink_support` int NULL,\n" +
             "  `ad_group_parent_id` varchar(200) NULL,\n" +
             "  `ad_parent_id` varchar(200) NULL,\n" +
             "  `category1_id` int NULL,\n" +
             "  `category1_name` varchar(200) NULL,\n" +
             "  `category2_id` int NULL,\n" +
             "  `category2_name` varchar(200) NULL,\n" +
             "  `category3_id` int NULL,\n" +
             "  `category3_name` varchar(200) NULL,\n" +
             "  `package_category1_id` int NULL,\n" +
             "  `package_category1_name` varchar(200) NULL,\n" +
             "  `package_category2_id` int NULL,\n" +
             "  `package_category2_name` varchar(200) NULL,\n" +
             "  `package_category3_id` int NULL,\n" +
             "  `package_category3_name` varchar(200) NULL,\n" +
             "  `affiliate_name` varchar(200) NULL,\n" +
             "  `campaign_name` varchar(200) NULL,\n" +
             "  `ad_group_name` varchar(200) NULL,\n" +
             "  `ad_name` varchar(200) NULL,\n" +
             "  `creative_name` varchar(200) NULL,\n" +
             "  `adv_name` varchar(200) NULL,\n" +
             "  `adv_type` varchar(200) NULL,\n" +
             "  `is_oem` int NULL,\n" +
             "  `tag_name` varchar(200) NULL,\n" +
             "  `request_count` bigint NULL,\n" +
             "  `response_count` bigint NULL,\n" +
             "  `rta_request` int NULL,\n" +
             "  `rta_request_true` int NULL,\n" +
             "  `shein_rta_request` int NULL,\n" +
             "  `shein_rta_request_true` int NULL,\n" +
             "  `tiktok_rta_request` int NULL,\n" +
             "  `tiktok_rta_request_true` int NULL,\n" +
             "  `win_count` bigint NULL,\n" +
             "  `imp_count` bigint NULL,\n" +
             "  `companion_imp_count` bigint NULL,\n" +
             "  `original_imp_count` bigint NULL,\n" +
             "  `click_count` bigint NULL,\n" +
             "  `companion_click_count` bigint NULL,\n" +
             "  `original_click_count` bigint NULL,\n" +
             "  `bid_floor_total` decimal(20,5) NULL,\n" +
             "  `bid_price_total` decimal(20,5) NULL,\n" +
             "  `bid_price_exp_total` decimal(20,5) NULL,\n" +
             "  `p_ctr_total` decimal(20,5) NULL,\n" +
             "  `p_ctr_exp_total` decimal(20,5) NULL,\n" +
             "  `p_ctr_calibrate_exp_total` decimal(20,5) NULL,\n" +
             "  `p_cvr_total` decimal(20,5) NULL,\n" +
             "  `p_cvr_imp_total` decimal(20,5) NULL,\n" +
             "  `p_cvr_ck_total` decimal(20,5) NULL,\n" +
             "  `p_d_cvr_total` decimal(20,5) NULL,\n" +
             "  `p_d_cvr_ck_total` decimal(20,5) NULL,\n" +
             "  `p_open_ivr_res_total` decimal(20,5) NULL,\n" +
             "  `p_open_ivr_imp_total` decimal(20,5) NULL,\n" +
             "  `p_open_ivr_ck_total` decimal(20,5) NULL,\n" +
             "  `win_success_price_total` decimal(20,5) NULL,\n" +
             "  `imp_success_price_total` decimal(20,5) NULL,\n" +
             "  `price_total` decimal(20,5) NULL,\n" +
             "  `event1_count` bigint NULL,\n" +
             "  `event2_count` bigint NULL,\n" +
             "  `event3_count` bigint NULL,\n" +
             "  `event4_count` bigint NULL,\n" +
             "  `event5_count` bigint NULL,\n" +
             "  `event6_count` bigint NULL,\n" +
             "  `event7_count` bigint NULL,\n" +
             "  `event8_count` bigint NULL,\n" +
             "  `event9_count` bigint NULL,\n" +
             "  `event10_count` bigint NULL,\n" +
             "  `event10_count_day0` bigint NULL,\n" +
             "  `event11_count` bigint NULL,\n" +
             "  `ad_estimated_commission` decimal(20,5) NULL,\n" +
             "  `adv_opt_price` decimal(20,5) NULL,\n" +
             "  `event10_count_rt` bigint NULL,\n" +
             "  `event11_count_rt` bigint NULL,\n" +
             "  `ad_estimated_commission_rt` decimal(20,5) NULL,\n" +
             "  `adv_opt_price_rt` decimal(20,5) NULL,\n" +
             "  `event11_day0` bigint NULL,\n" +
             "  `event11_day1` bigint NULL,\n" +
             "  `event11_day2` bigint NULL,\n" +
             "  `event11_day3` bigint NULL,\n" +
             "  `ad_estimated_commission_day0` decimal(20,5) NULL,\n" +
             "  `ad_estimated_commission_day1` decimal(20,5) NULL,\n" +
             "  `ad_estimated_commission_day2` decimal(20,5) NULL,\n" +
             "  `ad_estimated_commission_day3` decimal(20,5) NULL,\n" +
             "  `request_error_count` bigint NULL,\n" +
             "  `request_filter_count` bigint NULL,\n" +
             "  `p_reward` decimal(20,5) NULL,\n" +
             "  `request_id_count` bigint NULL,\n" +
             "  `imp_id_count` bigint NULL,\n" +
             "  `register_cnt` bigint NULL,\n" +
             "  `pay_cnt` bigint NULL,\n" +
             "  `2days_login_cnt` bigint NULL,\n" +
             "  `key_action_cnt` bigint NULL,\n" +
             "  `key_action2_cnt` bigint NULL,\n" +
             "  `key_action3_cnt` bigint NULL,\n" +
             "  `login_app_cnt` bigint NULL,\n" +
             "  `p_ct_cvr_imp_total` decimal(20,5) NULL,\n" +
             "  `p_ct_cvr_respond_total` decimal(20,5) NULL,\n" +
             "  `p_ct_cvr_click_total` decimal(20,5) NULL,\n" +
             "  `p_d_cvr_event10_total` decimal(20,5) NULL,\n" +
             "  `install_cnt` bigint NULL,\n" +
             "  `ecpm_total` decimal(20,5) NULL,\n" +
             "  `ecpm_before_adjust_total` decimal(20,5) NULL,\n" +
             "  `ecpm_after_adjust_total` decimal(20,5) NULL,\n" +
             "  `ecpm_before_adjust_imp_total` decimal(20,5) NULL,\n" +
             "  `p_rate_opt_total` decimal(20,5) NULL,\n" +
             "  `p_rate_imp_opt_total` decimal(20,5) NULL,\n" +
             "  `win_rate_lambda_total` decimal(20,5) NULL,\n" +
             "  `win_rate_mu_total` decimal(20,5) NULL,\n" +
             "  `win_rate_sigma_total` decimal(20,5) NULL,\n" +
             "  `imp_fj_ck` bigint NULL,\n" +
             "  `imp_fj_cfm` bigint NULL,\n" +
             "  `launch_success` bigint NULL,\n" +
             "  `launch_failed` bigint NULL,\n" +
             "  `purchase_event_a_cnt` bigint NULL,\n" +
             "  `purchase_event_c_cnt` bigint NULL,\n" +
             "  `purchase_event_a_cnt_art` bigint NULL,\n" +
             "  `purchase_event_c_cnt_art` bigint NULL,\n" +
             "  `app_open_cnt_art` bigint NULL,\n" +
             "  `order_amount` decimal(20,5) NULL,\n" +
             "  `order_amount_art` decimal(20,5) NULL,\n" +
             "  `adv_spend` decimal(20,5) NULL,\n" +
             "  `adv_spend_art` decimal(20,5) NULL,\n" +
             "  `ad_estimated_commission_art` decimal(20,5) NULL,\n" +
             "  `ad_estimated_commission_art_day0` decimal(20,5) NULL,\n" +
             "  `ad_estimated_commission_art_day1` decimal(20,5) NULL,\n" +
             "  `ad_estimated_commission_art_day2` decimal(20,5) NULL,\n" +
             "  `ad_estimated_commission_art_day3` decimal(20,5) NULL,\n" +
             "  `imp_count_et` bigint NULL,\n" +
             "  `click_count_et` bigint NULL,\n" +
             "  `purchase_event_a_cnt_et` bigint NULL,\n" +
             "  `purchase_event_c_cnt_et` bigint NULL,\n" +
             "  `purchase_event_a_cnt_art_et` bigint NULL,\n" +
             "  `purchase_event_c_cnt_art_et` bigint NULL,\n" +
             "  `app_open_cnt_art_et` bigint NULL,\n" +
             "  `order_amount_et` decimal(20,5) NULL,\n" +
             "  `order_amount_art_et` decimal(20,5) NULL,\n" +
             "  `adv_spend_et` decimal(20,5) NULL,\n" +
             "  `adv_spend_art_et` decimal(20,5) NULL,\n" +
             "  `event11_count_pt` bigint NULL,\n" +
             "  `ad_estimated_commission_pt` decimal(20,5) NULL,\n" +
             "  `etl_time` datetime NULL COMMENT 'ETL时间'\n" +
             ") ENGINE=OLAP\n" +
             "UNIQUE KEY(`source`, `data_region`, `report_date`, `report_hour`, `affiliate_id`, `ad_format`, `ad_width`, `ad_height`, `os`, `device_make`, `bundle_id`, `country`, `country_a2`, `connection_type`, `campaign_id`, `ad_group_id`, `ad_id`, `creative_id`, `adv_id`, `offer_id`, `bop_aff_id`, `bid_strategy`, `p_ctr_version`, `p_cvr_version`, `p_d_cvr_version`, `p_open_ivr_version`, `feature_1`, `response_type`, `pos`, `instl`, `domain`, `video_placement`, `is_rewarded`, `bid_algorithm`, `first_ssp`, `tag_id`, `publisher_id`, `fraud_type`, `empty_device_id`, `audience`, `opt_price`, `package`, `cold_boot_type`, `bid_mode`, `not_boot_cause`, `traffic_source`, `traffic_type`, `bid_not_adjust_cause`, `win_rate_target_roi`, `cpi_reduction_flag`, `companion_ad`, `companion_ad_w`, `companion_ad_h`, `parent_ad_creative_id`, `adjust_bid_version`, `win_rate_version`, `cpi_ad_cold_sort_flag`, `cpi_ad_cold_sort_rule`, `cps_cool_start_mode`, `deeplink_support`)\n" +
             "PARTITION BY RANGE(`report_date`)\n" +
             "(PARTITION p20250101 VALUES [('2025-01-01'), ('2025-01-02')),\n" +
             "PARTITION p20250102 VALUES [('2025-01-02'), ('2025-01-03')),\n" +
             "PARTITION p20250103 VALUES [('2025-01-03'), ('2025-01-04')),\n" +
             "PARTITION p20250104 VALUES [('2025-01-04'), ('2025-01-05')),\n" +
             "PARTITION p20250105 VALUES [('2025-01-05'), ('2025-01-06')),\n" +
             "PARTITION p20250106 VALUES [('2025-01-06'), ('2025-01-07')),\n" +
             "PARTITION p20250107 VALUES [('2025-01-07'), ('2025-01-08')),\n" +
             "PARTITION p20250108 VALUES [('2025-01-08'), ('2025-01-09')),\n" +
             "PARTITION p20250109 VALUES [('2025-01-09'), ('2025-01-10')),\n" +
             "PARTITION p20250110 VALUES [('2025-01-10'), ('2025-01-11')),\n" +
             "PARTITION p20250111 VALUES [('2025-01-11'), ('2025-01-12')),\n" +
             "PARTITION p20250112 VALUES [('2025-01-12'), ('2025-01-13')),\n" +
             "PARTITION p20250113 VALUES [('2025-01-13'), ('2025-01-14')),\n" +
             "PARTITION p20250114 VALUES [('2025-01-14'), ('2025-01-15')),\n" +
             "PARTITION p20250115 VALUES [('2025-01-15'), ('2025-01-16')),\n" +
             "PARTITION p20250116 VALUES [('2025-01-16'), ('2025-01-17')),\n" +
             "PARTITION p20250117 VALUES [('2025-01-17'), ('2025-01-18')),\n" +
             "PARTITION p20250118 VALUES [('2025-01-18'), ('2025-01-19')),\n" +
             "PARTITION p20250119 VALUES [('2025-01-19'), ('2025-01-20')),\n" +
             "PARTITION p20250120 VALUES [('2025-01-20'), ('2025-01-21')),\n" +
             "PARTITION p20250121 VALUES [('2025-01-21'), ('2025-01-22')),\n" +
             "PARTITION p20250122 VALUES [('2025-01-22'), ('2025-01-23')),\n" +
             "PARTITION p20250123 VALUES [('2025-01-23'), ('2025-01-24')),\n" +
             "PARTITION p20250124 VALUES [('2025-01-24'), ('2025-01-25')),\n" +
             "PARTITION p20250125 VALUES [('2025-01-25'), ('2025-01-26')),\n" +
             "PARTITION p20250126 VALUES [('2025-01-26'), ('2025-01-27')),\n" +
             "PARTITION p20250127 VALUES [('2025-01-27'), ('2025-01-28')),\n" +
             "PARTITION p20250128 VALUES [('2025-01-28'), ('2025-01-29')),\n" +
             "PARTITION p20250129 VALUES [('2025-01-29'), ('2025-01-30')),\n" +
             "PARTITION p20250130 VALUES [('2025-01-30'), ('2025-01-31')),\n" +
             "PARTITION p20250131 VALUES [('2025-01-31'), ('2025-02-01')),\n" +
             "PARTITION p20250201 VALUES [('2025-02-01'), ('2025-02-02')),\n" +
             "PARTITION p20250202 VALUES [('2025-02-02'), ('2025-02-03')),\n" +
             "PARTITION p20250203 VALUES [('2025-02-03'), ('2025-02-04')),\n" +
             "PARTITION p20250204 VALUES [('2025-02-04'), ('2025-02-05')),\n" +
             "PARTITION p20250205 VALUES [('2025-02-05'), ('2025-02-06')),\n" +
             "PARTITION p20250206 VALUES [('2025-02-06'), ('2025-02-07')),\n" +
             "PARTITION p20250207 VALUES [('2025-02-07'), ('2025-02-08')),\n" +
             "PARTITION p20250208 VALUES [('2025-02-08'), ('2025-02-09')),\n" +
             "PARTITION p20250209 VALUES [('2025-02-09'), ('2025-02-10')),\n" +
             "PARTITION p20250210 VALUES [('2025-02-10'), ('2025-02-11')),\n" +
             "PARTITION p20250211 VALUES [('2025-02-11'), ('2025-02-12')),\n" +
             "PARTITION p20250212 VALUES [('2025-02-12'), ('2025-02-13')),\n" +
             "PARTITION p20250213 VALUES [('2025-02-13'), ('2025-02-14')),\n" +
             "PARTITION p20250214 VALUES [('2025-02-14'), ('2025-02-15')),\n" +
             "PARTITION p20250215 VALUES [('2025-02-15'), ('2025-02-16')),\n" +
             "PARTITION p20250216 VALUES [('2025-02-16'), ('2025-02-17')),\n" +
             "PARTITION p20250217 VALUES [('2025-02-17'), ('2025-02-18')),\n" +
             "PARTITION p20250218 VALUES [('2025-02-18'), ('2025-02-19')),\n" +
             "PARTITION p20250219 VALUES [('2025-02-19'), ('2025-02-20')),\n" +
             "PARTITION p20250220 VALUES [('2025-02-20'), ('2025-02-21')),\n" +
             "PARTITION p20250221 VALUES [('2025-02-21'), ('2025-02-22')),\n" +
             "PARTITION p20250222 VALUES [('2025-02-22'), ('2025-02-23')),\n" +
             "PARTITION p20250223 VALUES [('2025-02-23'), ('2025-02-24')),\n" +
             "PARTITION p20250224 VALUES [('2025-02-24'), ('2025-02-25')),\n" +
             "PARTITION p20250225 VALUES [('2025-02-25'), ('2025-02-26')),\n" +
             "PARTITION p20250226 VALUES [('2025-02-26'), ('2025-02-27')),\n" +
             "PARTITION p20250227 VALUES [('2025-02-27'), ('2025-02-28')),\n" +
             "PARTITION p20250228 VALUES [('2025-02-28'), ('2025-03-01')),\n" +
             "PARTITION p20250301 VALUES [('2025-03-01'), ('2025-03-02')),\n" +
             "PARTITION p20250302 VALUES [('2025-03-02'), ('2025-03-03')),\n" +
             "PARTITION p20250303 VALUES [('2025-03-03'), ('2025-03-04')),\n" +
             "PARTITION p20250304 VALUES [('2025-03-04'), ('2025-03-05')),\n" +
             "PARTITION p20250305 VALUES [('2025-03-05'), ('2025-03-06')),\n" +
             "PARTITION p20250306 VALUES [('2025-03-06'), ('2025-03-07')),\n" +
             "PARTITION p20250307 VALUES [('2025-03-07'), ('2025-03-08')),\n" +
             "PARTITION p20250308 VALUES [('2025-03-08'), ('2025-03-09')),\n" +
             "PARTITION p20250309 VALUES [('2025-03-09'), ('2025-03-10')),\n" +
             "PARTITION p20250310 VALUES [('2025-03-10'), ('2025-03-11')),\n" +
             "PARTITION p20250311 VALUES [('2025-03-11'), ('2025-03-12')),\n" +
             "PARTITION p20250312 VALUES [('2025-03-12'), ('2025-03-13')),\n" +
             "PARTITION p20250313 VALUES [('2025-03-13'), ('2025-03-14')),\n" +
             "PARTITION p20250314 VALUES [('2025-03-14'), ('2025-03-15')),\n" +
             "PARTITION p20250315 VALUES [('2025-03-15'), ('2025-03-16')),\n" +
             "PARTITION p20250316 VALUES [('2025-03-16'), ('2025-03-17')),\n" +
             "PARTITION p20250317 VALUES [('2025-03-17'), ('2025-03-18')),\n" +
             "PARTITION p20250318 VALUES [('2025-03-18'), ('2025-03-19')),\n" +
             "PARTITION p20250319 VALUES [('2025-03-19'), ('2025-03-20')),\n" +
             "PARTITION p20250320 VALUES [('2025-03-20'), ('2025-03-21')),\n" +
             "PARTITION p20250321 VALUES [('2025-03-21'), ('2025-03-22')),\n" +
             "PARTITION p20250322 VALUES [('2025-03-22'), ('2025-03-23')),\n" +
             "PARTITION p20250323 VALUES [('2025-03-23'), ('2025-03-24')),\n" +
             "PARTITION p20250324 VALUES [('2025-03-24'), ('2025-03-25')),\n" +
             "PARTITION p20250325 VALUES [('2025-03-25'), ('2025-03-26')),\n" +
             "PARTITION p20250326 VALUES [('2025-03-26'), ('2025-03-27')),\n" +
             "PARTITION p20250327 VALUES [('2025-03-27'), ('2025-03-28')),\n" +
             "PARTITION p20250328 VALUES [('2025-03-28'), ('2025-03-29')),\n" +
             "PARTITION p20250329 VALUES [('2025-03-29'), ('2025-03-30')),\n" +
             "PARTITION p20250330 VALUES [('2025-03-30'), ('2025-03-31')),\n" +
             "PARTITION p20250331 VALUES [('2025-03-31'), ('2025-04-01')),\n" +
             "PARTITION p20250401 VALUES [('2025-04-01'), ('2025-04-02')),\n" +
             "PARTITION p20250402 VALUES [('2025-04-02'), ('2025-04-03')),\n" +
             "PARTITION p20250403 VALUES [('2025-04-03'), ('2025-04-04')),\n" +
             "PARTITION p20250404 VALUES [('2025-04-04'), ('2025-04-05')),\n" +
             "PARTITION p20250405 VALUES [('2025-04-05'), ('2025-04-06')),\n" +
             "PARTITION p20250406 VALUES [('2025-04-06'), ('2025-04-07')),\n" +
             "PARTITION p20250407 VALUES [('2025-04-07'), ('2025-04-08')),\n" +
             "PARTITION p20250408 VALUES [('2025-04-08'), ('2025-04-09')),\n" +
             "PARTITION p20250409 VALUES [('2025-04-09'), ('2025-04-10')),\n" +
             "PARTITION p20250410 VALUES [('2025-04-10'), ('2025-04-11')),\n" +
             "PARTITION p20250411 VALUES [('2025-04-11'), ('2025-04-12')),\n" +
             "PARTITION p20250412 VALUES [('2025-04-12'), ('2025-04-13')),\n" +
             "PARTITION p20250413 VALUES [('2025-04-13'), ('2025-04-14')),\n" +
             "PARTITION p20250414 VALUES [('2025-04-14'), ('2025-04-15')),\n" +
             "PARTITION p20250415 VALUES [('2025-04-15'), ('2025-04-16')),\n" +
             "PARTITION p20250416 VALUES [('2025-04-16'), ('2025-04-17')),\n" +
             "PARTITION p20250417 VALUES [('2025-04-17'), ('2025-04-18')),\n" +
             "PARTITION p20250418 VALUES [('2025-04-18'), ('2025-04-19')),\n" +
             "PARTITION p20250419 VALUES [('2025-04-19'), ('2025-04-20')),\n" +
             "PARTITION p20250420 VALUES [('2025-04-20'), ('2025-04-21')),\n" +
             "PARTITION p20250421 VALUES [('2025-04-21'), ('2025-04-22')),\n" +
             "PARTITION p20250422 VALUES [('2025-04-22'), ('2025-04-23')),\n" +
             "PARTITION p20250423 VALUES [('2025-04-23'), ('2025-04-24')),\n" +
             "PARTITION p20250424 VALUES [('2025-04-24'), ('2025-04-25')),\n" +
             "PARTITION p20250425 VALUES [('2025-04-25'), ('2025-04-26')),\n" +
             "PARTITION p20250426 VALUES [('2025-04-26'), ('2025-04-27')),\n" +
             "PARTITION p20250427 VALUES [('2025-04-27'), ('2025-04-28')),\n" +
             "PARTITION p20250428 VALUES [('2025-04-28'), ('2025-04-29')),\n" +
             "PARTITION p20250429 VALUES [('2025-04-29'), ('2025-04-30')),\n" +
             "PARTITION p20250430 VALUES [('2025-04-30'), ('2025-05-01')),\n" +
             "PARTITION p20250501 VALUES [('2025-05-01'), ('2025-05-02')),\n" +
             "PARTITION p20250502 VALUES [('2025-05-02'), ('2025-05-03')),\n" +
             "PARTITION p20250503 VALUES [('2025-05-03'), ('2025-05-04')),\n" +
             "PARTITION p20250504 VALUES [('2025-05-04'), ('2025-05-05')),\n" +
             "PARTITION p20250505 VALUES [('2025-05-05'), ('2025-05-06')),\n" +
             "PARTITION p20250506 VALUES [('2025-05-06'), ('2025-05-07')),\n" +
             "PARTITION p20250507 VALUES [('2025-05-07'), ('2025-05-08')),\n" +
             "PARTITION p20250508 VALUES [('2025-05-08'), ('2025-05-09')),\n" +
             "PARTITION p20250509 VALUES [('2025-05-09'), ('2025-05-10')),\n" +
             "PARTITION p20250510 VALUES [('2025-05-10'), ('2025-05-11')),\n" +
             "PARTITION p20250511 VALUES [('2025-05-11'), ('2025-05-12')),\n" +
             "PARTITION p20250512 VALUES [('2025-05-12'), ('2025-05-13')))\n" +
             "DISTRIBUTED BY HASH(`source`, `affiliate_id`, `ad_format`, `ad_width`, `ad_height`, `bundle_id`, `country`, `campaign_id`, `ad_group_id`) BUCKETS 24\n" +
             "PROPERTIES (\"enable_unique_key_merge_on_write\" = \"true\");"

     docker(options) {
          sql """ DROP TABLE IF EXISTS ${tbName} """
          sql initTable
          sql """  alter table ${tbName} add column p_order_ivr_version varchar(200) KEY NULL after p_open_ivr_version """

          def result
          for (int i = 0; i < 100; i++) {
               result = sql_return_maparray """ SHOW ALTER TABLE COLUMN WHERE TableName='${tbName}' ORDER BY createtime DESC LIMIT 1 """
               logger.info("result: ${result}")
               if (result.size() > 0 && (result[0].State == "FINISHED" || result[0].State == "CANCELLED")) {
                    break
               }
               sleep(1000)
          }
          assertEquals(1, result.size())
          assertEquals("FINISHED", result[0].State)
          sql """ insert into ${tbName}(source, report_date) values (1, '2025-05-12'); """
     }
}
