-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

CREATE TABLE `dwd_daytable` (
  `potno` int(11) NOT NULL,
  `ddate` date NOT NULL,
  `slot_days` varchar(20) REPLACE_IF_NOT_NULL NULL,
  `slot_generation` varchar(5) REPLACE_IF_NOT_NULL NULL,
  `slot_days_num` int(11) REPLACE_IF_NOT_NULL NULL,
  `runtime` int(11) REPLACE_IF_NOT_NULL NULL,
  `alouse_cnt` int(11) REPLACE_IF_NOT_NULL NULL,
  `alouse_num` int(11) REPLACE_IF_NOT_NULL NULL,
  `setv` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `realsetv` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `workv` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `averagev` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `ae_cnt` int(11) REPLACE_IF_NOT_NULL NULL,
  `blinkae_cnt` int(11) REPLACE_IF_NOT_NULL NULL,
  `aetime` int(11) REPLACE_IF_NOT_NULL NULL,
  `aewaittime` int(11) REPLACE_IF_NOT_NULL NULL,
  `aev` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `dybtime` int(11) REPLACE_IF_NOT_NULL NULL,
  `zf` int(11) REPLACE_IF_NOT_NULL NULL,
  `abnormitytime` int(11) REPLACE_IF_NOT_NULL NULL,
  `yjxc_d` int(11) REPLACE_IF_NOT_NULL NULL,
  `yjxc_cl` int(11) REPLACE_IF_NOT_NULL NULL,
  `temp` int(11) REPLACE_IF_NOT_NULL NULL,
  `fzb` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `setfhl_cnt` int(11) REPLACE_IF_NOT_NULL NULL,
  `fhl_cnt` int(11) REPLACE_IF_NOT_NULL NULL,
  `al_lvl` int(11) REPLACE_IF_NOT_NULL NULL,
  `djz_lvl` int(11) REPLACE_IF_NOT_NULL NULL,
  `tap` int(11) REPLACE_IF_NOT_NULL NULL,
  `plan_tap_m` int(11) REPLACE_IF_NOT_NULL NULL,
  `plan_tap_d` int(11) REPLACE_IF_NOT_NULL NULL,
  `fe_rto` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `si_rto` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `cathodev` int(11) REPLACE_IF_NOT_NULL NULL,
  `alo_rto` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `lif_rto` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `mgf_rto` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `caf_rto` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `kf_rto` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `workshop_name` varchar(20) REPLACE_IF_NOT_NULL NULL,
  `workroom_name` varchar(20) REPLACE_IF_NOT_NULL NULL,
  `lpw` varchar(20) REPLACE_IF_NOT_NULL NULL,
  `crrt` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `power_consum` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `workshop_code` bigint(20) REPLACE_IF_NOT_NULL NULL,
  `workroom_code` bigint(20) REPLACE_IF_NOT_NULL NULL,
  `potst` varchar(10) REPLACE_IF_NOT_NULL NULL,
  `slot_preheat_first_date` datetime REPLACE_IF_NOT_NULL NULL,
  `slot_preheat_generation` bigint(20) REPLACE_IF_NOT_NULL NULL,
  `slot_preheat_days` bigint(20) REPLACE_IF_NOT_NULL NULL,
  `slot_preheat_months` bigint(20) REPLACE_IF_NOT_NULL NULL,
  `electricity_efficiency` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `voltage_offset` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `over_temp` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `uo_rto` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `pot_design_type` text REPLACE_IF_NOT_NULL NULL,
  `tap_recom` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `electricity_efficiency_recom` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `fhl_cnt_recom` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `setv_recom` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL,
  `power_consum_recom` DECIMAL(27, 6) REPLACE_IF_NOT_NULL NULL
) ENGINE=OLAP
AGGREGATE KEY(`potno`, `ddate`)
DISTRIBUTED BY HASH(`potno`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);