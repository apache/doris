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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("test_o2") {
    sql """set enable_nereids_planner=true"""
    sql """SET enable_fallback_to_original_planner=false"""
    sql """ DROP TABLE IF EXISTS o2_order_events; """

    sql """
        CREATE TABLE `o2_order_events` (
        `ts` datetime NULL,
        `metric_name` varchar(20) NULL,
        `city_id` int(11) NULL,
        `platform` varchar(20) NULL,
        `vendor_id` int(11) NULL,
        `pos_id` int(11) NULL,
        `is_instant_restaurant` boolean NULL,
        `country_id` int(11) NULL,
        `logistics_partner_id` int(11) NULL,
        `rpf_order` int(11) NULL,
        `rejected_message_id` int(11) NULL,
        `count_value` int(11) SUM NULL DEFAULT "0"
        ) ENGINE=OLAP
        AGGREGATE KEY(`ts`, `metric_name`, `city_id`, `platform`, `vendor_id`, `pos_id`, `is_instant_restaurant`, `country_id`, `logistics_partner_id`, `rpf_order`, `rejected_message_id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`metric_name`, `platform`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql """insert into o2_order_events values ("2023-08-16 22:27:00 ","ax",1,"asd",2,1,1,1,1,1,1,1);"""

    createMV ("""
            create materialized view o2_order_events_mv as select ts,metric_name,platform,sum(count_value) from o2_order_events group by ts,metric_name,platform;;""")

    sql """insert into o2_order_events values ("2023-08-16 22:27:00 ","ax",1,"asd",2,1,1,1,1,1,1,1);"""

    explain {
        sql("select ts,metric_name,platform,sum(count_value) from o2_order_events group by ts,metric_name,platform;")
        contains "(o2_order_events_mv)"
    }
    qt_select_mv "select ts,metric_name,platform,sum(count_value) from o2_order_events group by ts,metric_name,platform;"
}
