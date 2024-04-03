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

suite ("test_tcu") {
    sql """set enable_nereids_planner=true"""
    sql """SET enable_fallback_to_original_planner=false"""
    sql """ DROP TABLE IF EXISTS tcu_test; """

    sql """
        CREATE TABLE `tcu_test` (
        `a` varchar(50) NULL,
        `b` datetime NULL,
        `c` datetime NULL,
        `d` datetime NULL,
        `e` datetime NULL,
        `f` text NULL,
        `g` text NULL,
        `h` text NULL,
        `i` text NULL,
        `j` text NULL,
        `k` text NULL
        ) ENGINE = OLAP DUPLICATE KEY(`a`) COMMENT 'OLAP' PARTITION BY RANGE(`e`) (
        PARTITION p20230822
        VALUES
            [('2023-08-22 00:00:00'), ('2023-08-23 00:00:00')), PARTITION p20230823 VALUES [('2023-08-23 00:00:00'), ('2023-08-24 00:00:00')), PARTITION p20230824 VALUES [('2023-08-24 00:00:00'), ('2023-08-25 00:00:00')), PARTITION p20230825 VALUES [('2023-08-25 00:00:00'), ('2023-08-26 00:00:00')), PARTITION p20230826 VALUES [('2023-08-26 00:00:00'), ('2023-08-27 00:00:00'))) DISTRIBUTED BY HASH(`a`) BUCKETS 20 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "is_being_synced" = "false", "dynamic_partition.enable" = "true", "dynamic_partition.time_unit" = "DAY", "dynamic_partition.time_zone" = "Etc/UTC", "dynamic_partition.start" = "-2147483648", "dynamic_partition.end" = "3", "dynamic_partition.prefix" = "p", "dynamic_partition.replication_allocation" = "tag.location.default: 1", "dynamic_partition.buckets" = "20", "dynamic_partition.create_history_partition" = "false", "dynamic_partition.history_partition_num" = "-1", "dynamic_partition.hot_partition_num" = "0", "dynamic_partition.reserved_history_periods" = "NULL", "dynamic_partition.storage_policy" = "", "dynamic_partition.storage_medium" = "HDD", "storage_format" = "V2", "light_schema_change" = "true", "disable_auto_compaction" = "false", "enable_single_replica_compaction" = "false" ) ;
        """

    sql """insert into tcu_test values('vin78215KHVB','2023-08-23 06:47:14','2023-08-23 06:47:15','2023-08-23 06:47:15','2023-08-23 00:00:00','MessageName','MessageType','referenceId','ActivityID','ProtofileVersion','{"k22":{"k221":{"k2211":2023,"k2212":8,"k2213":23,"k2214":6,"k2215":47,"k2216":14},"k222":{"k2221":84,"k2222":43,"k2223":3,"k2224":{"xxxx01_U_Actl":4.164,"xxxx02_U_Actl":4.163,"xxxx03_U_Actl":4.155,"xxxx04_U_Actl":4.164,"xxxx05_U_Actl":4.162,"xxxx06_U_Actl":4.159,"xxxx07_U_Actl":4.16,"xxxx08_U_Actl":4.162,"xxxx09_U_Actl":4.162,"xxxx10_U_Actl":4.164,"xxxx11_U_Actl":4.161,"xxxx12_U_Actl":4.162,"xxxx13_U_Actl":4.159,"xxxx14_U_Actl":4.159,"xxxx15_U_Actl":4.162,"xxxx16_U_Actl":4.163,"xxxx17_U_Actl":4.163,"xxxx18_U_Actl":4.159,"xxxx19_U_Actl":4.161,"xxxx20_U_Actl":4.159,"xxxx21_U_Actl":4.163,"xxxx22_U_Actl":4.161,"xxxx23_U_Actl":4.164,"xxxx24_U_Actl":4.161,"xxxx25_U_Actl":4.16,"xxxx26_U_Actl":4.161,"xxxx27_U_Actl":4.161,"xxxx28_U_Actl":4.164,"xxxx29_U_Actl":4.162,"xxxx30_U_Actl":4.161,"xxxx31_U_Actl":4.16,"xxxx32_U_Actl":4.162,"xxxx33_U_Actl":4.162,"xxxx34_U_Actl":4.165,"xxxx35_U_Actl":4.16,"xxxx36_U_Actl":4.163,"xxxx37_U_Actl":4.161,"xxxx38_U_Actl":4.165,"xxxx39_U_Actl":4.161,"xxxx40_U_Actl":4.162,"xxxx41_U_Actl":4.162,"xxxx42_U_Actl":4.164,"xxxx43_U_Actl":4.169,"xxxx44_U_Actl":4.167,"xxxx45_U_Actl":4.164,"xxxx46_U_Actl":4.161,"xxxx47_U_Actl":4.164,"xxxx48_U_Actl":4.161,"xxxx49_U_Actl":4.161,"xxxx50_U_Actl":4.164,"xxxx51_U_Actl":4.163,"xxxx52_U_Actl":4.164,"xxxx53_U_Actl":4.167,"xxxx54_U_Actl":4.163,"xxxx55_U_Actl":4.163,"xxxx56_U_Actl":4.165,"xxxx57_U_Actl":4.165,"xxxx58_U_Actl":4.164,"xxxx59_U_Actl":4.163,"xxxx60_U_Actl":4.163,"xxxx61_U_Actl":4.163,"xxxx62_U_Actl":4.163,"xxxx63_U_Actl":4.163,"xxxx64_U_Actl":4.164,"xxxx65_U_Actl":4.163,"xxxx66_U_Actl":4.164,"xxxx67_U_Actl":4.16,"xxxx68_U_Actl":4.163,"xxxx69_U_Actl":4.163,"xxxx70_U_Actl":4.163,"xxxx71_U_Actl":4.162,"xxxx72_U_Actl":4.162,"xxxx73_U_Actl":4.163,"xxxx74_U_Actl":4.162,"xxxx75_U_Actl":4.162,"xxxx76_U_Actl":4.163,"xxxx77_U_Actl":4.163,"xxxx78_U_Actl":4.163,"xxxx79_U_Actl":4.166,"xxxx80_U_Actl":4.164,"xxxx81_U_Actl":4.164,"xxxx82_U_Actl":4.164,"xxxx83_U_Actl":4.167,"xxxx84_U_Actl":4.169},"k2225":12,"k2226":7,"k2227":2,"k2228":{"k22281":33,"k22282":32,"k22283":32,"k22284":32,"k22285":34,"k22286":33,"k22287":35,"k22288":34,"k22289":33}},"k223":{"k2231":"Run"}}}')"""

    createMV ("""
            create materialized view tcu_test_index as
            select
                a as vin
                ,(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')-greatest(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl'))-least(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')))/1 as avg
                ,abs((json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')-greatest(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl'))-least(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')))/1 - greatest(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl'))) max_abs
                ,abs((json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')-greatest(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl'))-least(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')))/1 - least(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl'))) min_abs
                ,greatest(abs((json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')-greatest(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl'))-least(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')))/1 - greatest(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl'))),abs((json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')-greatest(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl'))-least(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')))/1 - least(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')))) as rs_abs_max FROM tcu_test ;""")

    sql """insert into tcu_test values('vin78215KHVB','2023-08-23 06:47:14','2023-08-23 06:47:15','2023-08-23 06:47:15','2023-08-23 00:00:00','MessageName','MessageType','referenceId','ActivityID','ProtofileVersion','{"k22":{"k221":{"k2211":2023,"k2212":8,"k2213":23,"k2214":6,"k2215":47,"k2216":14},"k222":{"k2221":84,"k2222":43,"k2223":3,"k2224":{"xxxx01_U_Actl":4.164,"xxxx02_U_Actl":4.163,"xxxx03_U_Actl":4.155,"xxxx04_U_Actl":4.164,"xxxx05_U_Actl":4.162,"xxxx06_U_Actl":4.159,"xxxx07_U_Actl":4.16,"xxxx08_U_Actl":4.162,"xxxx09_U_Actl":4.162,"xxxx10_U_Actl":4.164,"xxxx11_U_Actl":4.161,"xxxx12_U_Actl":4.162,"xxxx13_U_Actl":4.159,"xxxx14_U_Actl":4.159,"xxxx15_U_Actl":4.162,"xxxx16_U_Actl":4.163,"xxxx17_U_Actl":4.163,"xxxx18_U_Actl":4.159,"xxxx19_U_Actl":4.161,"xxxx20_U_Actl":4.159,"xxxx21_U_Actl":4.163,"xxxx22_U_Actl":4.161,"xxxx23_U_Actl":4.164,"xxxx24_U_Actl":4.161,"xxxx25_U_Actl":4.16,"xxxx26_U_Actl":4.161,"xxxx27_U_Actl":4.161,"xxxx28_U_Actl":4.164,"xxxx29_U_Actl":4.162,"xxxx30_U_Actl":4.161,"xxxx31_U_Actl":4.16,"xxxx32_U_Actl":4.162,"xxxx33_U_Actl":4.162,"xxxx34_U_Actl":4.165,"xxxx35_U_Actl":4.16,"xxxx36_U_Actl":4.163,"xxxx37_U_Actl":4.161,"xxxx38_U_Actl":4.165,"xxxx39_U_Actl":4.161,"xxxx40_U_Actl":4.162,"xxxx41_U_Actl":4.162,"xxxx42_U_Actl":4.164,"xxxx43_U_Actl":4.169,"xxxx44_U_Actl":4.167,"xxxx45_U_Actl":4.164,"xxxx46_U_Actl":4.161,"xxxx47_U_Actl":4.164,"xxxx48_U_Actl":4.161,"xxxx49_U_Actl":4.161,"xxxx50_U_Actl":4.164,"xxxx51_U_Actl":4.163,"xxxx52_U_Actl":4.164,"xxxx53_U_Actl":4.167,"xxxx54_U_Actl":4.163,"xxxx55_U_Actl":4.163,"xxxx56_U_Actl":4.165,"xxxx57_U_Actl":4.165,"xxxx58_U_Actl":4.164,"xxxx59_U_Actl":4.163,"xxxx60_U_Actl":4.163,"xxxx61_U_Actl":4.163,"xxxx62_U_Actl":4.163,"xxxx63_U_Actl":4.163,"xxxx64_U_Actl":4.164,"xxxx65_U_Actl":4.163,"xxxx66_U_Actl":4.164,"xxxx67_U_Actl":4.16,"xxxx68_U_Actl":4.163,"xxxx69_U_Actl":4.163,"xxxx70_U_Actl":4.163,"xxxx71_U_Actl":4.162,"xxxx72_U_Actl":4.162,"xxxx73_U_Actl":4.163,"xxxx74_U_Actl":4.162,"xxxx75_U_Actl":4.162,"xxxx76_U_Actl":4.163,"xxxx77_U_Actl":4.163,"xxxx78_U_Actl":4.163,"xxxx79_U_Actl":4.166,"xxxx80_U_Actl":4.164,"xxxx81_U_Actl":4.164,"xxxx82_U_Actl":4.164,"xxxx83_U_Actl":4.167,"xxxx84_U_Actl":4.169},"k2225":12,"k2226":7,"k2227":2,"k2228":{"k22281":33,"k22282":32,"k22283":32,"k22284":32,"k22285":34,"k22286":33,"k22287":35,"k22288":34,"k22289":33}},"k223":{"k2231":"Run"}}}')"""

    sql """ drop view IF EXISTS tcu_test_view ; """

    sql """ create view IF NOT EXISTS tcu_test_view
        (vin,avg,max_abs,min_abs,rs_abs_max) as select
    a as vin
     ,(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')-greatest(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl'))-least(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')))/1 as avg
     ,abs((json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')-greatest(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl'))-least(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')))/1 - greatest(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl'))) max_abs
     ,abs((json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')-greatest(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl'))-least(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')))/1 - least(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl'))) min_abs
     ,greatest(abs((json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')-greatest(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl'))-least(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')))/1 - greatest(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl'))),abs((json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl')+json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')-greatest(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl'))-least(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')))/1 - least(json_extract(k, '\$.k22.k222.k2224.xxxx01_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_U_Actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_U_Actl')))) as rs_abs_max FROM tcu_test ;
	  """

    // TODO reopen it when we could fix it in right way
    // explain {
    //     sql("select * from tcu_test_view;")
    //     contains "(tcu_test_index)"
    // }
    qt_select_mv "select * from tcu_test_view;"
}
