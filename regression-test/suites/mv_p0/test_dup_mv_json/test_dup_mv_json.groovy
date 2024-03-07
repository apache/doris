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

suite ("test_dup_mv_json") {
    sql """set enable_nereids_planner=true"""
    sql """SET enable_fallback_to_original_planner=false"""
    sql """ DROP TABLE IF EXISTS tcu_test; """

    sql """
        CREaTE TaBLE `tcu_test` (
        `a` varchar(50) NuLL,
        `b` datetime NuLL,
        `c` datetime NuLL,
        `d` datetime NuLL,
        `e` datetime NuLL,
        `f` text NuLL,
        `g` text NuLL,
        `h` text NuLL,
        `i` text NuLL,
        `j` text NuLL,
        `k` text NuLL
        ) ENGINE = OLaP DuPLICaTE KEY(`a`) COMMENT 'OLaP' PaRTITION BY RaNGE(`e`) (
        PaRTITION p20230822
        VaLuES
            [('2023-08-22 00:00:00'), ('2023-08-23 00:00:00')), PaRTITION p20230823 VaLuES [('2023-08-23 00:00:00'), ('2023-08-24 00:00:00')), PaRTITION p20230824 VaLuES [('2023-08-24 00:00:00'), ('2023-08-25 00:00:00')), PaRTITION p20230825 VaLuES [('2023-08-25 00:00:00'), ('2023-08-26 00:00:00')), PaRTITION p20230826 VaLuES [('2023-08-26 00:00:00'), ('2023-08-27 00:00:00'))) DISTRIBuTED BY HaSH(`a`) BuCKETS 20 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "is_being_synced" = "false") ;
        """

    sql """insert into tcu_test values('vin78215KHVB','2023-08-23 06:47:14','2023-08-23 06:47:15','2023-08-23 06:47:15','2023-08-23 00:00:00','MessageName','MessageType','referenceId','activityID','ProtofileVersion','{"k22":{"k221":{"k2211":2023,"k2212":8,"k2213":23,"k2214":6,"k2215":47,"k2216":14},"k222":{"k2221":84,"k2222":43,"k2223":3,"k2224":{"xxxx01_u_actl":4.164,"xxxx02_u_actl":4.163,"xxxx03_u_actl":4.155,"xxxx04_u_actl":4.164,"xxxx05_u_actl":4.162,"xxxx06_u_actl":4.159,"xxxx07_u_actl":4.16,"xxxx08_u_actl":4.162,"xxxx09_u_actl":4.162,"xxxx10_u_actl":4.164,"xxxx11_u_actl":4.161,"xxxx12_u_actl":4.162,"xxxx13_u_actl":4.159,"xxxx14_u_actl":4.159,"xxxx15_u_actl":4.162,"xxxx16_u_actl":4.163,"xxxx17_u_actl":4.163,"xxxx18_u_actl":4.159,"xxxx19_u_actl":4.161,"xxxx20_u_actl":4.159,"xxxx21_u_actl":4.163,"xxxx22_u_actl":4.161,"xxxx23_u_actl":4.164,"xxxx24_u_actl":4.161,"xxxx25_u_actl":4.16,"xxxx26_u_actl":4.161,"xxxx27_u_actl":4.161,"xxxx28_u_actl":4.164,"xxxx29_u_actl":4.162,"xxxx30_u_actl":4.161,"xxxx31_u_actl":4.16,"xxxx32_u_actl":4.162,"xxxx33_u_actl":4.162,"xxxx34_u_actl":4.165,"xxxx35_u_actl":4.16,"xxxx36_u_actl":4.163,"xxxx37_u_actl":4.161,"xxxx38_u_actl":4.165,"xxxx39_u_actl":4.161,"xxxx40_u_actl":4.162,"xxxx41_u_actl":4.162,"xxxx42_u_actl":4.164,"xxxx43_u_actl":4.169,"xxxx44_u_actl":4.167,"xxxx45_u_actl":4.164,"xxxx46_u_actl":4.161,"xxxx47_u_actl":4.164,"xxxx48_u_actl":4.161,"xxxx49_u_actl":4.161,"xxxx50_u_actl":4.164,"xxxx51_u_actl":4.163,"xxxx52_u_actl":4.164,"xxxx53_u_actl":4.167,"xxxx54_u_actl":4.163,"xxxx55_u_actl":4.163,"xxxx56_u_actl":4.165,"xxxx57_u_actl":4.165,"xxxx58_u_actl":4.164,"xxxx59_u_actl":4.163,"xxxx60_u_actl":4.163,"xxxx61_u_actl":4.163,"xxxx62_u_actl":4.163,"xxxx63_u_actl":4.163,"xxxx64_u_actl":4.164,"xxxx65_u_actl":4.163,"xxxx66_u_actl":4.164,"xxxx67_u_actl":4.16,"xxxx68_u_actl":4.163,"xxxx69_u_actl":4.163,"xxxx70_u_actl":4.163,"xxxx71_u_actl":4.162,"xxxx72_u_actl":4.162,"xxxx73_u_actl":4.163,"xxxx74_u_actl":4.162,"xxxx75_u_actl":4.162,"xxxx76_u_actl":4.163,"xxxx77_u_actl":4.163,"xxxx78_u_actl":4.163,"xxxx79_u_actl":4.166,"xxxx80_u_actl":4.164,"xxxx81_u_actl":4.164,"xxxx82_u_actl":4.164,"xxxx83_u_actl":4.167,"xxxx84_u_actl":4.169},"k2225":12,"k2226":7,"k2227":2,"k2228":{"k22281":33,"k22282":32,"k22283":32,"k22284":32,"k22285":34,"k22286":33,"k22287":35,"k22288":34,"k22289":33}},"k223":{"k2231":"Run"}}}');"""


    createMV ("""create materialized view tcu_test_index as
                select
                    a
                    ,(json_extract(k, '\$.k22.k222.k2224.xxxx01_u_actl')+json_extract(k, '\$.k22.k222.k2224.xxxx02_u_actl')+json_extract(k, '\$.k22.k222.k2224.xxxx03_u_actl')-greatest(json_extract(k, '\$.k22.k222.k2224.xxxx01_u_actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_u_actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_u_actl'))-least(json_extract(k, '\$.k22.k222.k2224.xxxx01_u_actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_u_actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_u_actl')))/1.0
                    FROM tcu_test;
            """
    )

    sql """insert into tcu_test values('vin78215KHVB','2023-08-23 06:47:14','2023-08-23 06:47:15','2023-08-23 06:47:15','2023-08-23 00:00:00','MessageName','MessageType','referenceId','activityID','ProtofileVersion','{"k22":{"k221":{"k2211":2023,"k2212":8,"k2213":23,"k2214":6,"k2215":47,"k2216":14},"k222":{"k2221":84,"k2222":43,"k2223":3,"k2224":{"xxxx01_u_actl":4.164,"xxxx02_u_actl":4.163,"xxxx03_u_actl":4.155,"xxxx04_u_actl":4.164,"xxxx05_u_actl":4.162,"xxxx06_u_actl":4.159,"xxxx07_u_actl":4.16,"xxxx08_u_actl":4.162,"xxxx09_u_actl":4.162,"xxxx10_u_actl":4.164,"xxxx11_u_actl":4.161,"xxxx12_u_actl":4.162,"xxxx13_u_actl":4.159,"xxxx14_u_actl":4.159,"xxxx15_u_actl":4.162,"xxxx16_u_actl":4.163,"xxxx17_u_actl":4.163,"xxxx18_u_actl":4.159,"xxxx19_u_actl":4.161,"xxxx20_u_actl":4.159,"xxxx21_u_actl":4.163,"xxxx22_u_actl":4.161,"xxxx23_u_actl":4.164,"xxxx24_u_actl":4.161,"xxxx25_u_actl":4.16,"xxxx26_u_actl":4.161,"xxxx27_u_actl":4.161,"xxxx28_u_actl":4.164,"xxxx29_u_actl":4.162,"xxxx30_u_actl":4.161,"xxxx31_u_actl":4.16,"xxxx32_u_actl":4.162,"xxxx33_u_actl":4.162,"xxxx34_u_actl":4.165,"xxxx35_u_actl":4.16,"xxxx36_u_actl":4.163,"xxxx37_u_actl":4.161,"xxxx38_u_actl":4.165,"xxxx39_u_actl":4.161,"xxxx40_u_actl":4.162,"xxxx41_u_actl":4.162,"xxxx42_u_actl":4.164,"xxxx43_u_actl":4.169,"xxxx44_u_actl":4.167,"xxxx45_u_actl":4.164,"xxxx46_u_actl":4.161,"xxxx47_u_actl":4.164,"xxxx48_u_actl":4.161,"xxxx49_u_actl":4.161,"xxxx50_u_actl":4.164,"xxxx51_u_actl":4.163,"xxxx52_u_actl":4.164,"xxxx53_u_actl":4.167,"xxxx54_u_actl":4.163,"xxxx55_u_actl":4.163,"xxxx56_u_actl":4.165,"xxxx57_u_actl":4.165,"xxxx58_u_actl":4.164,"xxxx59_u_actl":4.163,"xxxx60_u_actl":4.163,"xxxx61_u_actl":4.163,"xxxx62_u_actl":4.163,"xxxx63_u_actl":4.163,"xxxx64_u_actl":4.164,"xxxx65_u_actl":4.163,"xxxx66_u_actl":4.164,"xxxx67_u_actl":4.16,"xxxx68_u_actl":4.163,"xxxx69_u_actl":4.163,"xxxx70_u_actl":4.163,"xxxx71_u_actl":4.162,"xxxx72_u_actl":4.162,"xxxx73_u_actl":4.163,"xxxx74_u_actl":4.162,"xxxx75_u_actl":4.162,"xxxx76_u_actl":4.163,"xxxx77_u_actl":4.163,"xxxx78_u_actl":4.163,"xxxx79_u_actl":4.166,"xxxx80_u_actl":4.164,"xxxx81_u_actl":4.164,"xxxx82_u_actl":4.164,"xxxx83_u_actl":4.167,"xxxx84_u_actl":4.169},"k2225":12,"k2226":7,"k2227":2,"k2228":{"k22281":33,"k22282":32,"k22283":32,"k22284":32,"k22285":34,"k22286":33,"k22287":35,"k22288":34,"k22289":33}},"k223":{"k2231":"Run"}}}');"""

    qt_select_star "select * from tcu_test;"

    // TODO reopen it when we could fix it in right way
    // explain {
    //     sql("""select a
    //  ,(json_extract(k, '\$.k22.k222.k2224.xxxx01_u_actl')+json_extract(k, '\$.k22.k222.k2224.xxxx02_u_actl')+json_extract(k, '\$.k22.k222.k2224.xxxx03_u_actl')-greatest(json_extract(k, '\$.k22.k222.k2224.xxxx01_u_actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_u_actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_u_actl'))-least(json_extract(k, '\$.k22.k222.k2224.xxxx01_u_actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_u_actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_u_actl')))/1.0
    //  FROM tcu_test;""")
    //     contains "(tcu_test_index)"
    // }
    qt_select_mv """select a
     ,(json_extract(k, '\$.k22.k222.k2224.xxxx01_u_actl')+json_extract(k, '\$.k22.k222.k2224.xxxx02_u_actl')+json_extract(k, '\$.k22.k222.k2224.xxxx03_u_actl')-greatest(json_extract(k, '\$.k22.k222.k2224.xxxx01_u_actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_u_actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_u_actl'))-least(json_extract(k, '\$.k22.k222.k2224.xxxx01_u_actl'),json_extract(k, '\$.k22.k222.k2224.xxxx02_u_actl'),json_extract(k, '\$.k22.k222.k2224.xxxx03_u_actl')))/1.0
     FROM tcu_test ;"""
}
