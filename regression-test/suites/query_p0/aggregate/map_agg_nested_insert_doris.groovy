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

suite("map_agg_nested_insert_doris", "p0") {
    def tb_base = "test_map_agg_nested_insert_base"
    def tb_doris = "test_map_agg_nested_insert_target"
    sql "DROP TABLE IF EXISTS `${tb_base}`;"
    sql "DROP TABLE IF EXISTS `${tb_doris}`;"
    sql """ ADMIN SET FRONTEND CONFIG ('disable_nested_complex_type' = 'false'); """

    sql """
        CREATE TABLE `${tb_base}` (
            `id` int(11) NOT NULL,
            `label_name` varchar(32) NOT NULL,
            `a` ARRAY<DOUBLE> NOT NULL,
            `m` MAP<STRING, DOUBLE>
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
     """

    sql """
        CREATE TABLE `${tb_doris}` (
            `id` int(11) NOT NULL,
            `m_a` MAP<STRING, ARRAY<DOUBLE>> NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
     """

    sql """
        insert into `${tb_base}` values
            (1, "LA", [0.35815922932906263, 0.0011899152357573994, 0.28749219850167373, 0.93512930168283781, 0.1552584991620739, 0.733089], {"UsPMIs-ipxhEnU-1EG-RJpe":0.22741640776012562, "gcn7fm-ILbMhQ6-fcz-TndT":0.36370276228098763, "o8dgBn-1bM26Wz-8SJ-xW6u":0.55671646501523719, "wav6ZA-780SwLJ-Vj3-KCv5":0.8665187582647581}),
            (1, "LB", [0.25076205844319044, 0.54003619330849928, 0.70661164863002113, 0.99472899095144263, 0.07831494], {"UsPMIs-ipxhEnU-1EG-RJpe":0.22741640776012562, "gcn7fm-ILbMhQ6-fcz-TndT":0.36370276228098763, "o8dgBn-1bM26Wz-8SJ-xW6u":0.55671646501523719, "wav6ZA-780SwLJ-Vj3-KCv5":0.8665187582647581}),
            (1, "LC", [0.20961408068486054, 0.67796351365684493, 0.088745389947127551, 0.12660368488966578, 0.50555432], {"RelKZ3-etM12wN-uP4-XR7Z":0.28928123482977663, "FEpfM2-MGtIq1y-Ily-hqLN":0.75982378533995065}),
            (2, "LA", [], {"iK1yJB-yuEElEZ-li4-vxAT":0.43894802430489277, "4kF6cZ-Cwlf33s-eYD-92Dy":0.97378869646008837}),
            (2, "LB", [NULL, 1.22344], {NULL: 0.69249939434627483}),
            (2, "LC", [222,NULL], {"sUMCB1-rKPSp71-F9O-PyDS": NULL}),
            (3, "LA", [0.20961408068486054, 0.67796351365684493], {NULL:NULL, "FEpfM2-MGtIq1y-Ily-hqLN":0.75982378533995065, "Dy4lap-sNXoyV3-LBz-ikjw":0.3353160677758501}),
            (3, "LB", [0.20961408068486054,0], {"4kF6cZ-Cwlf33s-eYD-92Dy":0.97378869646008837, "AyE8nj-ecfHXG1-XuB-ye3B":0.69671634023926254, "CwYJPz-oSaS398-CfV-MOLZ":0.9422562276573786, "xMk5Ob-lCuzA3R-yXs-NLnc":0.31773598018625049}),
            (3, "LC", [1.], {"" : 0.69249939434627483, "sUMCB1-rKPSp71-F9O-S":NULL}),
            (4, "LA", [0.39467746014773986, 0.68089927382163351, 0.17672102367531073, 0.075743536456418625], {"kduA0V-rqJ4ga1-mvG-p3OK":0.0980960662977799, "xpDNOl-iTcYRWk-6Ak-9smj":0.81335421914074846}),
            (4, "LB", [1.11, 2.22, 3.33], {"amory": 7, "doris": 6}),
            (4, "LC", [4.44, 5.55, 6.66, 7.77], {"a": 77, "d": 66}),
            (5, "LA", [1234.3456, 345.3453456], {"":NULL}),
            (5, "LB", [0.68817051068356083, 0.58866705765486893, 0.9770647663621238], {"CDuRMK-JH8MfuY-ogq-QiQS":0.061575881523062148, "3WYdO0-XltW1HK-hjM-LNlz":0.86164972492374081, "SxBfEK-pIiMQV6-qwZ-B2rj":0.80341434305286941}),
            (5, "LC", [1, 3, 7], {"a": 3, "up": 7});
    """

    qt_sql """ select * from `${tb_base}` order by id;"""

    sql """ insert into `${tb_doris}` select id, map_agg(label_name, a) from `${tb_base}` group by id; """

    qt_sql """ select * from `${tb_doris}` order by id;"""

    sql """ drop table `${tb_base}`; """
    sql """ drop table `${tb_doris}`; """

 }
