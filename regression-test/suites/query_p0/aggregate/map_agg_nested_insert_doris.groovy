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
            (1, "LA", [0.3581592, 0.0011899, 0.2874921, 0.9351293, 0.15525849, 0.733089], {"UsPMIs-ipxhEnU-1EG-RJpe":0.227416, "gcn7fm-ILbMhQ6-fcz-TndT":0.36370, "o8dgBn-1bM26Wz-8SJ-xW6u":0.556716, "wav6ZA-780SwLJ-Vj3-KCv5":0.866518}),
            (1, "LB", [0.2507620, 0.5400361, 0.7066116, 0.9947289, 0.07831494], {"UsPMIs-ipxhEnU-1EG-RJpe":0.227416, "gcn7fm-ILbMhQ6-fcz-TndT":0.363702, "o8dgBn-1bM26Wz-8SJ-xW6u":0.55671, "wav6ZA-780SwLJ-Vj3-KCv5":0.86651}),
            (1, "LC", [0.2096140, 0.6779635, 0.0887453, 0.1266036, 0.50555432], {"RelKZ3-etM12wN-uP4-XR7Z":0.289281, "FEpfM2-MGtIq1y-Ily-hqLN":0.759823}),
            (2, "LA", [], {"iK1yJB-yuEElEZ-li4-vxAT":0.4389480, "4kF6cZ-Cwlf33s-eYD-92Dy":0.97378}),
            (2, "LB", [NULL, 1.22344], {NULL: 0.6924}),
            (2, "LC", [222,NULL], {"sUMCB1-rKPSp71-F9O-PyDS": NULL}),
            (3, "LA", [0.2096140, 0.6779635], {NULL:NULL, "FEpfM2-MGtIq1y-Ily-hqLN":0.7598, "Dy4lap-sNXoyV3-LBz-ikjw":0.33531}),
            (3, "LB", [0.2096140,0], {"4kF6cZ-Cwlf33s-eYD-92Dy":0.97378, "AyE8nj-ecfHXG1-XuB-ye3B":0.696716, "CwYJPz-oSaS398-CfV-MOLZ":0.94225, "xMk5Ob-lCuzA3R-yXs-NLnc":0.31773}),
            (3, "LC", [1.], {"" : 0.69249, "sUMCB1-rKPSp71-F9O-S":NULL}),
            (4, "LA", [0.39464, 0.68089, 0.17672, 0.0757435], {"kduA0V-rqJ4ga1-mvG-p3OK":0.0980960, "xpDNOl-iTcYRWk-6Ak-9smj":0.813354}),
            (4, "LB", [1.11, 2.22, 3.33], {"amory": 7, "doris": 6}),
            (4, "LC", [4.44, 5.55, 6.66, 7.77], {"a": 77, "d": 66}),
            (5, "LA", [1234.3456, 345.3453456], {"":NULL}),
            (5, "LB", [0.688170, 0.5886670, 0.97706], {"CDuRMK-JH8MfuY-ogq-QiQS":0.0615758, "3WYdO0-XltW1HK-hjM-LNlz":0.8616, "SxBfEK-pIiMQV6-qwZ-B2rj":0.80341}),
            (5, "LC", [1, 3, 7], {"a": 3, "up": 7});
    """

    qt_sql """ select * from `${tb_base}` order by id;"""

    sql """ insert into `${tb_doris}` select id, map_agg(label_name, a) from `${tb_base}` group by id; """

    qt_sql """ select * from `${tb_doris}` order by id;"""

    sql """ drop table `${tb_base}`; """
    sql """ drop table `${tb_doris}`; """

 }
