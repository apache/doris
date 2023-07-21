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

suite("test_view") {
    sql "use test_query_db"
    sql "drop table if exists test_insert"
    sql '''
        CREATE TABLE `test_insert` (
          `id` varchar(11) NULL,
          `name` varchar(10) NULL,
          `age` int(11) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT 'test\'
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false"
        );
    '''

    sql '''
        INSERT INTO `test_insert` (`id`, `name`, `age`) VALUES 
            ('10006', 'doris', 10006), ('10025', 'doris', 10025), ('10031', 'doris', 10031), ('10046', 'doris', 10046),
            ('10051', 'doris', 10051), ('10070', 'doris', 10070), ('10072', 'doris', 10072), ('10079', 'doris', 10079),
            ('10085', 'doris', 10085), ('10086', 'doris', 10086), ('10099', 'doris', 10099), ('10101', 'doris', 10101),
            ('10115', 'doris', 10115), ('10140', 'doris', 10140), ('10142', 'doris', 10142), ('10149', 'doris', 10149),
            ('10155', 'doris', 10155), ('10156', 'doris', 10156), ('10157', 'doris', 10157), ('10176', 'doris', 10176),
            ('10183', 'doris', 10183), ('10196', 'doris', 10196), ('10204', 'doris', 10204), ('10233', 'doris', 10233),
            ('10238', 'doris', 10238), ('10247', 'doris', 10247), ('10264', 'doris', 10264), ('10265', 'doris', 10265),
            ('1027', 'doris', 1027), ('10270', 'doris', 10270), ('10271', 'doris', 10271), ('10302', 'doris', 10302),
            ('10314', 'doris', 10314), ('10316', 'doris', 10316), ('10320', 'doris', 10320), ('10321', 'doris', 10321),
            ('10328', 'doris', 10328), ('10343', 'doris', 10343), ('10360', 'doris', 10360), ('10374', 'doris', 10374),
            ('10377', 'doris', 10377), ('1038', 'doris', 1038), ('10381', 'doris', 10381), ('10382', 'doris', 10382),
            ('10407', 'doris', 10407), ('10411', 'doris', 10411), ('10424', 'doris', 10424), ('10426', 'doris', 10426),
            ('10427', 'doris', 10427), ('10431', 'doris', 10431), ('10459', 'doris', 10459), ('10466', 'doris', 10466),
            ('1047', 'doris', 1047), ('10534', 'doris', 10534), ('10582', 'doris', 10582), ('10589', 'doris', 10589),
            ('10594', 'doris', 10594), ('10597', 'doris', 10597), ('10605', 'doris', 10605), ('10606', 'doris', 10606),
            ('10607', 'doris', 10607), ('10619', 'doris', 10619), ('10625', 'doris', 10625), ('10631', 'doris', 10631),
            ('10633', 'doris', 10633), ('10638', 'doris', 10638), ('1065', 'doris', 1065), ('10651', 'doris', 10651),
            ('1067', 'doris', 1067), ('10678', 'doris', 10678), ('10686', 'doris', 10686), ('10687', 'doris', 10687),
            ('10692', 'doris', 10692), ('10699', 'doris', 10699), ('1070', 'doris', 1070), ('10709', 'doris', 10709),
            ('10716', 'doris', 10716), ('10728', 'doris', 10728), ('10729', 'doris', 10729), ('10741', 'doris', 10741),
            ('10743', 'doris', 10743), ('10760', 'doris', 10760), ('10776', 'doris', 10776), ('1078', 'doris', 1078),
            ('10795', 'doris', 10795), ('108', 'doris', 108), ('10811', 'doris', 10811), ('10844', 'doris', 10844),
            ('10846', 'doris', 10846), ('10864', 'doris', 10864), ('10873', 'doris', 10873), ('10879', 'doris', 10879),
            ('10886', 'doris', 10886), ('10899', 'doris', 10899), ('10908', 'doris', 10908), ('1091', 'doris', 1091),
            ('10914', 'doris', 10914), ('10917', 'doris', 10917), ('1092', 'doris', 1092), ('1093', 'doris', 1093),
            ('10934', 'doris', 10934), ('10937', 'doris', 10937), ('10942', 'doris', 10942), ('10948', 'doris', 10948),
            ('10954', 'doris', 10954), ('10955', 'doris', 10955), ('10957', 'doris', 10957), ('10961', 'doris', 10961),
            ('10968', 'doris', 10968), ('10976', 'doris', 10976), ('10977', 'doris', 10977), ('10980', 'doris', 10980),
            ('10981', 'doris', 10981), ('10988', 'doris', 10988), ('10994', 'doris', 10994), ('10997', 'doris', 10997),
            ('11003', 'doris', 11003), ('11016', 'doris', 11016), ('11020', 'doris', 11020), ('11028', 'doris', 11028),
            ('11029', 'doris', 11029), ('11037', 'doris', 11037), ('11041', 'doris', 11041), ('11057', 'doris', 11057),
            ('11081', 'doris', 11081), ('11096', 'doris', 11096), ('11125', 'doris', 11125), ('11139', 'doris', 11139),
            ('11150', 'doris', 11150), ('11164', 'doris', 11164), ('11165', 'doris', 11165), ('11167', 'doris', 11167),
            ('11172', 'doris', 11172), ('11185', 'doris', 11185), ('11192', 'doris', 11192), ('1120', 'doris', 1120),
            ('11203', 'doris', 11203), ('1121', 'doris', 1121), ('11215', 'doris', 11215), ('11217', 'doris', 11217),
            ('11221', 'doris', 11221), ('11229', 'doris', 11229), ('11243', 'doris', 11243), ('11255', 'doris', 11255),
            ('1128', 'doris', 1128), ('11281', 'doris', 11281), ('11283', 'doris', 11283), ('11288', 'doris', 11288),
            ('1129', 'doris', 1129), ('11296', 'doris', 11296), ('11305', 'doris', 11305), ('11307', 'doris', 11307),
            ('11332', 'doris', 11332), ('11347', 'doris', 11347), ('11351', 'doris', 11351), ('11358', 'doris', 11358),
            ('11366', 'doris', 11366), ('11373', 'doris', 11373), ('11379', 'doris', 11379), ('11385', 'doris', 11385),
            ('11390', 'doris', 11390), ('11392', 'doris', 11392), ('11399', 'doris', 11399), ('1140', 'doris', 1140),
            ('11400', 'doris', 11400), ('11402', 'doris', 11402), ('11417', 'doris', 11417), ('11436', 'doris', 11436),
            ('11448', 'doris', 11448), ('11455', 'doris', 11455), ('11460', 'doris', 11460), ('11468', 'doris', 11468),
            ('11469', 'doris', 11469), ('11475', 'doris', 11475), ('1148', 'doris', 1148), ('11488', 'doris', 11488),
            ('11506', 'doris', 11506), ('11518', 'doris', 11518), ('11531', 'doris', 11531), ('11532', 'doris', 11532),
            ('11550', 'doris', 11550), ('11551', 'doris', 11551), ('11592', 'doris', 11592), ('1160', 'doris', 1160),
            ('11603', 'doris', 11603), ('1161', 'doris', 1161), ('11615', 'doris', 11615), ('11617', 'doris', 11617),
            ('11623', 'doris', 11623), ('11629', 'doris', 11629), ('11634', 'doris', 11634), ('11637', 'doris', 11637),
            ('11642', 'doris', 11642), ('11656', 'doris', 11656), ('11663', 'doris', 11663), ('11688', 'doris', 11688),
            ('11689', 'doris', 11689), ('1169', 'doris', 1169), ('117', 'doris', 117), ('11710', 'doris', 11710),
            ('11711', 'doris', 11711)
    '''

    sql "drop view if exists v"
    sql "CREATE VIEW v (id, name, age) AS SELECT id, name, age FROM test_insert order by age desc limit 2;"

    test {
        sql "select * from v"
        result([
                ['11711', 'doris', 11711],
                ['11710', 'doris', 11710]
        ])
    }
}