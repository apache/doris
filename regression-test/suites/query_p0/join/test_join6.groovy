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

suite("test_join6", "query,p0") {
    def DBname = "regression_test_join6"
    sql "DROP DATABASE IF EXISTS ${DBname}"
    sql "CREATE DATABASE IF NOT EXISTS ${DBname}"
    sql "use ${DBname}"

    def tbName1 = "table_20_undef_partitions2_keys3_properties4_distributed_by5"
    def tbName2 = "table_20_undef_partitions2_keys3_properties4_distributed_by52"
    def tbName3 = "table_30_undef_partitions2_keys3_properties4_distributed_by5"
    def tbName4 = "table_30_undef_partitions2_keys3_properties4_distributed_by52"
    def tbName5 = "table_50_undef_partitions2_keys3_properties4_distributed_by5"
    def tbName6 = "table_50_undef_partitions2_keys3_properties4_distributed_by52"
    def tbName7 = "table_100_undef_partitions2_keys3_properties4_distributed_by5"
    def tbName8 = "table_100_undef_partitions2_keys3_properties4_distributed_by52"
    def tbName9 = "table_200_undef_partitions2_keys3_properties4_distributed_by5"
    def tbName10 = "table_200_undef_partitions2_keys3_properties4_distributed_by52"

    sql "DROP TABLE IF EXISTS ${tbName1};"
    sql "DROP TABLE IF EXISTS ${tbName2};"
    sql "DROP TABLE IF EXISTS ${tbName3};"
    sql "DROP TABLE IF EXISTS ${tbName4};"
    sql "DROP TABLE IF EXISTS ${tbName5};"
    sql "DROP TABLE IF EXISTS ${tbName6};"
    sql "DROP TABLE IF EXISTS ${tbName7};"
    sql "DROP TABLE IF EXISTS ${tbName8};"
    sql "DROP TABLE IF EXISTS ${tbName9};"
    sql "DROP TABLE IF EXISTS ${tbName10};"

    sql """
        create table table_20_undef_partitions2_keys3_properties4_distributed_by5 (
        col_int_undef_signed2 int    ,
        col_int_undef_signed int    ,
        col_int_undef_signed3 int    ,
        col_int_undef_signed4 int    ,
        pk int
        ) engine=olap
        DUPLICATE KEY(col_int_undef_signed2)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """
    sql """
        insert into table_20_undef_partitions2_keys3_properties4_distributed_by5(pk,col_int_undef_signed,col_int_undef_signed2,col_int_undef_signed3,col_int_undef_signed4) values (0,6,4,-3343259,7),(1,null,2,-5659896,0),(2,2,2369913,-5247778,-4711382),(3,6545002,3,2,4),(4,9,3,4,5),(5,4,5,4,1),(6,4,-4704791,null,6),(7,null,3,null,9),(8,-1012411,4,null,-1244656),(9,1,8,9,-5175872),(10,8,0,-4239951,2),(11,8,-2231762,4817469,2),(12,9,9,5,-427963),(13,4,0,null,-5587539),(14,-5949786,2,2,3432246),(15,1009336,1,null,9),(16,1,3,8,6),(17,1,null,-5766728,6),(18,4,6,9,-2808412),(19,2699189,6,4,2);
    """

    sql """
        create table table_20_undef_partitions2_keys3_properties4_distributed_by52 (
        col_int_undef_signed int    ,
        col_int_undef_signed2 int    ,
        col_int_undef_signed3 int    ,
        col_int_undef_signed4 int    ,
        pk int
        ) engine=olap
        DUPLICATE KEY(col_int_undef_signed, col_int_undef_signed2)
        PARTITION BY             RANGE(col_int_undef_signed) (
                        PARTITION p0 VALUES LESS THAN ('4'),
                        PARTITION p1 VALUES LESS THAN ('6'),
                        PARTITION p2 VALUES LESS THAN ('7'),
                        PARTITION p3 VALUES LESS THAN ('8'),
                        PARTITION p4 VALUES LESS THAN ('10'),
                        PARTITION p5 VALUES LESS THAN ('83647'),
                        PARTITION p100 VALUES LESS THAN ('2147483647')
                    )
                
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """
    sql """
        insert into table_20_undef_partitions2_keys3_properties4_distributed_by52(pk,col_int_undef_signed,col_int_undef_signed2,col_int_undef_signed3,col_int_undef_signed4) values (0,6,-179064,5213411,5),(1,3,5,2,6),(2,4226261,7,null,3),(3,9,null,4,4),(4,-1003770,2,1,1),(5,8,7,null,8176864),(6,3388266,5,8,8),(7,5,1,2,null),(8,9,2064412,0,null),(9,1489553,8,-446412,6),(10,1,3,0,1),(11,null,3,4621304,null),(12,null,-3058026,-262645,9),(13,null,null,9,3),(14,null,null,5037128,7),(15,299896,-1444893,8,1480339),(16,7,7,0,1470826),(17,-7378014,5,null,5),(18,0,3,6,5),(19,5,3,-4403612,-3103249);
    """

    sql """
        create table table_30_undef_partitions2_keys3_properties4_distributed_by5 (
        col_int_undef_signed2 int    ,
        col_int_undef_signed int    ,
        col_int_undef_signed3 int    ,
        col_int_undef_signed4 int    ,
        pk int
        ) engine=olap
        DUPLICATE KEY(col_int_undef_signed2)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """
    sql """
        insert into table_30_undef_partitions2_keys3_properties4_distributed_by5(pk,col_int_undef_signed,col_int_undef_signed2,col_int_undef_signed3,col_int_undef_signed4) values (0,2,null,0,null),(1,-242819,2983243,7071252,3),(2,1,-2342407,-1423905,8),(3,null,null,7,4),(4,-1494065,3,7,2),(5,5,0,-595225,5),(6,5,-3324113,0,5),(7,6829192,3527453,6,5436506),(8,1,-3189592,2,9),(9,null,2,6,2),(10,-4070807,null,-3324205,7),(11,8,-5293967,1,-5040205),(12,6,7440524,null,null),(13,null,2,9,5),(14,4,null,2,-6877636),(15,1,2,5,2642189),(16,null,-5867739,-6454009,8),(17,3,0,2,2),(18,null,null,9,2227146),(19,2,null,0,4),(20,2708363,-7827919,3,null),(21,7,7919706,-6642647,7870064),(22,6,3,7,5),(23,null,null,-1806180,1),(24,6,null,8,4),(25,2,3,0,4),(26,3,-6860149,9,-4519275),(27,7,4,3,-6966921),(28,1,3,0,-981573),(29,0,8,2413602,4);
    """

    sql """
        create table table_30_undef_partitions2_keys3_properties4_distributed_by52 (
        col_int_undef_signed int    ,
        col_int_undef_signed2 int    ,
        col_int_undef_signed3 int    ,
        col_int_undef_signed4 int    ,
        pk int
        ) engine=olap
        DUPLICATE KEY(col_int_undef_signed, col_int_undef_signed2)
        PARTITION BY             RANGE(col_int_undef_signed) (
                        PARTITION p0 VALUES LESS THAN ('4'),
                        PARTITION p1 VALUES LESS THAN ('6'),
                        PARTITION p2 VALUES LESS THAN ('7'),
                        PARTITION p3 VALUES LESS THAN ('8'),
                        PARTITION p4 VALUES LESS THAN ('10'),
                        PARTITION p5 VALUES LESS THAN ('83647'),
                        PARTITION p100 VALUES LESS THAN ('2147483647')
                    )
                
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """
    sql """
        insert into table_30_undef_partitions2_keys3_properties4_distributed_by52(pk,col_int_undef_signed,col_int_undef_signed2,col_int_undef_signed3,col_int_undef_signed4) values (0,9,9,null,1),(1,6821639,9,null,-5431086),(2,8,4,6,7701043),(3,2,-6700938,1425835,7),(4,null,1,3,4),(5,8,8,-714745,null),(6,7,3,4447765,null),(7,1,-2101501,0,5),(8,7,0,9,6),(9,4696294,3,2,-3197661),(10,8,4600901,8,1),(11,-1042936,null,-2187191,0),(12,5116430,0,2687672,9),(13,3,3,8,1287742),(14,-3829647,3,4,7510940),(15,3,6,-6497897,4),(16,3,8,-4319004,0),(17,7,9,null,9),(18,158350,8,3664898,1992801),(19,4,null,2,3867538),(20,4,2339201,7,-4271280),(21,8,0,2,2),(22,-3774459,9,3881435,null),(23,2,-1138190,2037481,-1390541),(24,2,8,7,7),(25,0,2,9,0),(26,-2573578,4,9,5),(27,0,9,1,5),(28,7842282,null,6,-6210668),(29,6,6366889,3,7);
    """

    sql """
        create table table_50_undef_partitions2_keys3_properties4_distributed_by5 (
        col_int_undef_signed2 int    ,
        col_int_undef_signed int    ,
        col_int_undef_signed3 int    ,
        col_int_undef_signed4 int    ,
        pk int
        ) engine=olap
        DUPLICATE KEY(col_int_undef_signed2)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """
    sql """
        insert into table_50_undef_partitions2_keys3_properties4_distributed_by5(pk,col_int_undef_signed,col_int_undef_signed2,col_int_undef_signed3,col_int_undef_signed4) values (0,8,0,3,7),(1,6,227612,4,8),(2,-590975,9,-4411568,6),(3,-7241036,null,3,5),(4,1,7,null,8),(5,2509741,5,5,1),(6,2,9,null,4817793),(7,6,8,3,0),(8,null,1,4,null),(9,711269,null,-613109,null),(10,null,7,0,7),(11,null,-5534845,0,4),(12,5,2,9,6850777),(13,-5789051,8,6,2463068),(14,2,5,953451,1),(15,-6229147,-6738861,4,0),(16,6,8,3548335,1),(17,-3735352,2218004,6,7),(18,9,0,5,8),(19,8,2,6,-758380),(20,7,0,null,6048817),(21,9,8,4,3),(22,2,-4481854,9,null),(23,3653640,null,null,null),(24,null,-7944779,7,null),(25,9,9,2163480,580893),(26,null,2,-7799420,-6345780),(27,3,6950630,-5639142,6),(28,6,2,9,2),(29,3,55558,1,5),(30,-1503616,-2549706,2114904,-1843209),(31,5,8,1,2),(32,5,7,9,9),(33,-5092612,-4128816,3,2743926),(34,2786602,4,1,-5603742),(35,-260462,4,8191134,1),(36,0,1912113,1860687,6),(37,8,0,3690937,5),(38,null,-2269664,5,1),(39,4,5,null,7),(40,8,3288161,8,3120054),(41,8,3,1,1598458),(42,6,null,8031283,-687039),(43,8,3,1,1),(44,1,9,null,8),(45,3,8,3,7876177),(46,-7963243,-1063851,8,-2615276),(47,null,4,6,7357475),(48,null,7584425,-1733070,1),(49,2685134,3986870,5,null);
    """

    sql """
        create table table_50_undef_partitions2_keys3_properties4_distributed_by52 (
        col_int_undef_signed int    ,
        col_int_undef_signed2 int    ,
        col_int_undef_signed3 int    ,
        col_int_undef_signed4 int    ,
        pk int
        ) engine=olap
        DUPLICATE KEY(col_int_undef_signed, col_int_undef_signed2)
        PARTITION BY             RANGE(col_int_undef_signed) (
                        PARTITION p0 VALUES LESS THAN ('4'),
                        PARTITION p1 VALUES LESS THAN ('6'),
                        PARTITION p2 VALUES LESS THAN ('7'),
                        PARTITION p3 VALUES LESS THAN ('8'),
                        PARTITION p4 VALUES LESS THAN ('10'),
                        PARTITION p5 VALUES LESS THAN ('83647'),
                        PARTITION p100 VALUES LESS THAN ('2147483647')
                    )
                
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """
    sql """
        insert into table_50_undef_partitions2_keys3_properties4_distributed_by52(pk,col_int_undef_signed,col_int_undef_signed2,col_int_undef_signed3,col_int_undef_signed4) values (0,-7314662,null,0,4373927),(1,0,9,2,null),(2,5,2151343,-1467194,null),(3,null,null,-6124108,null),(4,5795207,4306466,4,7),(5,6,8,3,9),(6,null,8,-7232808,9),(7,9,6,9,6),(8,4637962,-1241311,2,8),(9,1,2,3,null),(10,0,-1652390,1,3),(11,0,9,6,2),(12,-8342795,0,5539034,-4960208),(13,2768087,7,-6242297,4996873),(14,1,2,1004860,9),(15,7,6,4,3),(16,-5667354,6,-658029,-3453673),(17,2,4,null,6),(18,7316298,4317813,-3103121,6697594),(19,-6020689,null,5,4828114),(20,5213384,6,9,8),(21,-6236361,4,4,8),(22,9,6,-1584500,1),(23,0,-2374298,-3839603,4),(24,4,2277204,1533423,6665310),(25,3,4,-6827212,5),(26,7305845,6618742,5,null),(27,-7139586,8,9,-5562137),(28,-1125938,-7214153,7,3),(29,null,null,2,1),(30,0,8,-6322297,-754270),(31,-2399194,9,-3216746,7),(32,-4869619,8,1161765,-6021329),(33,1,6,1,-2737761),(34,0,2,3,9),(35,7193618,1,0,7),(36,1,2898967,1,6142194),(37,6,2322198,0,1),(38,2081553,3,4,8),(39,6,5348917,0,null),(40,8,-3538489,-8035524,8),(41,9,8,1,-8066548),(42,null,-1868330,-7649411,6),(43,1,5,8,7986791),(44,7,3,5120842,6),(45,3043240,5,2,2),(46,null,null,4600741,5),(47,1,9,-6246097,1),(48,9,1358064,1451574,5),(49,0,9,2731609,8);
    """

    sql """
        create table table_100_undef_partitions2_keys3_properties4_distributed_by5 (
        col_int_undef_signed2 int    ,
        col_int_undef_signed int    ,
        col_int_undef_signed3 int    ,
        col_int_undef_signed4 int    ,
        pk int
        ) engine=olap
        DUPLICATE KEY(col_int_undef_signed2)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """
    sql """
        insert into table_100_undef_partitions2_keys3_properties4_distributed_by5(pk,col_int_undef_signed,col_int_undef_signed2,col_int_undef_signed3,col_int_undef_signed4) values (0,3,7164641,5,8),(1,null,3916062,5,6),(2,1,5533498,0,9),(3,7,2,null,7057679),(4,1,0,7,7),(5,null,4,2448564,1),(6,7531976,7324373,9,7),(7,3,1,1,3),(8,6,8131576,9,-1793807),(9,9,2,4214547,9),(10,-7299852,5,1,3),(11,7,3,-1036551,5),(12,-6108579,84823,4,1229534),(13,-1065629,5,4,null),(14,null,8072633,3328285,2),(15,2,7,6,6),(16,8,5,-4582103,1),(17,5,-4677722,-2379367,4),(18,-7807532,-6686732,0,5329341),(19,8,7,-4013246,-7013374),(20,0,2,9,2),(21,7,2383333,5,4),(22,5844611,2,2,0),(23,0,4756185,0,-5612039),(24,6,4878754,608172,0),(25,null,7858692,7,-6704206),(26,7,-1697597,6,9),(27,9,-7021349,3,-3094786),(28,2,2830915,null,8),(29,4133633,489212,5,9),(30,6,-3346211,3668768,2),(31,1,4862070,-5066405,0),(32,9,6,7,8),(33,2,null,4,2),(34,1,2893430,-3282825,5),(35,2,3,4,2),(36,4,-3418732,6,1263819),(37,5,4,-6342170,6),(38,0,1504502,5,7),(39,7,5,-4314363,7),(40,3,3,3011893,7),(41,8,6200448,3,1270233),(42,7,-3625763,3,7),(43,2230247,3,0,9),(44,7,4,6,-5230722),(45,1115781,null,1557109,9),(46,null,8,null,1),(47,9,3,7,5),(48,5,3141719,null,9),(49,9,null,0,0),(50,null,8,8,-2016458),(51,null,1,3,null),(52,4833633,-7173697,0,-49106),(53,-4274483,1,3,5),(54,2,7,9,9),(55,3,5081308,null,6),(56,7,5,2,5440962),(57,4134482,7,0,-1039760),(58,7,598030,-5395628,1),(59,-3468730,null,9,4),(60,0,-6649977,0,2),(61,0,1,7,1),(62,3,0,2555415,9),(63,null,1,2579183,6),(64,5,8,3,4),(65,null,7628588,null,590278),(66,-7980069,7,8251558,-2343468),(67,5,1,5,-7202142),(68,8,8,6222687,-4593472),(69,2,8,5,3083283),(70,1,7,6929070,3),(71,3,1419679,-3881637,4),(72,1,5,6,4),(73,2,3,-6038388,9),(74,-6424449,0,-7361251,5063185),(75,9,4,null,4),(76,3,-2254219,1,null),(77,7,6,5,0),(78,9,5797500,null,3785274),(79,-856346,6,5,6136879),(80,9,2,4,null),(81,0,-3084158,5131916,3),(82,3440925,3,2,9),(83,8,-2183171,5528033,9),(84,0,-6447705,4,4),(85,3,4201313,1,1833517),(86,-5105048,3,4,-4153030),(87,3282388,null,-4993062,6),(88,3350021,6,-6436291,1),(89,4,6,5,6),(90,2,4,7,-1680630),(91,4643658,-513326,9,3),(92,-320709,8,-6117514,4),(93,4,4872800,5,-930711),(94,0,2012084,null,0),(95,8,8,9,1194098),(96,0,null,2,5),(97,3,3,4,9),(98,5,6,5,2593535),(99,9,2,8,null);
    """


    sql """
        create table table_100_undef_partitions2_keys3_properties4_distributed_by52 (
        col_int_undef_signed int    ,
        col_int_undef_signed2 int    ,
        col_int_undef_signed3 int    ,
        col_int_undef_signed4 int    ,
        pk int
        ) engine=olap
        DUPLICATE KEY(col_int_undef_signed, col_int_undef_signed2)
        PARTITION BY             RANGE(col_int_undef_signed) (
                        PARTITION p0 VALUES LESS THAN ('4'),
                        PARTITION p1 VALUES LESS THAN ('6'),
                        PARTITION p2 VALUES LESS THAN ('7'),
                        PARTITION p3 VALUES LESS THAN ('8'),
                        PARTITION p4 VALUES LESS THAN ('10'),
                        PARTITION p5 VALUES LESS THAN ('83647'),
                        PARTITION p100 VALUES LESS THAN ('2147483647')
                    )
                
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """
    sql """
        insert into table_100_undef_partitions2_keys3_properties4_distributed_by52(pk,col_int_undef_signed,col_int_undef_signed2,col_int_undef_signed3,col_int_undef_signed4) values (0,7865838,-348902,null,8),(1,-9434,9,8,0),(2,1845860,6675073,-7931956,-66007),(3,-7523286,210291,3,4),(4,null,-1341350,-5318642,1),(5,-6634226,2179558,2,7),(6,2,7,2,3),(7,9,2,3,-7773846),(8,0,8,6,2407384),(9,0,1,7,7),(10,5,5,null,8),(11,9,null,8283010,6),(12,7359987,5145929,2,5),(13,0,5225949,0,6770846),(14,1,454548,7,3),(15,4429471,3,6,-3746585),(16,8,1,1,5),(17,-3681554,9,6,9),(18,8,6625564,null,7),(19,7,-1391149,-6540242,3),(20,-4552986,-3094213,2,3),(21,2,-5742470,2,3),(22,1,4,3,-8373664),(23,9,null,0,6),(24,5,8,0,0),(25,null,6,-4716070,1),(26,-2301615,3,-5825636,-2086462),(27,2,8,3,2),(28,3,84928,0,-5660227),(29,6731053,9,null,-5919170),(30,0,7,0,5),(31,6,9,9,8059309),(32,-6393100,null,5,-4967524),(33,3,6,0,-3032361),(34,6,-4864946,-2105421,3),(35,1,-7451745,-2259887,-2089962),(36,0,null,-2422637,null),(37,4,2,6,2923491),(38,7,-5404336,8,6),(39,null,9,9,9),(40,7,2829945,7,8),(41,0,6,null,5553115),(42,4484999,5,5956381,7),(43,0,5,5,9),(44,4,2,9,7618190),(45,null,1258496,5,81600),(46,7,6,3,9),(47,1,7825001,null,9),(48,1551837,1,1013781,6519816),(49,7,8,null,1),(50,5,5773935,7374934,7978090),(51,4678457,5,3,1),(52,1,7,7,null),(53,1,-1803026,3,2909249),(54,-3971388,5,2305082,6),(55,4,7082848,3,null),(56,1,6,8,-89996),(57,0,-6953252,3435902,4),(58,-1725266,1,6,7),(59,3,2,5,2),(60,-5633253,-4222570,-7759796,7),(61,8,1863317,4,3159792),(62,5,2,9,8),(63,5623617,6,-3409569,7240590),(64,3,null,5,-6877359),(65,8,-8364539,7,null),(66,null,0,8,2),(67,5,1,5024672,5),(68,6448944,7,-237775,9),(69,null,4726085,8,3),(70,5,5,7,3220780),(71,9,5,7,8),(72,-2768334,289680,3,null),(73,6,9,-838198,4094997),(74,1,5584313,7214070,0),(75,0,5,663506,4),(76,null,null,5,2),(77,8,0,-1365192,0),(78,-4491579,-3199988,null,7),(79,1,382052,null,-2783713),(80,5,-516120,3,6),(81,7106844,-4368854,4,1),(82,6,4,null,6161227),(83,4,6515928,7,7),(84,-2652991,6420412,-2935612,-554335),(85,7131403,5,4,3),(86,3341348,4825568,1,9),(87,6,6,3,5),(88,-660413,2239981,2,6),(89,7,6,5,-8025658),(90,7,1007386,2740140,4583854),(91,3,null,0,null),(92,2,8163860,1,-992043),(93,2,7,1454321,-822592),(94,7,9,5,0),(95,889343,1549247,7,7164591),(96,8,0,-3972698,-8438),(97,-7575716,0,4,null),(98,2216759,1,2,5),(99,-2206677,8,4,-7081330);
    """

    sql """
        create table table_200_undef_partitions2_keys3_properties4_distributed_by5 (
        col_int_undef_signed2 int    ,
        col_int_undef_signed int    ,
        col_int_undef_signed3 int    ,
        col_int_undef_signed4 int    ,
        pk int
        ) engine=olap
        DUPLICATE KEY(col_int_undef_signed2)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """
    sql """
        insert into table_200_undef_partitions2_keys3_properties4_distributed_by5(pk,col_int_undef_signed,col_int_undef_signed2,col_int_undef_signed3,col_int_undef_signed4) values (0,null,7,3,9),(1,6970022,9,6,2),(2,null,0,null,7262031),(3,4,6,null,7236151),(4,789682,7324018,5,5),(5,-2056178,9,0,0),(6,-7081969,-2103366,0,1),(7,3,5,3,3),(8,3175437,4,6,-2017026),(9,3,null,null,7),(10,-5725039,5,2,3),(11,8,9,2,5),(12,-6487649,1,5,-2847073),(13,3415118,null,4,-6786736),(14,null,4,7,1),(15,995946,-2771253,8,6),(16,0,9,5,8),(17,2,2,-5317956,9),(18,8,9,5,null),(19,-6375971,643844,2,3),(20,8,-1571429,1,-2805449),(21,5,5,9,null),(22,3,-393635,5771753,7),(23,2,1,7,4),(24,-6819994,1,-5139740,-5079454),(25,7,null,9,3),(26,6,-2099215,7,-3798909),(27,-7961233,5380797,3,1),(28,6,6,4,-5144614),(29,4660858,-2349596,-4785873,1042165),(30,1,-2047080,0,7),(31,4,-7478720,4226769,0),(32,0,2,7,6),(33,null,2,null,-2096393),(34,6,6104075,2,1),(35,8,9,9,2),(36,3,null,-8298202,9),(37,0,2365635,5,6),(38,7614177,null,1481723,7),(39,4,3,-4309582,5),(40,2,5,null,9),(41,-2609561,2,3309378,2533949),(42,7,8,-1673025,3),(43,8,-279340,6,6),(44,0,357996,-1102117,6),(45,0,8,4,null),(46,9,1,5,3063994),(47,9,-4556195,null,3),(48,8,8105464,6959246,0),(49,9,0,6,7),(50,2,3,6,5),(51,-5497079,4568000,null,2),(52,-4197555,4,null,null),(53,-6699636,3,8,0),(54,4,6,null,null),(55,5,0,6468962,7),(56,null,5,2,9),(57,8,-5905832,5,9),(58,1,0,6,8),(59,5,4,-896639,null),(60,2,6,-7030129,-6022946),(61,4,7,5,-7100013),(62,null,5,3747094,1),(63,1,3,6401303,1),(64,null,null,null,5),(65,3,6,7,null),(66,3,-2765038,2,0),(67,-479346,6,5,2),(68,9,5808638,8,3),(69,346241,5,2,0),(70,7,2,-5028868,9),(71,6961476,null,6,-2699357),(72,7,2,7,6),(73,4,-5581974,-275721,582623),(74,6909128,5,0,1831856),(75,7,2,9,null),(76,8,7,-420247,5),(77,null,8,2,3125488),(78,3,7,-275070,5),(79,null,-8312528,-6434187,8098870),(80,1064140,7,-6447998,3230795),(81,6,3,5,4),(82,8,5,4,0),(83,6,8,7,7738045),(84,7825346,2137318,6,362187),(85,8,7657714,6,5),(86,7,0,null,6),(87,-221143,0,1334377,0),(88,4,1,1,8),(89,null,-5978479,-2419740,-4120764),(90,4,-3657254,-2463751,2),(91,-6122506,4,5,null),(92,0,null,3,783235),(93,5,0,6,-6147000),(94,5414851,7133045,4,8031979),(95,0,7406057,4,5),(96,5,null,4,1),(97,6,9,9,5817594),(98,null,1877662,4,9),(99,-5364707,2,-954043,null),(100,-1316865,3,2,5),(101,0,-5398548,7,null),(102,-4839606,0,0,5),(103,-3260975,2,2034150,null),(104,4,-201841,null,1),(105,4,5,-1314818,8),(106,6,0,4,null),(107,8,6,5,7),(108,0,-1608587,2,null),(109,9,null,6,-3167738),(110,9,4,7,-1021334),(111,-3024439,8,3298077,null),(112,5662867,6,5181411,6),(113,1,7,7,-5877590),(114,6,0,3,8),(115,-483310,7,7,-7165261),(116,1799354,-5186852,null,6),(117,2,-2498436,1,7627199),(118,0,null,193825,null),(119,2,null,9,-4990196),(120,2,7,4,9),(121,5,null,-3288744,-336922),(122,6157285,1,4730918,7162634),(123,7,6,8,3),(124,2,-6494404,3,-6970087),(125,6985774,8,9,5),(126,0,2,4,6219402),(127,8108820,-734237,1,-6750965),(128,6,null,-5280508,0),(129,8,0,8,-8174316),(130,8,null,1,7),(131,1,3,null,7),(132,-3780009,null,6,2),(133,3,1,3,3),(134,8,4,4786495,5),(135,-7592793,6,5259535,-4860692),(136,-3001350,1,1,6235187),(137,7,5,7,9),(138,6,625113,0,5),(139,9,7,null,null),(140,-126629,7057845,6,2382468),(141,null,3,-2127625,5),(142,4,1086711,6,2),(143,2565832,9,null,-53549),(144,5,1302422,9,3717673),(145,4204840,7,6,null),(146,7,null,9,7),(147,-4052886,7,1,7),(148,2,2440059,1,3797585),(149,null,6,-2646442,4),(150,-3494204,null,null,-4946122),(151,2,6,0,4772964),(152,0,6376949,-6344511,8281963),(153,-1754999,6,0,-340620),(154,3,9,3393103,1951924),(155,1,-3106085,2,6),(156,null,8,-5562118,null),(157,-3513266,-6150488,1,0),(158,1,1,-763094,8),(159,2,-5610364,null,-6984814),(160,0,null,8,-4211146),(161,9,9,-7832606,4415314),(162,4,-1620624,3,null),(163,4,-2179110,-8211873,-7708838),(164,-2847162,4,7,3),(165,4516616,5,8,0),(166,null,1,1316084,-7113354),(167,5,9,9,973279),(168,null,2028491,3,5709749),(169,9,6,2,6),(170,3,null,2,4),(171,1633219,0,3390861,7775013),(172,31564,6,8,0),(173,3,143865,-6415524,2),(174,3,1,7,7),(175,2,0,2,7),(176,9,4,8,-3013395),(177,0,4,0,1027843),(178,4,0,4,1),(179,8,7,2,9),(180,2412849,7,2660211,6),(181,8247209,4,-4171423,8233115),(182,null,1,-4435291,2240356),(183,1,7,-8199791,4850797),(184,-8321306,-4383858,6586434,0),(185,5,8,1,7),(186,4,4,5394164,0),(187,2583874,null,-2546933,null),(188,2,3,2,-7412965),(189,-6998004,6,0,853103),(190,3,3,null,3),(191,0,1,null,6),(192,8,-5042796,8,-2304154),(193,null,4617646,1442715,4589328),(194,7613834,3,4,0),(195,6960573,316842,5819605,8),(196,8,null,-1277508,null),(197,222440,null,6,3),(198,5,7,8,-3474684),(199,6,6025637,6,4);
    """

    sql """
        create table table_200_undef_partitions2_keys3_properties4_distributed_by52 (
        col_int_undef_signed int    ,
        col_int_undef_signed2 int    ,
        col_int_undef_signed3 int    ,
        col_int_undef_signed4 int    ,
        pk int
        ) engine=olap
        DUPLICATE KEY(col_int_undef_signed, col_int_undef_signed2)
        PARTITION BY             RANGE(col_int_undef_signed) (
                        PARTITION p0 VALUES LESS THAN ('4'),
                        PARTITION p1 VALUES LESS THAN ('6'),
                        PARTITION p2 VALUES LESS THAN ('7'),
                        PARTITION p3 VALUES LESS THAN ('8'),
                        PARTITION p4 VALUES LESS THAN ('10'),
                        PARTITION p5 VALUES LESS THAN ('83647'),
                        PARTITION p100 VALUES LESS THAN ('2147483647')
                    )
                
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """
    sql """
        insert into table_200_undef_partitions2_keys3_properties4_distributed_by52(pk,col_int_undef_signed,col_int_undef_signed2,col_int_undef_signed3,col_int_undef_signed4) values (0,null,6178782,4,-1498997),(1,null,null,2,4),(2,8,6,6114625,6840353),(3,6,-3487226,4,-18364),(4,6647558,0,7,4),(5,5,1,3,3991803),(6,null,3,3,6),(7,-1597140,3,3,2),(8,6415967,null,9,null),(9,0,2,-1569216,8263281),(10,2546741,4,-4334118,8),(11,2375117,5,null,-3767162),(12,4,290235,null,6),(13,5569849,8,6,null),(14,4,5,-3740535,7909612),(15,2954179,-292766,6,4),(16,-4432343,8342925,4132933,2),(17,2,4,5,4),(18,0,7416400,1726394,6),(19,2,1,9,0),(20,-2702638,4,9,-1517767),(21,5078458,8,1695037,5),(22,7,6,1,9),(23,8,8,1,2399207),(24,5028458,-908290,1,0),(25,9,7,7,8),(26,6,3,null,7259132),(27,7232253,837442,null,9),(28,9,0,8,9),(29,-1660135,null,0,6),(30,4696514,3,null,7),(31,-4265870,-2667011,1272099,8),(32,1,3,5,3),(33,6,null,3,9),(34,1,9,1,1),(35,null,null,8,3078856),(36,5820773,0,8,9),(37,7,0,4,2),(38,5,-44891,-7746811,1),(39,9,2059680,5,4),(40,1,4,null,2),(41,-7393782,9,5,5),(42,406763,5111434,4,9),(43,4,8,9,4064831),(44,null,0,2,null),(45,5,-4221280,8,-684654),(46,2,null,9,1),(47,2,-5619599,null,-5584546),(48,6606832,0,6,-343137),(49,3,7,3,-8339240),(50,5,0,9,null),(51,1859745,8,2,6),(52,null,9,null,null),(53,-3647016,0,5,2),(54,null,1,5,3),(55,-2022192,null,7809,1073314),(56,8354799,3193237,7,8),(57,-5301306,null,-5202675,6455106),(58,null,null,0,-6930650),(59,3,0,0,1),(60,7,4,-3165652,1),(61,7,-1166293,2,1729346),(62,-4455739,0,-7577482,6),(63,null,8,5,8),(64,5,7,7,-3058810),(65,null,1,2,7845156),(66,null,3787589,4,0),(67,-5051518,null,4,5),(68,9,2,null,5),(69,-6039045,1980745,5,0),(70,8,3,6,4),(71,3,0,3,0),(72,1,null,4,5),(73,5486783,4181578,9,7),(74,4,null,6,7),(75,6,0,2,-3573906),(76,8,7,9,-3242676),(77,2131963,-8023562,4,6),(78,null,-1813465,7,1625761),(79,6,null,8,2),(80,6,8282063,4542291,null),(81,-3447275,4,7,8195816),(82,5,5,-7144863,1),(83,7855661,4438085,3,3),(84,null,null,5,8),(85,9,2,5989898,null),(86,3108008,7,1473271,2),(87,6,null,1,4030180),(88,6,null,null,5),(89,0,5,6,-3057457),(90,2,7625892,7550192,null),(91,6713047,2,4,4528321),(92,3482251,0,4,1321810),(93,8,null,3,2),(94,null,6,-8163104,-5890573),(95,5,null,7126287,7772193),(96,7,9,7337156,3870836),(97,1849,3,3,0),(98,-3681228,5,3,2),(99,7,4,670347,4),(100,1,3,-6990744,-1824996),(101,4,0,4,3),(102,0,0,-3356937,1),(103,7660632,7,9,2171576),(104,7,4,4,7),(105,5,3,1,-495626),(106,6,-2682869,5,4917151),(107,-7590927,6,2,1),(108,9,428607,null,1),(109,-8349605,null,5,-1820345),(110,null,1,null,1),(111,null,1,7354638,4),(112,6463635,9,8,1),(113,6,0,7816986,null),(114,7342624,2629972,-2862972,-5580907),(115,null,3,-8122151,2),(116,8,6,3368312,-956244),(117,null,-1012440,-8304192,2),(118,0,3,146040,null),(119,7826337,8,9,9),(120,6,0,2,5),(121,2390443,null,7,null),(122,7112285,8,2,9),(123,4,3,7,9),(124,3515934,7,7621347,5620623),(125,3,3,6,4),(126,0,0,null,null),(127,969322,5,-652982,-5905446),(128,0,-2589537,4013990,2),(129,5,6966116,5709770,2),(130,1,-2839812,5,2),(131,null,6,0,3),(132,4,null,-2366225,3),(133,-7926343,8,-4452559,9),(134,null,9,-82987,-2645919),(135,6,2,6,3),(136,-6807353,-4563122,3407887,8013352),(137,5,2841074,-3713355,-4116178),(138,9,null,-1836053,9),(139,6,8,8,2),(140,null,6,1077375,0),(141,6,5327580,0,-5352457),(142,1,-3137279,7912391,8),(143,6669706,3,5,6),(144,6,null,3,4),(145,9,1,-509877,7),(146,null,0,8,-8152686),(147,9,-7965026,1635897,3),(148,4,2,4,6),(149,0,7,9,3),(150,5,8,2,1289391),(151,null,null,-1932126,9),(152,2657827,1,5731905,9),(153,4,null,0,8),(154,7,null,2,null),(155,5881903,5,4,5),(156,2,4,2648023,-2842013),(157,5,4,null,-885436),(158,5,-1410085,-2864937,4306061),(159,0,null,6100685,-1437455),(160,1,null,-6601342,0),(161,9,9,9,0),(162,-840205,null,2,0),(163,2790632,8,7004661,-1185387),(164,8373617,7,4,1793318),(165,0,9,7,null),(166,7,8,3,4),(167,5,2,9,4),(168,-6213873,6,9,1695027),(169,6677609,0,7473276,null),(170,null,5,3,-271621),(171,9,null,5,null),(172,6,6714376,5,3),(173,9,null,8,null),(174,-183166,5,1,6),(175,604398,4,8,-7971195),(176,null,2,-3090204,7),(177,1,7,0,2),(178,134741,2508149,281592,3),(179,5,7317983,3559498,4),(180,8,7,2,3),(181,-6125766,0,5,1),(182,-6879979,8171557,9,8),(183,5,7214081,-1076983,null),(184,null,3292785,null,5),(185,-5710737,-453654,-3629445,null),(186,2,5,2,4),(187,0,8,5,-4938639),(188,6,2,6,2),(189,8,-2903909,-7706609,-3825498),(190,1865997,9,null,698864),(191,-6091945,507316,5359362,6),(192,177285,4,0,5874469),(193,1,1,9,null),(194,7,91202,-7822986,7),(195,-1363190,null,9,-6492428),(196,5289424,-5453242,-6339553,2),(197,6,1,null,4),(198,-5460610,0,1912986,3),(199,7837695,1,-5806483,3);
    """

    qt_select_hash_join """
        SELECT /*+   leading( tbl5 { tbl3 tbl4 } tbl1 tbl2    ) */ tbl4 . col_int_undef_signed4 AS col_int_undef_signed , 2 AS col_int_undef_signed2 , tbl3 . col_int_undef_signed2 AS col_int_undef_signed3 , tbl1 . col_int_undef_signed AS col_int_undef_signed4 FROM table_50_undef_partitions2_keys3_properties4_distributed_by5 AS tbl1  INNER JOIN table_20_undef_partitions2_keys3_properties4_distributed_by5 AS tbl2 ON tbl2 . col_int_undef_signed = tbl1 . col_int_undef_signed2 AND tbl2 . col_int_undef_signed3 = tbl2 . col_int_undef_signed2 OR tbl1 . col_int_undef_signed = tbl2 . col_int_undef_signed4 AND tbl2 . col_int_undef_signed4 = tbl2 . col_int_undef_signed4   INNER JOIN table_50_undef_partitions2_keys3_properties4_distributed_by52 AS tbl3 ON tbl3 . col_int_undef_signed3 = tbl3 . col_int_undef_signed3    RIGHT  JOIN table_50_undef_partitions2_keys3_properties4_distributed_by52 AS tbl4 ON tbl3 . col_int_undef_signed4 = tbl4 . col_int_undef_signed    JOIN table_30_undef_partitions2_keys3_properties4_distributed_by52 AS tbl5 ON tbl3 . col_int_undef_signed = tbl5 . col_int_undef_signed   WHERE  (  tbl1 . col_int_undef_signed4 = (  SELECT /*+   leading( tbl1 tbl2       ) */ AVG( 1 ) FROM (  SELECT /*+  ORDERED leading( { tbl3 tbl4 tbl1 tbl2  }      ) */ tbl1 . col_int_undef_signed AS col_int_undef_signed , tbl3 . col_int_undef_signed3 AS col_int_undef_signed2 , tbl1 . col_int_undef_signed AS col_int_undef_signed3 , 3 AS col_int_undef_signed4 FROM table_200_undef_partitions2_keys3_properties4_distributed_by5 AS tbl1  INNER JOIN table_30_undef_partitions2_keys3_properties4_distributed_by52 AS tbl2 ON tbl2 . col_int_undef_signed = tbl1 . col_int_undef_signed4    JOIN table_50_undef_partitions2_keys3_properties4_distributed_by5 AS tbl3 ON tbl3 . col_int_undef_signed4 = tbl1 . col_int_undef_signed    JOIN table_30_undef_partitions2_keys3_properties4_distributed_by52 AS tbl4 ON tbl3 . col_int_undef_signed2 = tbl1 . col_int_undef_signed2    WHERE  ( tbl3 . col_int_undef_signed3  <  ( 0 + 3 ) AND  ( tbl1 . col_int_undef_signed2  IN (  SELECT /*+   leading(  tbl2 tbl1       ) */ 0 AS col_int_undef_signed FROM (  SELECT /*+   leading(  tbl3 tbl1 { tbl4 tbl2 } ) */ tbl2 . col_int_undef_signed AS col_int_undef_signed , tbl2 . col_int_undef_signed4 AS col_int_undef_signed2 , tbl4 . col_int_undef_signed AS col_int_undef_signed3 , tbl2 . col_int_undef_signed AS col_int_undef_signed4 FROM table_20_undef_partitions2_keys3_properties4_distributed_by52 AS tbl1   JOIN table_30_undef_partitions2_keys3_properties4_distributed_by52 AS tbl2 ON tbl2 . col_int_undef_signed4 = tbl1 . col_int_undef_signed2   INNER JOIN table_30_undef_partitions2_keys3_properties4_distributed_by5 AS tbl3 ON tbl3 . col_int_undef_signed2 = tbl2 . col_int_undef_signed4   INNER JOIN table_200_undef_partitions2_keys3_properties4_distributed_by52 AS tbl4 ON tbl1 . col_int_undef_signed4 = tbl3 . col_int_undef_signed4    WHERE  ( tbl2 . col_int_undef_signed2 IN ( NULL , 6 ) AND  ( tbl3 . col_int_undef_signed2 IN ( 5  *  6 , 2 ) ) ) ORDER BY 1,2,3,4 ASC LIMIT 5 OFFSET 500  ) AS tbl1   JOIN table_30_undef_partitions2_keys3_properties4_distributed_by5 AS tbl2 ON tbl1 . col_int_undef_signed = tbl2 . col_int_undef_signed4      WHERE  ( tbl1 . col_int_undef_signed3 IN ( 6  *  5 ) ) ORDER BY 1 DESC LIMIT 1000  ) AND  ( tbl1 . col_int_undef_signed2 NOT IN (  SELECT /*+   leading( tbl1       ) */ tbl1 . col_int_undef_signed AS col_int_undef_signed FROM table_100_undef_partitions2_keys3_properties4_distributed_by5 AS tbl1      WHERE  ( tbl1 . col_int_undef_signed2 = tbl1 . col_int_undef_signed2 OR  ( tbl1 . col_int_undef_signed2 IN ( 0  *  5 ) ) ) ORDER BY 1 ASC LIMIT 6666  ) AND NOT ( tbl1 . col_int_undef_signed = tbl1 . col_int_undef_signed2 ) ) ) ) ORDER BY 1,2,3,4 DESC LIMIT 1000 OFFSET 500  ) AS tbl1  INNER JOIN table_30_undef_partitions2_keys3_properties4_distributed_by5 AS tbl2 ON tbl2 . col_int_undef_signed3 = tbl2 . col_int_undef_signed3      WHERE  ( tbl1 . col_int_undef_signed4 IN ( NULL ) ) LIMIT 5  ) ) ORDER BY 2,4,1,3 DESC LIMIT 5 OFFSET 0 ;    
    """

    qt_select_hash_join2 """
        SELECT
            /*+  ORDERED leading( { tbl3 tbl4 tbl1 tbl2  }      ) */
            3
        FROM
            table_30_undef_partitions2_keys3_properties4_distributed_by52 AS tbl1
        WHERE
            (
                tbl1.col_int_undef_signed2 IN (
                    SELECT
                        0
                    FROM
                        (
                            SELECT
                                1
                            FROM
                                table_20_undef_partitions2_keys3_properties4_distributed_by52 AS tbl1
                                JOIN table_30_undef_partitions2_keys3_properties4_distributed_by52 AS tbl2 ON tbl2.col_int_undef_signed4 = tbl1.col_int_undef_signed2
                        ) AS tbl1
                )
                AND (
                    tbl1.col_int_undef_signed2 NOT IN (
                        SELECT
                            tbl1.col_int_undef_signed AS col_int_undef_signed
                        FROM
                            table_20_undef_partitions2_keys3_properties4_distributed_by52 AS tbl1
                    )
                )
            );
    """
}
