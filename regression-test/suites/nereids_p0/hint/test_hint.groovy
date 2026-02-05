/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("test_hint") {
    // create database and tables
    sql 'DROP DATABASE IF EXISTS test_hint'
    sql 'CREATE DATABASE IF NOT EXISTS test_hint'
    sql 'use test_hint'

    // setting planner to nereids
    sql 'set exec_mem_limit=21G'
    sql 'set be_number_for_test=1'
    sql 'set parallel_pipeline_task_num=1'
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_nereids_distribute_planner=false'
    sql "set ignore_shape_nodes='PhysicalProject'"
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set runtime_filter_mode=OFF'

    // create tables
    sql """drop table if exists t1;"""
    sql """drop table if exists t2;"""

    sql """create table t1 (c1 int, c11 int) distributed by hash(c1) buckets 3 properties('replication_num' = '1');"""
    sql """create table t2 (c2 int, c22 int) distributed by hash(c2) buckets 3 properties('replication_num' = '1');"""

// test hint positions, remove join in order to make sure shape stable when no use hint
    qt_select1_1 """explain shape plan select /*+ leading(t2 broadcast t1) */ count(*) from t1 join t2 on c1 = c2;"""

    qt_select1_2 """explain shape plan /*+ leading(t2 broadcast t1) */ select count(*) from t1;"""

    qt_select1_3 """explain shape plan select /*+DBP: ROUTE={GROUP_ID(zjaq)}*/ count(*) from t1;"""

    qt_select1_4 """explain shape plan/*+DBP: ROUTE={GROUP_ID(zjaq)}*/ select count(*) from t1;"""

    qt_select1_5 """explain shape plan /*+ leading(t2 broadcast t1) */ select /*+ leading(t2 broadcast t1) */ count(*) from t1 join t2 on c1 = c2;"""

    qt_select1_6 """explain shape plan/*+DBP: ROUTE={GROUP_ID(zjaq)}*/ select /*+ leading(t2 broadcast t1) */ count(*) from t1 join t2 on c1 = c2;"""

    qt_select1_7 """explain shape plan /*+ leading(t2 broadcast t1) */ select /*+DBP: ROUTE={GROUP_ID(zjaq)}*/ count(*) from t1;"""

    qt_select1_8 """explain shape plan /*+DBP: ROUTE={GROUP_ID(zjaq)}*/ select /*+DBP: ROUTE={GROUP_ID(zjaq)}*/ count(*) from t1;"""

    multi_sql '''
    drop table if exists table_50_undef_partitions2_keys3_properties4_distributed_by5;
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
    insert into table_50_undef_partitions2_keys3_properties4_distributed_by5(pk,col_int_undef_signed,col_int_undef_signed2,col_int_undef_signed3,col_int_undef_signed4) values (0,8,0,3,7),(1,6,227612,4,8),(2,-590975,9,-4411568,6),(3,-7241036,null,3,5),(4,1,7,null,8),(5,2509741,5,5,1),(6,2,9,null,4817793),(7,6,8,3,0),(8,null,1,4,null),(9,711269,null,-613109,null),(10,null,7,0,7),(11,null,-5534845,0,4),(12,5,2,9,6850777),(13,-5789051,8,6,2463068),(14,2,5,953451,1),(15,-6229147,-6738861,4,0),(16,6,8,3548335,1),(17,-3735352,2218004,6,7),(18,9,0,5,8),(19,8,2,6,-758380),(20,7,0,null,6048817),(21,9,8,4,3),(22,2,-4481854,9,null),(23,3653640,null,null,null),(24,null,-7944779,7,null),(25,9,9,2163480,580893),(26,null,2,-7799420,-6345780),(27,3,6950630,-5639142,6),(28,6,2,9,2),(29,3,55558,1,5),(30,-1503616,-2549706,2114904,-1843209),(31,5,8,1,2),(32,5,7,9,9),(33,-5092612,-4128816,3,2743926),(34,2786602,4,1,-5603742),(35,-260462,4,8191134,1),(36,0,1912113,1860687,6),(37,8,0,3690937,5),(38,null,-2269664,5,1),(39,4,5,null,7),(40,8,3288161,8,3120054),(41,8,3,1,1598458),(42,6,null,8031283,-687039),(43,8,3,1,1),(44,1,9,null,8),(45,3,8,3,7876177),(46,-7963243,-1063851,8,-2615276),(47,null,4,6,7357475),(48,null,7584425,-1733070,1),(49,2685134,3986870,5,null);

    drop table if exists table_50_undef_partitions2_keys3_properties4_distributed_by52;
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
    insert into table_50_undef_partitions2_keys3_properties4_distributed_by52(pk,col_int_undef_signed,col_int_undef_signed2,col_int_undef_signed3,col_int_undef_signed4) values (0,-7314662,null,0,4373927),(1,0,9,2,null),(2,5,2151343,-1467194,null),(3,null,null,-6124108,null),(4,5795207,4306466,4,7),(5,6,8,3,9),(6,null,8,-7232808,9),(7,9,6,9,6),(8,4637962,-1241311,2,8),(9,1,2,3,null),(10,0,-1652390,1,3),(11,0,9,6,2),(12,-8342795,0,5539034,-4960208),(13,2768087,7,-6242297,4996873),(14,1,2,1004860,9),(15,7,6,4,3),(16,-5667354,6,-658029,-3453673),(17,2,4,null,6),(18,7316298,4317813,-3103121,6697594),(19,-6020689,null,5,4828114),(20,5213384,6,9,8),(21,-6236361,4,4,8),(22,9,6,-1584500,1),(23,0,-2374298,-3839603,4),(24,4,2277204,1533423,6665310),(25,3,4,-6827212,5),(26,7305845,6618742,5,null),(27,-7139586,8,9,-5562137),(28,-1125938,-7214153,7,3),(29,null,null,2,1),(30,0,8,-6322297,-754270),(31,-2399194,9,-3216746,7),(32,-4869619,8,1161765,-6021329),(33,1,6,1,-2737761),(34,0,2,3,9),(35,7193618,1,0,7),(36,1,2898967,1,6142194),(37,6,2322198,0,1),(38,2081553,3,4,8),(39,6,5348917,0,null),(40,8,-3538489,-8035524,8),(41,9,8,1,-8066548),(42,null,-1868330,-7649411,6),(43,1,5,8,7986791),(44,7,3,5120842,6),(45,3043240,5,2,2),(46,null,null,4600741,5),(47,1,9,-6246097,1),(48,9,1358064,1451574,5),(49,0,9,2731609,8);

    drop table if exists table_100_undef_partitions2_keys3_properties4_distributed_by52;
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
    insert into table_100_undef_partitions2_keys3_properties4_distributed_by52(pk,col_int_undef_signed,col_int_undef_signed2,col_int_undef_signed3,col_int_undef_signed4) values (0,7865838,-348902,null,8),(1,-9434,9,8,0),(2,1845860,6675073,-7931956,-66007),(3,-7523286,210291,3,4),(4,null,-1341350,-5318642,1),(5,-6634226,2179558,2,7),(6,2,7,2,3),(7,9,2,3,-7773846),(8,0,8,6,2407384),(9,0,1,7,7),(10,5,5,null,8),(11,9,null,8283010,6),(12,7359987,5145929,2,5),(13,0,5225949,0,6770846),(14,1,454548,7,3),(15,4429471,3,6,-3746585),(16,8,1,1,5),(17,-3681554,9,6,9),(18,8,6625564,null,7),(19,7,-1391149,-6540242,3),(20,-4552986,-3094213,2,3),(21,2,-5742470,2,3),(22,1,4,3,-8373664),(23,9,null,0,6),(24,5,8,0,0),(25,null,6,-4716070,1),(26,-2301615,3,-5825636,-2086462),(27,2,8,3,2),(28,3,84928,0,-5660227),(29,6731053,9,null,-5919170),(30,0,7,0,5),(31,6,9,9,8059309),(32,-6393100,null,5,-4967524),(33,3,6,0,-3032361),(34,6,-4864946,-2105421,3),(35,1,-7451745,-2259887,-2089962),(36,0,null,-2422637,null),(37,4,2,6,2923491),(38,7,-5404336,8,6),(39,null,9,9,9),(40,7,2829945,7,8),(41,0,6,null,5553115),(42,4484999,5,5956381,7),(43,0,5,5,9),(44,4,2,9,7618190),(45,null,1258496,5,81600),(46,7,6,3,9),(47,1,7825001,null,9),(48,1551837,1,1013781,6519816),(49,7,8,null,1),(50,5,5773935,7374934,7978090),(51,4678457,5,3,1),(52,1,7,7,null),(53,1,-1803026,3,2909249),(54,-3971388,5,2305082,6),(55,4,7082848,3,null),(56,1,6,8,-89996),(57,0,-6953252,3435902,4),(58,-1725266,1,6,7),(59,3,2,5,2),(60,-5633253,-4222570,-7759796,7),(61,8,1863317,4,3159792),(62,5,2,9,8),(63,5623617,6,-3409569,7240590),(64,3,null,5,-6877359),(65,8,-8364539,7,null),(66,null,0,8,2),(67,5,1,5024672,5),(68,6448944,7,-237775,9),(69,null,4726085,8,3),(70,5,5,7,3220780),(71,9,5,7,8),(72,-2768334,289680,3,null),(73,6,9,-838198,4094997),(74,1,5584313,7214070,0),(75,0,5,663506,4),(76,null,null,5,2),(77,8,0,-1365192,0),(78,-4491579,-3199988,null,7),(79,1,382052,null,-2783713),(80,5,-516120,3,6),(81,7106844,-4368854,4,1),(82,6,4,null,6161227),(83,4,6515928,7,7),(84,-2652991,6420412,-2935612,-554335),(85,7131403,5,4,3),(86,3341348,4825568,1,9),(87,6,6,3,5),(88,-660413,2239981,2,6),(89,7,6,5,-8025658),(90,7,1007386,2740140,4583854),(91,3,null,0,null),(92,2,8163860,1,-992043),(93,2,7,1454321,-822592),(94,7,9,5,0),(95,889343,1549247,7,7164591),(96,8,0,-3972698,-8438),(97,-7575716,0,4,null),(98,2216759,1,2,5),(99,-2206677,8,4,-7081330);

    drop table if exists table_200_undef_partitions2_keys3_properties4_distributed_by52;
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
    insert into table_200_undef_partitions2_keys3_properties4_distributed_by52(pk,col_int_undef_signed,col_int_undef_signed2,col_int_undef_signed3,col_int_undef_signed4) values (0,null,6178782,4,-1498997),(1,null,null,2,4),(2,8,6,6114625,6840353),(3,6,-3487226,4,-18364),(4,6647558,0,7,4),(5,5,1,3,3991803),(6,null,3,3,6),(7,-1597140,3,3,2),(8,6415967,null,9,null),(9,0,2,-1569216,8263281),(10,2546741,4,-4334118,8),(11,2375117,5,null,-3767162),(12,4,290235,null,6),(13,5569849,8,6,null),(14,4,5,-3740535,7909612),(15,2954179,-292766,6,4),(16,-4432343,8342925,4132933,2),(17,2,4,5,4),(18,0,7416400,1726394,6),(19,2,1,9,0),(20,-2702638,4,9,-1517767),(21,5078458,8,1695037,5),(22,7,6,1,9),(23,8,8,1,2399207),(24,5028458,-908290,1,0),(25,9,7,7,8),(26,6,3,null,7259132),(27,7232253,837442,null,9),(28,9,0,8,9),(29,-1660135,null,0,6),(30,4696514,3,null,7),(31,-4265870,-2667011,1272099,8),(32,1,3,5,3),(33,6,null,3,9),(34,1,9,1,1),(35,null,null,8,3078856),(36,5820773,0,8,9),(37,7,0,4,2),(38,5,-44891,-7746811,1),(39,9,2059680,5,4),(40,1,4,null,2),(41,-7393782,9,5,5),(42,406763,5111434,4,9),(43,4,8,9,4064831),(44,null,0,2,null),(45,5,-4221280,8,-684654),(46,2,null,9,1),(47,2,-5619599,null,-5584546),(48,6606832,0,6,-343137),(49,3,7,3,-8339240),(50,5,0,9,null),(51,1859745,8,2,6),(52,null,9,null,null),(53,-3647016,0,5,2),(54,null,1,5,3),(55,-2022192,null,7809,1073314),(56,8354799,3193237,7,8),(57,-5301306,null,-5202675,6455106),(58,null,null,0,-6930650),(59,3,0,0,1),(60,7,4,-3165652,1),(61,7,-1166293,2,1729346),(62,-4455739,0,-7577482,6),(63,null,8,5,8),(64,5,7,7,-3058810),(65,null,1,2,7845156),(66,null,3787589,4,0),(67,-5051518,null,4,5),(68,9,2,null,5),(69,-6039045,1980745,5,0),(70,8,3,6,4),(71,3,0,3,0),(72,1,null,4,5),(73,5486783,4181578,9,7),(74,4,null,6,7),(75,6,0,2,-3573906),(76,8,7,9,-3242676),(77,2131963,-8023562,4,6),(78,null,-1813465,7,1625761),(79,6,null,8,2),(80,6,8282063,4542291,null),(81,-3447275,4,7,8195816),(82,5,5,-7144863,1),(83,7855661,4438085,3,3),(84,null,null,5,8),(85,9,2,5989898,null),(86,3108008,7,1473271,2),(87,6,null,1,4030180),(88,6,null,null,5),(89,0,5,6,-3057457),(90,2,7625892,7550192,null),(91,6713047,2,4,4528321),(92,3482251,0,4,1321810),(93,8,null,3,2),(94,null,6,-8163104,-5890573),(95,5,null,7126287,7772193),(96,7,9,7337156,3870836),(97,1849,3,3,0),(98,-3681228,5,3,2),(99,7,4,670347,4),(100,1,3,-6990744,-1824996),(101,4,0,4,3),(102,0,0,-3356937,1),(103,7660632,7,9,2171576),(104,7,4,4,7),(105,5,3,1,-495626),(106,6,-2682869,5,4917151),(107,-7590927,6,2,1),(108,9,428607,null,1),(109,-8349605,null,5,-1820345),(110,null,1,null,1),(111,null,1,7354638,4),(112,6463635,9,8,1),(113,6,0,7816986,null),(114,7342624,2629972,-2862972,-5580907),(115,null,3,-8122151,2),(116,8,6,3368312,-956244),(117,null,-1012440,-8304192,2),(118,0,3,146040,null),(119,7826337,8,9,9),(120,6,0,2,5),(121,2390443,null,7,null),(122,7112285,8,2,9),(123,4,3,7,9),(124,3515934,7,7621347,5620623),(125,3,3,6,4),(126,0,0,null,null),(127,969322,5,-652982,-5905446),(128,0,-2589537,4013990,2),(129,5,6966116,5709770,2),(130,1,-2839812,5,2),(131,null,6,0,3),(132,4,null,-2366225,3),(133,-7926343,8,-4452559,9),(134,null,9,-82987,-2645919),(135,6,2,6,3),(136,-6807353,-4563122,3407887,8013352),(137,5,2841074,-3713355,-4116178),(138,9,null,-1836053,9),(139,6,8,8,2),(140,null,6,1077375,0),(141,6,5327580,0,-5352457),(142,1,-3137279,7912391,8),(143,6669706,3,5,6),(144,6,null,3,4),(145,9,1,-509877,7),(146,null,0,8,-8152686),(147,9,-7965026,1635897,3),(148,4,2,4,6),(149,0,7,9,3),(150,5,8,2,1289391),(151,null,null,-1932126,9),(152,2657827,1,5731905,9),(153,4,null,0,8),(154,7,null,2,null),(155,5881903,5,4,5),(156,2,4,2648023,-2842013),(157,5,4,null,-885436),(158,5,-1410085,-2864937,4306061),(159,0,null,6100685,-1437455),(160,1,null,-6601342,0),(161,9,9,9,0),(162,-840205,null,2,0),(163,2790632,8,7004661,-1185387),(164,8373617,7,4,1793318),(165,0,9,7,null),(166,7,8,3,4),(167,5,2,9,4),(168,-6213873,6,9,1695027),(169,6677609,0,7473276,null),(170,null,5,3,-271621),(171,9,null,5,null),(172,6,6714376,5,3),(173,9,null,8,null),(174,-183166,5,1,6),(175,604398,4,8,-7971195),(176,null,2,-3090204,7),(177,1,7,0,2),(178,134741,2508149,281592,3),(179,5,7317983,3559498,4),(180,8,7,2,3),(181,-6125766,0,5,1),(182,-6879979,8171557,9,8),(183,5,7214081,-1076983,null),(184,null,3292785,null,5),(185,-5710737,-453654,-3629445,null),(186,2,5,2,4),(187,0,8,5,-4938639),(188,6,2,6,2),(189,8,-2903909,-7706609,-3825498),(190,1865997,9,null,698864),(191,-6091945,507316,5359362,6),(192,177285,4,0,5874469),(193,1,1,9,null),(194,7,91202,-7822986,7),(195,-1363190,null,9,-6492428),(196,5289424,-5453242,-6339553,2),(197,6,1,null,4),(198,-5460610,0,1912986,3),(199,7837695,1,-5806483,3);

    drop table if exists tmp_t006;
    CREATE TABLE tmp_t006 properties ("replication_num" = "1") as select 1, original_sql.* from  ( SELECT /*+   leading( tbl4 ) */ tbl4 . col_int_undef_signed AS col_int_undef_signed , 8 AS col_int_undef_signed2 , 4 AS col_int_undef_signed3 , 6 AS col_int_undef_signed4 FROM table_50_undef_partitions2_keys3_properties4_distributed_by52 AS tbl1  INNER JOIN table_50_undef_partitions2_keys3_properties4_distributed_by52 AS tbl2 ON tbl1 . col_int_undef_signed = tbl1 . col_int_undef_signed   INNER JOIN table_50_undef_partitions2_keys3_properties4_distributed_by5 AS tbl3 ON tbl3 . col_int_undef_signed4  !=  tbl3 . col_int_undef_signed   INNER JOIN (  SELECT /*+   leading( tbl1        ) */ 0 AS col_int_undef_signed , tbl1 . col_int_undef_signed2 AS col_int_undef_signed2 , tbl1 . col_int_undef_signed2 AS col_int_undef_signed3 , tbl1 . col_int_undef_signed2 AS col_int_undef_signed4 FROM table_50_undef_partitions2_keys3_properties4_distributed_by52 AS tbl1      WHERE  ( tbl1 . col_int_undef_signed3  IN (  SELECT /*+   leading( tbl2 tbl1 ) */ 3 AS col_int_undef_signed FROM table_200_undef_partitions2_keys3_properties4_distributed_by52 AS tbl1  INNER JOIN table_100_undef_partitions2_keys3_properties4_distributed_by52 AS tbl2 ON tbl2 . col_int_undef_signed2 = tbl1 . col_int_undef_signed3      WHERE  ( tbl1 . col_int_undef_signed2 = tbl2 . col_int_undef_signed2 AND  ( tbl1 . col_int_undef_signed4 IN ( 2 , NULL , 6 , 0 , 1 , NULL ) ) ) ORDER BY 1 ASC LIMIT 5  ) AND  ( tbl1 . col_int_undef_signed2 IN ( 0  +  5 , 9 , 9  *  7 , 8  +  5 , 9  -  7 , 7 ) ) ) ORDER BY 1,2,3,4 DESC LIMIT 6666 OFFSET 50  ) AS tbl4 ON tbl1 . col_int_undef_signed3 = tbl1 . col_int_undef_signed    WHERE NOT ( tbl1 . col_int_undef_signed3 IN ( NULL , 4 , 1 , 0 , 6 , 8  *  6 ) ) ORDER BY 1,2,3,4 ASC LIMIT 6666 OFFSET 500 ) as original_sql;
    '''

    explain {
        sql('''
        select
            1,
            original_sql.*
        from
            (
                SELECT
                    /*+   leading( tbl4 ) */
                    tbl4.col_int_undef_signed AS col_int_undef_signed,
                    8 AS col_int_undef_signed2,
                    4 AS col_int_undef_signed3,
                    6 AS col_int_undef_signed4
                FROM
                    table_50_undef_partitions2_keys3_properties4_distributed_by52 AS tbl1
                    INNER JOIN table_50_undef_partitions2_keys3_properties4_distributed_by52 AS tbl2 ON tbl1.col_int_undef_signed = tbl1.col_int_undef_signed
                    INNER JOIN table_50_undef_partitions2_keys3_properties4_distributed_by5 AS tbl3 ON tbl3.col_int_undef_signed4 != tbl3.col_int_undef_signed
                    INNER JOIN (
                        SELECT
                            /*+   leading( tbl1        ) */
                            0 AS col_int_undef_signed,
                            tbl1.col_int_undef_signed2 AS col_int_undef_signed2,
                            tbl1.col_int_undef_signed2 AS col_int_undef_signed3,
                            tbl1.col_int_undef_signed2 AS col_int_undef_signed4
                        FROM
                            table_50_undef_partitions2_keys3_properties4_distributed_by52 AS tbl1
                        WHERE
                            (
                                tbl1.col_int_undef_signed3 IN (
                                    SELECT
                                        /*+   leading( tbl2 tbl1 ) */
                                        3 AS col_int_undef_signed
                                    FROM
                                        table_200_undef_partitions2_keys3_properties4_distributed_by52 AS tbl1
                                        INNER JOIN table_100_undef_partitions2_keys3_properties4_distributed_by52 AS tbl2 ON tbl2.col_int_undef_signed2 = tbl1.col_int_undef_signed3
                                    WHERE
                                        (
                                            tbl1.col_int_undef_signed2 = tbl2.col_int_undef_signed2
                                            AND (
                                                tbl1.col_int_undef_signed4 IN (2, NULL, 6, 0, 1, NULL)
                                            )
                                        )
                                    ORDER BY
                                        1 ASC
                                    LIMIT
                                        5
                                )
                                AND (
                                    tbl1.col_int_undef_signed2 IN (0 + 5, 9, 9 * 7, 8 + 5, 9 -  7, 7)
                                )
                            )
                        ORDER BY
                            1,
                            2,
                            3,
                            4 DESC
                        LIMIT
                            6666 OFFSET 50
                    ) AS tbl4 ON tbl1.col_int_undef_signed3 = tbl1.col_int_undef_signed
                WHERE
                    NOT (
                        tbl1.col_int_undef_signed3 IN (NULL, 4, 1, 0, 6, 8 * 6)
                    )
                ORDER BY
                    1,
                    2,
                    3,
                    4 ASC
                LIMIT
                    6666 OFFSET 500
            ) as original_sql INTO OUTFILE "file:///tmp/tt.csv" FORMAT AS CSV_WITH_NAMES PROPERTIES(
                "column_separator" = ",",
                "success_file_name" = "_SUCCESS"
            );
        ''')
    }

}
