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

suite("push_down_join_on_assertnumrows") {
      multi_sql """
        drop table if exists table_200_undef_partitions2_keys3_properties4_distributed_by52;
        drop table if exists table_20_undef_partitions2_keys3_properties4_distributed_by5;
        drop table if exists table_30_undef_partitions2_keys3_properties4_distributed_by5;

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
        insert into table_20_undef_partitions2_keys3_properties4_distributed_by5(pk,col_int_undef_signed,col_int_undef_signed2,col_int_undef_signed3,col_int_undef_signed4) values (0,6,4,-3343259,7),(1,null,2,-5659896,0),(2,2,2369913,-5247778,-4711382),(3,6545002,3,2,4),(4,9,3,4,5),(5,4,5,4,1),(6,4,-4704791,null,6),(7,null,3,null,9),(8,-1012411,4,null,-1244656),(9,1,8,9,-5175872),(10,8,0,-4239951,2),(11,8,-2231762,4817469,2),(12,9,9,5,-427963),(13,4,0,null,-5587539),(14,-5949786,2,2,3432246),(15,1009336,1,null,9),(16,1,3,8,6),(17,1,null,-5766728,6),(18,4,6,9,-2808412),(19,2699189,6,4,2);

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
        insert into table_30_undef_partitions2_keys3_properties4_distributed_by5(pk,col_int_undef_signed,col_int_undef_signed2,col_int_undef_signed3,col_int_undef_signed4) values (0,2,null,0,null),(1,-242819,2983243,7071252,3),(2,1,-2342407,-1423905,8),(3,null,null,7,4),(4,-1494065,3,7,2),(5,5,0,-595225,5),(6,5,-3324113,0,5),(7,6829192,3527453,6,5436506),(8,1,-3189592,2,9),(9,null,2,6,2),(10,-4070807,null,-3324205,7),(11,8,-5293967,1,-5040205),(12,6,7440524,null,null),(13,null,2,9,5),(14,4,null,2,-6877636),(15,1,2,5,2642189),(16,null,-5867739,-6454009,8),(17,3,0,2,2),(18,null,null,9,2227146),(19,2,null,0,4),(20,2708363,-7827919,3,null),(21,7,7919706,-6642647,7870064),(22,6,3,7,5),(23,null,null,-1806180,1),(24,6,null,8,4),(25,2,3,0,4),(26,3,-6860149,9,-4519275),(27,7,4,3,-6966921),(28,1,3,0,-981573),(29,0,8,2413602,4);
    """

    sql """
        WITH tbl1 AS (
        SELECT
            tbl1.col_int_undef_signed2 AS col_int_undef_signed,
            7 AS col_int_undef_signed2,
            tbl3.col_int_undef_signed3 AS col_int_undef_signed3,
            tbl3.col_int_undef_signed2 AS col_int_undef_signed4
        FROM
            table_200_undef_partitions2_keys3_properties4_distributed_by52 AS tbl1
            INNER JOIN table_20_undef_partitions2_keys3_properties4_distributed_by5 AS tbl2 ON tbl2.col_int_undef_signed4 = tbl1.col_int_undef_signed4
            INNER JOIN table_30_undef_partitions2_keys3_properties4_distributed_by5 AS tbl3 ON tbl1.col_int_undef_signed2 = tbl1.col_int_undef_signed3
        WHERE
            ((7 + 5) < tbl2.col_int_undef_signed2)
        ORDER BY
            1,
            2,
            3,
            4 DESC
        LIMIT
            6666 OFFSET 100
    )
    SELECT
        /*+   leading( { tbl3 tbl2 tbl4    }             ) */
        6 AS col_int_undef_signed,
        tbl2.col_int_undef_signed2 AS col_int_undef_signed2,
        tbl4.col_int_undef_signed AS col_int_undef_signed3,
        tbl3.col_int_undef_signed3 AS col_int_undef_signed4
    FROM
        table_20_undef_partitions2_keys3_properties4_distributed_by5 AS tbl2
        RIGHT JOIN table_200_undef_partitions2_keys3_properties4_distributed_by52 AS tbl3 ON tbl2.col_int_undef_signed = tbl3.col_int_undef_signed
        INNER JOIN tbl1 AS tbl4 ON tbl3.col_int_undef_signed = tbl3.col_int_undef_signed
        AND tbl3.col_int_undef_signed3 > tbl2.col_int_undef_signed3
    WHERE
        (
            NOT tbl4.col_int_undef_signed = (
                SELECT
                    AVG(1)
                FROM
                    table_30_undef_partitions2_keys3_properties4_distributed_by5 AS tbl2
                WHERE
                    (
                        tbl2.col_int_undef_signed4 IN (9, 6, 6, NULL, 6, 3 + 3, 6, NULL)
                        OR (
                            tbl2.col_int_undef_signed4 = tbl2.col_int_undef_signed3
                            AND NOT (tbl2.col_int_undef_signed IS NOT NULL)
                        )
                    )
                LIMIT
                    1000
            )
        )
    ORDER BY
        1,
        2,
        3,
        4 ASC
    LIMIT
        1000 OFFSET 100;
    """
}
