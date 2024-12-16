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
suite("test_cte_column_pruning") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_pipeline_engine=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """drop table if exists t1"""
    sql """drop table if exists t2"""
    sql """drop table if exists t3"""
    sql """drop table if exists t4"""

    sql """
        create table if not exists t1 (
        c2 int   ,
        c1 int   ,
        c3 int   ,
        c4 int   ,
        pk int
        )
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """
        create table if not exists t2 (
        c1 int   ,
        c2 int   ,
        c3 int   ,
        c4 int   ,
        pk int
        )
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """
        create table if not exists t3 (
        c2 int   ,
        c1 int   ,
        c3 int   ,
        c4 int   ,
        pk int
        )
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """
        create table if not exists t4 (
        c1 int   ,
        c2 int   ,
        c3 int   ,
        c4 int   ,
        pk int
        )
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """
        insert into t1(pk,c1,c2,c3,c4) values (0,7,2,3328056,7),(1,3,5,3,3045349),(2,2130015,0,7,-7116176),(3,4411710,1203314,1,2336164),(4,4,-8001461,0,8),(5,9,3,6,2),(6,-8088092,null,-7256698,-2025142),(7,8,2,5,1),(8,4,4953685,3,null),(9,-6662413,-3845449,4,2),(10,5315281,0,5,null),(11,9,3,7,7),(12,4341905,null,null,8),(13,3,6,5,1),(14,5,9,6541164,3),(15,1,-582319,1,9),(16,5533636,4,39841,0),(17,1,1,null,7),(18,742881,-1420303,6,1),(19,281430,6753011,3,2),(20,7,1,4,-31350),(21,-5663089,9,2278262,9),(22,6,0,2706409,3),(23,-3841713,9,3,9),(24,1,6,3,4059303),(25,0,1,-5700982,3),(26,5,0,6,1),(27,7,2,2,4),(28,6,-2140815,-8190613,6),(29,-8214516,4,3,6),(30,4393731,null,7,2),(31,-2524331,8,2,9),(32,5,1,7,3),(33,2,968001,-1718546,0),(34,9,2,null,-7682164),(35,5,-3302521,8,2),(36,2,1325975,null,2826927),(37,-6607898,null,4,8),(38,7,3,5284408,-265983),(39,1,null,2,-559197),(40,9,7,2,6),(41,-7193680,null,3,8),(42,-4800310,8,9,5),(43,0,8,0,-2429477),(44,-1007106,-7583038,9,2627388),(45,7,-6572230,4,-1789489),(46,8,4,null,7837867),(47,7,8,7,null),(48,8,-2618403,2723851,3),(49,1,3,1,0),(50,null,3241893,0,8),(51,1934849,-1353430,1,9),(52,5148268,6,null,1),(53,null,3922713,4,47559),(54,2038005,-7625242,null,-5606136),(55,4,449100,2108275,5147506),(56,5,5,4316929,null),(57,5049864,null,4,9),(58,null,7,2,9),(59,5,2,5,7),(60,9,9,5,-2774033),(61,4,0,6,1),(62,5,-7700238,6,3),(63,658183,-7933445,1,4),(64,8,8,-7019658,-7873828),(65,1,1,null,0),(66,1,2,9,7320481),(67,3,2099077,9,3),(68,-7120763,276954,0,4),(69,9,5,5170840,null),(70,null,6,220899,-5774478),(71,null,null,3,6),(72,7,2,8101877,null),(73,1,null,5,-5141920),(74,8,-7143195,0,6),(75,6,5,3388863,4),(76,6,6,-8015259,1),(77,5207959,-4325820,791546,7),(78,2,4411975,2,null),(79,9,2379417,8,3),(80,3,null,-6968517,-336360),(81,null,0,5,1),(82,3,0,6,-4536269),(83,2,7,0,7),(84,1,7,1,5),(85,3,3,7509217,2920951),(86,6,null,8,3),(87,9,8,8,5941004),(88,8023576,1036293,9,2),(89,5,3,1,5),(90,5,5,6,2170127),(91,null,1,7,null),(92,-5659717,4,null,6),(93,848413,9,-2742042,4980140),(94,1,9,467168,9),(95,6,6,4783371,-5096980),(96,3,2,4,3),(97,3,2,2,1),(98,8,0,-6734149,2),(99,4985816,3,null,8);
    """

    sql """
        insert into t2(pk,c1,c2,c3,c4) values (0,5,4,189864,-7663457),(1,7,null,6,1),(2,null,8,-3362640,9),(3,3,2,5,-2197130),(4,2,3,7160615,1),(5,null,-57834,420441,3),(6,0,null,2,2),(7,1,-3681539,3,4),(8,548866,3,0,5),(9,8,-2824887,0,3246956),(10,5,3,7,2),(11,8,8,6,8),(12,0,2,7,9),(13,8,6,null,null),(14,-4103729,4,5,8),(15,-3659292,2,7,5),(16,8,7,1,null),(17,2526018,4,8069607,5),(18,6,6,5,2802235),(19,9,0,6379201,null),(20,3,null,4,3),(21,0,8,-5506402,2),(22,6,4,3,1),(23,4,5225086,3,1),(24,-211796,2,0,null),(25,5,2,-4100572,7),(26,2345127,2,null,1),(27,8,2,4893754,2),(28,null,-5580446,4,0),(29,3,1,2,6);
    """

    sql """
        insert into t3(pk,c1,c2,c3,c4) values (0,3,2,6,-3164679),(1,-6216443,3437690,-288827,6),(2,4,-5352286,-1005469,4118240),(3,9,6795167,5,1616205),(4,8,-4659990,-4816829,6),(5,0,9,4,8),(6,-4454766,2,2510766,3),(7,7860071,-3434966,8,3),(8,null,0,2,1),(9,8031908,2,-6673194,-5981416),(10,5,6716310,8,2529959),(11,null,-3622116,1,-7891010),(12,null,3527222,7993802,null),(13,null,1,2,1),(14,2,8,7,7),(15,0,9,5,null),(16,7452083,null,-4620796,0),(17,9,9,null,6),(18,3,1,-1578776,5),(19,9,2532045,-3577349,null);
    """

    sql """
        insert into t4(pk,c1,c2,c3,c4) values (0,-4263513,null,null,6),(1,1,3,4,null),(2,2460936,6,5,6299003),(3,null,7,7107446,-2366754),(4,6247611,4785035,3,-8014875),(5,0,2,5249218,3),(6,null,253825,4,3),(7,null,2,9,-350785),(8,6,null,null,4),(9,1,3,1,3422691),(10,0,-6596165,1808018,3),(11,2,752342,null,1),(12,-5220927,2676278,9,7),(13,6025864,2,1,4),(14,7,4,4,9),(15,5,9,9,849881),(16,-4253076,null,-4404479,-6365351),(17,null,6,4240023,3),(18,7,1276495,7,6),(19,null,-4459040,178194,-6974337),(20,6,2498738,9,6),(21,8,-1047876,8,-3519551),(22,4477868,6,3,-7237985),(23,9,1,null,7),(24,null,2,-6996324,4),(25,2,2,-7965145,2),(26,5339549,6,null,4),(27,0,4,4,4),(28,null,6563965,-5816143,2),(29,4,7245227,3239886,1),(30,9,9,-8134757,0),(31,-1787881,7769609,8306001,null),(32,-1817246,1,3,-8163782),(33,7,4018844,0,4),(34,null,5,3,4),(35,8,-1698017,0,3024748),(36,2,7,5330073,3654557),(37,null,null,1,7),(38,6,9,0,2),(39,-3988946,-1465296,3,3),(40,4939439,null,null,3),(41,6,-7235968,1,0),(42,5141520,-7389145,8,1),(43,5,89342,1,0),(44,1,641063,9,4718353),(45,5,4,4,6),(46,2,6,4,4),(47,3,2,2,-7137584),(48,6735548,0,1,7),(49,6,4,7,-4864341);
    """

    sql """
        sync
    """

    sql """
        WITH tbl1 AS (
            SELECT
                tbl2.c1 AS c1,
                tbl3.c2 AS c2,
                tbl5.c4 AS c3,
                tbl3.c1 AS c4
            FROM
                t1 AS tbl1
                JOIN t2 AS tbl2 ON tbl1.c2 = tbl1.c4
                RIGHT JOIN t1 AS tbl3 ON tbl3.c3 = tbl1.c3
                INNER JOIN t3 AS tbl4 ON tbl3.c2 = tbl4.c3
                INNER JOIN t4 AS tbl5 ON tbl5.c4 = tbl4.c2
                AND tbl3.c3 = tbl3.c4
            WHERE
                (
                    tbl2.c4 = (0 + 9)
                    AND ((4 + 1) IS NULL)
                )
            ORDER BY
                1,
                2,
                3,
                4 DESC
            LIMIT
                6666 OFFSET 500
        )
        SELECT
            tbl3.c4 AS c1,
            tbl2.c4 AS c2,
            tbl3.c3 AS c3,
            tbl2.c2 AS c4
        FROM
            tbl1 AS tbl2
            JOIN tbl1 AS tbl3 ON tbl3.c2 = tbl2.c2
        WHERE
            (
                tbl2.c3 != tbl2.c3
                AND ((2 + 0) IS NOT NULL)
            )
        ORDER BY
            2,
            4,
            1,
            3 ASC
        LIMIT
            6666 OFFSET 2;
    """
}
