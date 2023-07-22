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

suite("test_analyze") {
    String db = "regression_test_statistics"
    String tbl = "analyzetestlimited_duplicate_all"

    sql """
        DROP TABLE IF EXISTS `${tbl}`
    """

    sql """
          CREATE TABLE IF NOT EXISTS `${tbl}` (
            `analyzetestlimitedk3` int(11) null comment "",
            `analyzetestlimitedk0` boolean null comment "",
            `analyzetestlimitedk1` tinyint(4) null comment "",
            `analyzetestlimitedk2` smallint(6) null comment "",
            `analyzetestlimitedk4` bigint(20) null comment "",
            `analyzetestlimitedk5` decimalv3(9, 3) null comment "",
            `analyzetestlimitedk6` char(36) null comment "",
            `analyzetestlimitedk10` date null comment "",
            `analyzetestlimitedk11` datetime null comment "",
            `analyzetestlimitedk7` varchar(64) null comment "",
            `analyzetestlimitedk8` double null comment "",
            `analyzetestlimitedk9` float null comment "",
            `analyzetestlimitedk12` string  null comment "",
            `analyzetestlimitedk13` largeint(40)  null comment ""
        ) engine=olap
        DUPLICATE KEY(`analyzetestlimitedk3`)
        DISTRIBUTED BY HASH(`analyzetestlimitedk3`) BUCKETS 5 properties("replication_num" = "1")
    """

    sql """
        INSERT INTO `${tbl}` VALUES (-2103297891,1,101,15248,4761818404925265645,939926.283,
        'UTmCFKMbprf0zSVOIlBJRNOl3JcNBdOsnCDt','2022-09-28','2022-10-28 01:56:56','tVvGDSrN6kyn',
        -954349107.187117,-40.46286,'g1ZP9nqVgaGKya3kPERdBofTWJQ4TIJEz972Xvw4hfPpTpWwlmondiLVTCyld7rSBlSWrE7NJRB0pvPGEFQKOx1s3',
        '-1559301292834325905'),
        (-2094982029,0,-81,-14746,-2618177187906633064,121889.100,NULL,'2023-05-01','2022-11-25 00:24:12',
        '36jVI0phYfhFucAOEASbh4OdvUYcI7QZFgQSveNyfGcRRUtQG9HGN1UcCmUH',-82250254.174239,NULL,
        'bTUHnMC4v7dI8U3TK0z4wZHdytjfHQfF1xKdYAVwPVNMT4fT4F92hj8ENQXmCkWtfp','6971810221218612372'),
        (-1840301109,1,NULL,NULL,7805768460922079440,546556.220,'wC7Pif9SJrg9b0wicGfPz2ezEmEKotmN6AMI',NULL,
        '2023-05-20 18:13:14','NM5SLu62SGeuD',-1555800813.9748349,-11122.953,
        'NH97wIjXk7dspvvfUUKe41ZetUnDmqLxGg8UYXwOwK3Jlu7dxO2GE9UJjyKW0NBxqUk1DY','-5004534044262380098'),
        (-1819679967,0,10,NULL,-5772413527188525359,-532045.626,'kqMe4VYEZAmajLthCLRkl8StDQHKrDWz91AQ','2022-06-30',
        '2023-02-22 15:30:38','wAbeF3p84j5pFJTInQuKZOezFbsy8HIjmuUF',-1766437367.4377379,1791.4128,
        '6OWmBD04UeKt1xI2XnR8t1kPG7qEYrf4J8RkA8UMs4HF33Yl','-8433424551792664598'),
        (-1490846276,0,NULL,7744,6074522476276146996,594200.976,NULL,'2022-11-27','2023-03-11 21:28:44',
        'yr8AuJLr2ud7DIwlt06cC7711UOsKslcDyySuqqfQE5X7Vjic6azHOrM6W',-715849856.288922,3762.217,
        '4UpWZJ0Twrefw0Tm0AxFS38V5','7406302706201801560'),(-1465848366,1,72,29170,-5585523608136628843,-34210.874,
        'rMGygAWU91Wa3b5A7l1wheo6EF0o6zhw4YeE','2022-09-20','2023-06-11 18:17:16','B6m9S9O2amsa4SXrEKK0ivJ2x9m1u8av',
        862085772.298349,-22304.209,'1','-3399178642401166400'),(-394034614,1,65,5393,-200651968801088119,NULL,
        '9MapWX9pn8zes9Gey1lhRsH3ATyQPIysjQYi','2023-05-11','2022-07-02 02:56:53','z5VWbuKr6HiK7yC7MRIoQGrb98VUS',
        1877828963.091433,-1204.1926,'fSDQqT38rkrJEi6fwc90rivgQcRPaW5V1aEmZpdSvUm','8882970420609470903'),
        (-287465855,0,-10,-32484,-5161845307234178602,748718.592,'n64TXbG25DQL5aw5oo9o9cowSjHCXry9HkId','2023-01-02',
        '2022-11-17 14:58:52','d523m4PwLdHZtPTqSoOBo5IGivCKe4A1Sc8SKCILFxgzYLe0',NULL,27979.855,
        'ps7qwcZjBjkGfcXYMw5HQMwnElzoHqinwk8vhQCbVoGBgfotc4oSkpD3tP34h4h0tTogDMwFu60iJm1bofUzyUQofTeRwZk8','4692206687866847780')
    """

    def frontends = sql """
        SHOW FRONTENDS;
    """
    if (frontends.size > 1) {
        return;
    }
    sql """
        ANALYZE DATABASE ${db}
    """

    sql """
        ANALYZE DATABASE ${db} WITH SYNC
    """

    sql """
        SET enable_nereids_planner=true;
        
        """
    sql """
        SET enable_fallback_to_original_planner=false;
        """
    sql """
        SET forbid_unknown_col_stats=true;
        """

    Thread.sleep(1000 * 60)

    sql """
        SELECT COUNT(*) FROM ${tbl}; 
    """

    sql """
        DROP STATS ${tbl}(analyzetestlimitedk3)
    """

    exception = null

    try {
        sql """
            SELECT COUNT(*) FROM ${tbl}; 
        """
    } catch (Exception e) {
        exception = e
    }

    assert exception != null

    exception = null

    sql """
        ANALYZE TABLE ${tbl} WITH SYNC
    """

    sql """
        SELECT COUNT(*) FROM ${tbl}; 
    """

    sql """
        DROP STATS ${tbl}
    """

    try {
        sql """
            SELECT COUNT(*) FROM ${tbl}; 
        """
    } catch (Exception e) {
        exception = e
    }

    a_result_1 = sql """
        ANALYZE DATABASE ${db} WITH SYNC WITH SAMPLE PERCENT 10
    """

    a_result_2 = sql """
        ANALYZE DATABASE ${db} WITH SYNC WITH SAMPLE PERCENT 5
    """

    a_result_3 = sql """
        ANALYZE DATABASE ${db} WITH SAMPLE PERCENT 5 WITH AUTO
    """

    show_result = sql """
        SHOW ANALYZE
    """

    def contains_expected_table = {r ->
        for(int i = 0; i < show_result.size; i++) {
            if (show_result[i][3] == "${tbl}" ) {
                return true
            }
        }
        return false
    }

    def stats_job_removed = {r, id ->
        for(int i = 0; i < r.size; i++) {
            if (r[i][0] == id ) {
                return false
            }
        }
        return true
    }

    assert contains_expected_table(show_result)

    sql """
        DROP ANALYZE JOB ${a_result_3[0][4]}
    """

    show_result = sql """
        SHOW ANALYZE
    """

    assert stats_job_removed(show_result, a_result_3[0][4])

    sql """
        ANALYZE DATABASE ${db} WITH SAMPLE ROWS 5 WITH PERIOD 100000
    """

}

