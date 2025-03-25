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

suite("slow_fold_constant_case_when") {
    sql "drop table if exists slow_fold_constant_case_when"

    sql """
        CREATE TABLE `slow_fold_constant_case_when` (
          `c1` decimal(38,10) NULL,
          `c2` decimal(38,10) NULL,
          `c3` decimal(38,10) NULL,
          `c4` decimal(38,10) NULL,
          `c5` decimal(38,10) NULL,
          `c6` decimal(38,10) NULL,
          `c7` decimal(38,10) NULL,
          `c8` decimal(38,10) NULL,
          `c9` decimal(38,10) NULL,
          `c10` decimal(38,10) NULL,
          `c11` decimal(38,10) NULL,
          `c12` decimal(38,10) NULL,
          `c13` decimal(38,10) NULL,
          `c14` decimal(38,10) NULL,
          `c15` decimal(38,10) NULL,
          `c16` decimal(38,10) NULL,
          `c17` decimal(38,10) NULL,
          `c18` decimal(38,10) NULL,
          `c19` decimal(38,10) NULL,
          `c20` decimal(38,10) NULL,
          `c21` decimal(38,10) NULL,
          `c22` decimal(38,10) NULL,
          `c23` decimal(38,10) NULL,
          `c24` decimal(38,10) NULL,
          `c25` decimal(38,10) NULL,
          `c26` decimal(38,10) NULL,
          `c27` decimal(38,10) NULL,
          `c28` decimal(38,10) NULL,
          `c29` decimal(38,10) NULL,
          `c30` decimal(38,10) NULL,
          `c31` decimal(38,10) NULL,
          `c32` decimal(38,10) NULL,
          `c33` decimal(38,10) NULL,
          `c34` decimal(38,10) NULL,
          `c35` decimal(38,10) NULL,
          `c36` decimal(38,10) NULL,
          `c37` decimal(38,10) NULL,
          `c38` decimal(38,10) NULL,
          `c39` decimal(38,10) NULL,
          `c40` decimal(38,10) NULL,
          `c41` decimal(38,10) NULL,
          `c42` decimal(38,10) NULL,
          `c43` varchar(65532) NULL,
          `c44` varchar(65532) NULL,
          `c45` varchar(65532) NULL,
          `c46` varchar(65532) NULL,
          `c47` varchar(65532) NULL,
          `c48` varchar(65532) NULL,
          `c49` varchar(65532) NULL,
          `c50` varchar(65532) NULL,
          `c51` varchar(65532) NULL,
          `c52` varchar(65532) NULL,
          `c53` varchar(65532) NULL,
          `c54` varchar(65532) NULL,
          `c55` varchar(65532) NULL,
          `c56` varchar(65532) NULL,
          `c57` varchar(65532) NULL,
          `c58` varchar(65532) NULL,
          `c59` varchar(65532) NULL,
          `c60` varchar(65532) NULL,
          `c61` varchar(65532) NULL,
          `c62` varchar(65532) NULL,
          `c63` varchar(65532) NULL,
          `c64` varchar(65532) NULL,
          `c65` varchar(65532) NULL,
          `c66` varchar(65532) NULL,
          `c67` varchar(65532) NULL,
          `c68` varchar(65532) NULL,
          `c69` varchar(65532) NULL,
          `c70` varchar(65532) NULL,
          `c71` varchar(65532) NULL,
          `c72` varchar(65532) NULL,
          `c73` varchar(65532) NULL,
          `c74` varchar(65532) NOT NULL,
        ) ENGINE=OLAP
        DUPLICATE KEY(`c1`, `c2`)
        PARTITION BY LIST(`c74`)
        (PARTITION P_201901 VALUES IN ("201901"),
        PARTITION P_201902 VALUES IN ("201902"),
        PARTITION P_201903 VALUES IN ("201903"),
        PARTITION P_201904 VALUES IN ("201904"),
        PARTITION P_201905 VALUES IN ("201905"),
        PARTITION P_201906 VALUES IN ("201906"),
        PARTITION P_201907 VALUES IN ("201907"),
        PARTITION P_201908 VALUES IN ("201908"),
        PARTITION P_201909 VALUES IN ("201909"),
        PARTITION P_201910 VALUES IN ("201910"),
        PARTITION P_201911 VALUES IN ("201911"),
        PARTITION P_201912 VALUES IN ("201912"),
        PARTITION P_202000 VALUES IN ("202000"),
        PARTITION P_202001 VALUES IN ("202001"),
        PARTITION P_202002 VALUES IN ("202002"),
        PARTITION P_202003 VALUES IN ("202003"),
        PARTITION P_202004 VALUES IN ("202004"),
        PARTITION P_202005 VALUES IN ("202005"),
        PARTITION P_202006 VALUES IN ("202006"),
        PARTITION P_202007 VALUES IN ("202007"),
        PARTITION P_202008 VALUES IN ("202008"),
        PARTITION P_202009 VALUES IN ("202009"),
        PARTITION P_202010 VALUES IN ("202010"),
        PARTITION P_202011 VALUES IN ("202011"),
        PARTITION P_202012 VALUES IN ("202012"),
        PARTITION P_202100 VALUES IN ("202100"),
        PARTITION P_202101 VALUES IN ("202101"),
        PARTITION P_202102 VALUES IN ("202102"),
        PARTITION P_202103 VALUES IN ("202103"),
        PARTITION P_202104 VALUES IN ("202104"),
        PARTITION P_202105 VALUES IN ("202105"),
        PARTITION P_202106 VALUES IN ("202106"),
        PARTITION P_202107 VALUES IN ("202107"),
        PARTITION P_202108 VALUES IN ("202108"),
        PARTITION P_202109 VALUES IN ("202109"),
        PARTITION P_202110 VALUES IN ("202110"),
        PARTITION P_202111 VALUES IN ("202111"),
        PARTITION P_202112 VALUES IN ("202112"),
        PARTITION P_202201 VALUES IN ("202201"),
        PARTITION P_202202 VALUES IN ("202202"),
        PARTITION P_202203 VALUES IN ("202203"),
        PARTITION P_202204 VALUES IN ("202204"),
        PARTITION P_202205 VALUES IN ("202205"),
        PARTITION P_202206 VALUES IN ("202206"),
        PARTITION P_202207 VALUES IN ("202207"),
        PARTITION P_202208 VALUES IN ("202208"),
        PARTITION P_202209 VALUES IN ("202209"),
        PARTITION P_202210 VALUES IN ("202210"),
        PARTITION P_202211 VALUES IN ("202211"),
        PARTITION P_202212 VALUES IN ("202212"),
        PARTITION P_202300 VALUES IN ("202300"),
        PARTITION P_202301 VALUES IN ("202301"),
        PARTITION P_202302 VALUES IN ("202302"),
        PARTITION P_202303 VALUES IN ("202303"),
        PARTITION P_202304 VALUES IN ("202304"),
        PARTITION P_202305 VALUES IN ("202305"),
        PARTITION P_202306 VALUES IN ("202306"),
        PARTITION P_202307 VALUES IN ("202307"),
        PARTITION P_202308 VALUES IN ("202308"),
        PARTITION P_202309 VALUES IN ("202309"),
        PARTITION P_202310 VALUES IN ("202310"),
        PARTITION P_202311 VALUES IN ("202311"),
        PARTITION P_202312 VALUES IN ("202312"),
        PARTITION P_202401 VALUES IN ("202401"),
        PARTITION P_202402 VALUES IN ("202402"),
        PARTITION P_202403 VALUES IN ("202403"),
        PARTITION P_202404 VALUES IN ("202404"),
        PARTITION P_202405 VALUES IN ("202405"),
        PARTITION P_202406 VALUES IN ("202406"),
        PARTITION P_202407 VALUES IN ("202407"),
        PARTITION P_202408 VALUES IN ("202408"),
        PARTITION P_202409 VALUES IN ("202409"))
        DISTRIBUTED BY HASH(`c44`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """


    def sqlStr = """
                SELECT `_____` ,
         `c24` ,
         `c28` ,
         `c26` ,
         `c21` ,
         `c25` ,
         `c29` ,
         `c5` ,
         `c6` ,
         `c7` ,
         `c15` ,
         `c4` ,
         `c27` ,
         `c19` ,
         `c18` ,
         `c20` ,
         `c8` ,
         `c13` ,
         `c16` ,
         `c11` ,
         `c10` ,
         `c14` ,
         `c9` ,
         `c12` ,
         `c17` ,
         (c22)/(((c39)+(c40))/2) AS `_____` ,
         ((c39)+(c40))/2 AS `_______`
FROM 
    (SELECT sum(c22) AS `c22` ,
         sum(c16) AS `c16` ,
         sum(c6) AS `c6` ,
         sum(c26) AS `c26` ,
         sum(c10) AS `c10` ,
         sum(c19) AS `c19` ,
         sum(c17) AS `c17` ,
         sum(c39) AS `c39` ,
         sum(c21) AS `c21` ,
         sum(c12) AS `c12` ,
         sum(c5) AS `c5` ,
         sum(c11) AS `c11` ,
         sum(c4) AS `c4` ,
         sum(c27) AS `c27` ,
         sum(c29) AS `c29` ,
         sum(c20) AS `c20` ,
         sum(c28) AS `c28` ,
         sum(c13) AS `c13` ,
         sum(c25) AS `c25` ,
         sum(c9) AS `c9` ,
         sum(c24) AS `c24` ,
         sum(c14) AS `c14` ,
         sum(c40) AS `c40` ,
         sum(c7) AS `c7` ,
         sum(c15) AS `c15` ,
         sum(c18) AS `c18` ,
         sum(c8) AS `c8` ,
         `_____`
    FROM 
        (SELECT `_____` ,
         `c24` ,
         `c28` ,
         `c26` ,
         `c21` ,
         `c25` ,
         `c29` ,
         `c5` ,
         `c6` ,
         `c7` ,
         `c15` ,
         `c4` ,
         `c27` ,
         `c19` ,
         `c18` ,
         `c20` ,
         `c8` ,
         `c13` ,
         `c16` ,
         `c11` ,
         `c10` ,
         `c14` ,
         `c9` ,
         `c12` ,
         `c17` ,
         `c22` ,
         `c39` ,
         `c40`
        FROM 
            (SELECT array ( `_______0` ) AS _______temp,
         `c24` ,
         `c28` ,
         `c26` ,
         `c21` ,
         `c25` ,
         `c29` ,
         `c5` ,
         `c6` ,
         `c7` ,
         `c15` ,
         `c4` ,
         `c27` ,
         `c19` ,
         `c18` ,
         `c20` ,
         `c8` ,
         `c13` ,
         `c16` ,
         `c11` ,
         `c10` ,
         `c14` ,
         `c9` ,
         `c12` ,
         `c17` ,
         `c22` ,
         `c39` ,
         `c40`
            FROM ( select
                CASE
                WHEN c57 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c63 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c73 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c72 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c72 = '______' THEN
                '______'
                ELSE (
                CASE
                WHEN c72 = '______' THEN
                '______'
                ELSE '______'
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END )
                END AS `_______0` , sum(c24) AS `c24` , sum(c28) AS `c28` , sum(c26) AS `c26` , sum(c21) AS `c21` , sum(c25) AS `c25` , sum(c29) AS `c29` , sum(c5) AS `c5` , sum(c6) AS `c6` , sum(c7) AS `c7` , sum(c15) AS `c15` , sum(c4) AS `c4` , sum(c27) AS `c27` , sum(c19) AS `c19` , sum(c18) AS `c18` , sum(c20) AS `c20` , sum(c8) AS `c8` , sum(c13) AS `c13` , sum(c16) AS `c16` , sum(c11) AS `c11` , sum(c10) AS `c10` , sum(c14) AS `c14` , sum(c9) AS `c9` , sum(c12) AS `c12` , sum(c17) AS `c17` , sum(c22)c22 , sum(c39)c39 , sum(c40)c40
            FROM slow_fold_constant_case_when
            WHERE slow_fold_constant_case_when.`c43` IN ('______')
                    AND (( ((`c72` IN ('______'))
                    OR (`c73` IN ('______'))))
                    OR (`c63` IN ('______')))
            GROUP BY  _______0 limit 3000 ) t ) t lateral view explode ( `_______temp` ) c0 AS _____
            WHERE _____ is NOT null
                    AND _____ != '______' ) t
            GROUP BY  grouping sets((_____) ,())
            ORDER BY  _____ nulls last limit 3000 ) t
        ORDER BY  _____ nulls last
            """

    sql "set enable_fold_constant_by_be=false"
    test {
        sql sqlStr
        time 60000
    }

    sql "set enable_fold_constant_by_be=true"
    test {
        sql sqlStr
        time 60000
    }
}
