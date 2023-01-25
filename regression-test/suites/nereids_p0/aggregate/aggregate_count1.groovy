/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("aggregate_count1", "query") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    sql """ DROP TABLE IF EXISTS aggregate_count1 """
    sql """create table if not exists aggregate_count1 (
                name varchar(128),
                age INT,
                identityCode  varchar(128),
                cardNo String,
                number String,
                birthday DATETIME,
                birthday1 DATETIMEV2,
                birthday2 DATETIMEV2(3),
                birthday3 DATETIMEV2(6),
                country String,
                gender String,
                covid BOOLEAN 
            )UNIQUE KEY(name,age,`identityCode`)
            DISTRIBUTED BY HASH(`identityCode`) BUCKETS 10
            PROPERTIES("replication_num" = "1");
            """
    sql "insert into aggregate_count1 values ('张三0',11,'1234567','123','321312','1999-02-13','1999-02-13','1999-02-13','1999-02-13','中国','男',false)," +
            "('张三1',11,'12345678','123','321312','1999-02-13','1999-02-13','1999-02-13','1999-02-13','中国','男',false)," +
            "('张三2',11,'12345671','123','321312','1999-02-13','1999-02-13','1999-02-13','1999-02-13','中国','男',false)," +
            "('张三3',11,'12345673','123','321312','1999-02-13','1999-02-13','1999-02-13','1999-02-13','中国','男',false)," +
            "('张三4',11,'123456711','123','321312','1999-02-13','1999-02-13','1999-02-13','1999-02-13','中国','男',false)," +
            "('张三5',11,'1232134567','123','321312','1999-02-13','1999-02-13','1999-02-13','1999-02-13','中国','男',false)," +
            "('张三6',11,'124314567','123','321312','1999-02-13','1999-02-13','1999-02-13','1999-02-13','中国','男',false)," +
            "('张三7',11,'123445167','123','321312','1998-02-13','1999-02-13','1999-02-13','1999-02-13','中国','男',false);"
    qt_select """ SELECT count(1) FROM (WITH t1 AS (
                 WITH t AS (
                            SELECT * FROM aggregate_count1
                    )
                    SELECT
                            identityCode,
                            COUNT(1) as dataAmount,
                            ROUND(COUNT(1) / tableWithSum.sumResult,4) as proportion,
                           MD5(identityCode) as virtuleUniqKey
                    FROM t,(SELECT COUNT(1) as sumResult from t) tableWithSum
                    GROUP BY identityCode ,tableWithSum.sumResult
            )
            SELECT
                    identityCode,dataAmount,
                    (
                            CASE
                            WHEN t1.virtuleUniqKey = tableWithMaxId.max_virtuleUniqKey THEN
                                    ROUND(proportion + calcTheTail, 4)
                            ELSE
                                    proportion
                          END
                    ) proportion
            FROM t1,
                    (SELECT (1 - sum(t1.proportion)) as calcTheTail FROM t1 ) tableWithTail,
                    (SELECT virtuleUniqKey as  max_virtuleUniqKey FROM t1 ORDER BY proportion DESC LIMIT 1 ) tableWithMaxId
            ORDER BY identityCode) t_a76fe3e829ddb51;
            """
    sql "drop table aggregate_count1"
}