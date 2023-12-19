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

suite("test_compound_predicate_constant_child", "query") {
    // define a sql table
    def testTable = "test_compound_predicate_constant_child"
    sql """drop table if exists ${testTable};"""

    sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
                c0 BOOLEAN
            )
            AGGREGATE KEY(c0)
            DISTRIBUTED BY HASH (c0)
            BUCKETS 28
            PROPERTIES (
                "replication_num" = "1"
            );
            """
    // prepare data
    sql """ INSERT INTO ${testTable} VALUES (false) """
    sql """ INSERT INTO ${testTable} VALUES (true) """

    sql """ set experimental_enable_nereids_planner = true """
    sql """ set enable_fallback_to_original_planner = false """
    sql """ set enable_fold_constant_by_be=true """

    qt_select """ SELECT SUM(count) FROM
                (SELECT CAST((NOT ((true)||(CASE ${testTable}.c0  WHEN ${testTable}.c0 THEN false  WHEN ${testTable}.c0 THEN true ELSE true END ))) IS NOT NULL AND
                (NOT ((true)||(CASE ${testTable}.c0  WHEN ${testTable}.c0 THEN false  WHEN ${testTable}.c0 THEN true ELSE true END ))) AS INT) as count
                FROM ${testTable}) as res;
              """

    sql """ set enable_fold_constant_by_be=true """

    qt_select """ SELECT SUM(count) FROM
                (SELECT CAST((NOT ((true)||(CASE ${testTable}.c0  WHEN ${testTable}.c0 THEN false  WHEN ${testTable}.c0 THEN true ELSE true END ))) IS NOT NULL AND
                (NOT ((true)||(CASE ${testTable}.c0  WHEN ${testTable}.c0 THEN false  WHEN ${testTable}.c0 THEN true ELSE true END ))) AS INT) as count
                FROM ${testTable}) as res;
              """

   sql """ set enable_fold_constant_by_be=true """

   qt_select """ SELECT SUM(count) FROM
               (SELECT CAST((NOT ((1378719999)||(CASE ${testTable}.c0  WHEN ${testTable}.c0 THEN -388844163  WHEN ${testTable}.c0 THEN 1455674610 ELSE 671348352 END ))) IS NOT NULL AND
               (NOT ((1378719999)||(CASE ${testTable}.c0  WHEN ${testTable}.c0 THEN -388844163  WHEN ${testTable}.c0 THEN 1455674610 ELSE 671348352 END ))) AS INT) as count
               FROM ${testTable}) as res;
             """

   sql """ set enable_fold_constant_by_be=false """

   qt_select """ SELECT SUM(count) FROM
               (SELECT CAST((NOT ((1378719999)||(CASE ${testTable}.c0  WHEN ${testTable}.c0 THEN -388844163  WHEN ${testTable}.c0 THEN 1455674610 ELSE 671348352 END ))) IS NOT NULL AND
               (NOT ((1378719999)||(CASE ${testTable}.c0  WHEN ${testTable}.c0 THEN -388844163  WHEN ${testTable}.c0 THEN 1455674610 ELSE 671348352 END ))) AS INT) as count
               FROM ${testTable}) as res;
             """
}
