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

suite("test_join3", "query,p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def DBname = "regression_test_join3"
    sql "DROP DATABASE IF EXISTS ${DBname}"
    sql "CREATE DATABASE IF NOT EXISTS ${DBname}"
    sql "use ${DBname}"

    def tbName1 = "t1"
    def tbName2 = "t2"
    def tbName3 = "t3"

    sql """CREATE TABLE IF NOT EXISTS ${tbName1} (name varchar(255), n INTEGER) DISTRIBUTED BY HASH(name) properties("replication_num" = "1");"""
    sql """CREATE TABLE IF NOT EXISTS ${tbName2} (name varchar(255), n INTEGER) DISTRIBUTED BY HASH(name) properties("replication_num" = "1");"""
    sql """CREATE TABLE IF NOT EXISTS ${tbName3} (name varchar(255), n INTEGER) DISTRIBUTED BY HASH(name) properties("replication_num" = "1");"""

    sql "INSERT INTO ${tbName1} VALUES ( 'bb', 11 );"
    sql "INSERT INTO ${tbName2} VALUES ( 'bb', 12 );"
    sql "INSERT INTO ${tbName2} VALUES ( 'cc', 22 );"
    sql "INSERT INTO ${tbName2} VALUES ( 'ee', 42 );"
    sql "INSERT INTO ${tbName3} VALUES ( 'bb', 13 );"
    sql "INSERT INTO ${tbName3} VALUES ( 'cc', 23 );"
    sql "INSERT INTO ${tbName3} VALUES ( 'dd', 33 );"

    qt_join1 """
            SELECT * FROM ${tbName1} FULL JOIN ${tbName2} USING (name) FULL JOIN ${tbName3} USING (name) ORDER BY 1,2,3,4,5,6;
        """
    qt_join2 """
            SELECT * FROM
            (SELECT * FROM ${tbName2}) as s2
            INNER JOIN
            (SELECT * FROM ${tbName3}) s3
            USING (name)
            ORDER BY 1,2,3,4;
        """
    qt_join3 """
            SELECT * FROM
            (SELECT * FROM ${tbName2}) as s2
            LEFT JOIN
            (SELECT * FROM ${tbName3}) s3
            USING (name)
            ORDER BY 1,2,3,4;
        """
    qt_join4 """
            SELECT * FROM
            (SELECT * FROM ${tbName2}) as s2
            FULL JOIN
            (SELECT * FROM ${tbName3}) s3
            USING (name)
            ORDER BY 1,2,3,4;
        """

// wait fix
//     qt_join5 """
//             SELECT * FROM
//             (SELECT name, n as s2_n, 2 as s2_2 FROM ${tbName2}) as s2
//             NATURAL INNER JOIN
//             (SELECT name, n as s3_n, 3 as s3_2 FROM ${tbName3}) s3
//             ORDER BY 1,2,3,4;
//         """

//     qt_join6 """
//             SELECT * FROM
//             (SELECT name, n as s1_n, 1 as s1_1 FROM ${tbName1}) as s1
//             NATURAL INNER JOIN
//             (SELECT name, n as s2_n, 2 as s2_2 FROM ${tbName2}) as s2
//             NATURAL INNER JOIN
//             (SELECT name, n as s3_n, 3 as s3_2 FROM ${tbName3}) s3;
//         """

    qt_join7 """
        SELECT * FROM
          (SELECT name, n as s1_n FROM ${tbName1}) as s1
        FULL JOIN
          (SELECT name, 2 as s2_n FROM ${tbName2}) as s2
        ON (s1_n = s2_n)
        ORDER BY 1,2,3,4;
        """

    sql "DROP DATABASE IF EXISTS ${DBname}"
}
