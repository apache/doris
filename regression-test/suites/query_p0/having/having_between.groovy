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

suite("test_having_between", "query,p0") {
    def DBname = "test_having_between"
    def tableName = "tbl_having_between"
    sql "DROP DATABASE IF EXISTS ${DBname}"
    sql "CREATE DATABASE IF NOT EXISTS ${DBname}"
    sql "use ${DBname}"

    sql "DROP TABLE IF EXISTS ${tableName};"

    sql """
        CREATE TABLE ${tableName}(a DATE, b VARCHAR(100), c VARCHAR(100), d VARCHAR(100), e INT, f INT)  
        ENGINE=OLAP
        DUPLICATE KEY( a )
        COMMENT "OLAP"
        DISTRIBUTED BY HASH( e ) BUCKETS  auto
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    def test_query = {
        qt_sql01 """
            select t1.b,t1.c,t1.f,t1.d,t1.e,stddev_pop(t1.d+t1.e) std from ${tableName} t1 
            group by t1.f,t1.d,t1.e,t1.a,t1.b,t1.c
            having t1.b between 'aa' and 'bb' order by 1;
        """

        qt_sql02 """
            SELECT b, count(*) FROM ${tableName} GROUP BY 1 HAVING b BETWEEN 'aa' AND 'bb' order by 1;
        """

        qt_sql03 """
            SELECT b, count(*) FROM ${tableName} GROUP BY 1 HAVING b >= 'aa' AND b <= 'bb' order by 1;
        """
    }

    sql "INSERT INTO ${tableName} VALUES(now(), 'a', 'b', 'c', 11, 22);"
    sql "INSERT INTO ${tableName} VALUES(now(), 'aa', 'ba', 'cc', 11, 22);"
    sql "INSERT INTO ${tableName} VALUES(now(), 'b', 'b', 'c', 121, 242);"
    sql "INSERT INTO ${tableName} VALUES(now(), 'bb', 'b', 'c', 11, 22);"
    sql "INSERT INTO ${tableName} VALUES(now(), 'bbb', 'b', 'c', 11, 22);"

    test_query()

    sql "DROP TABLE ${tableName};"
    sql "DROP DATABASE ${DBname};"
}
