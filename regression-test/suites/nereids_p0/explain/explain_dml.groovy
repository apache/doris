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

suite("explain_dml") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    multi_sql """
        drop table if exists epldel1;
        CREATE TABLE epldel1
        (id INT, c1 BIGINT, c2 STRING, c3 DOUBLE, c4 DATE)
        UNIQUE KEY (id)
        DISTRIBUTED BY HASH (id)
        PROPERTIES('replication_num'='1', "function_column.sequence_col" = "c4");
        
        drop table if exists epldel2;
        CREATE TABLE epldel2
        (id INT, c1 BIGINT, c2 STRING, c3 DOUBLE, c4 DATE)
        DISTRIBUTED BY HASH (id)
        PROPERTIES('replication_num'='1');

        drop table if exists epldel3;
        CREATE TABLE epldel3
        (id INT)
        DISTRIBUTED BY HASH (id)
        PROPERTIES('replication_num'='1');

        INSERT INTO epldel1 VALUES
        (1, 1, '1', 1.0, '2000-01-01'),
        (2, 2, '2', 2.0, '2000-01-02'),
        (3, 3, '3', 3.0, '2000-01-03');

        INSERT INTO epldel2 VALUES
        (1, 10, '10', 10.0, '2000-01-10'),
        (2, 20, '20', 20.0, '2000-01-20'),
        (3, 30, '30', 30.0, '2000-01-30'),
        (4, 4, '4', 4.0, '2000-01-04'),
        (5, 5, '5', 5.0, '2000-01-05');

        INSERT INTO epldel3 VALUES
        (1),
        (4),
        (5);

        drop table if exists aggtbl;
        CREATE TABLE `aggtbl` (
                `k1` int(11) NULL COMMENT "",
                `v1` int(11) SUM DEFAULT "0",
                `v2` int(11) SUM DEFAULT "0"
                ) 
                aggregate key (k1)
                DISTRIBUTED BY HASH(`k1`) BUCKETS 10
                PROPERTIES ('replication_num' = '1');
        insert into aggtbl values (1, 1, 1);

        drop table if exists duptbl;
        CREATE TABLE `duptbl` (
                `k1` int(11) NULL COMMENT "",
                `v1` int(11) SUM DEFAULT "0",
                `v2` int(11) SUM DEFAULT "0"
                ) 
                aggregate key (k1)
                DISTRIBUTED BY HASH(`k1`) BUCKETS 10
                PROPERTIES ('replication_num' = '1');
        insert into duptbl values (1,1,1);
    """
    
    explain {
        sql "delete from epldel1 where id=0;"
        contains "PLAN FRAGMENT 0"
    } 
    
    explain {
        sql """
            DELETE FROM epldel1
            USING epldel2 INNER JOIN epldel3 ON epldel2.id = epldel3.id
            WHERE epldel1.id = epldel2.id;
            """
        contains "PLAN FRAGMENT 0"
    }

    test {
        sql "explain delete from aggtbl where v1=6;"
        exception "delete command on aggregate/duplicate table is not explainable"
    }

    test {
        sql """
            explain DELETE FROM aggtbl
            USING epldel2 INNER JOIN epldel3 ON epldel2.id = epldel3.id
            WHERE aggtbl.k1 = epldel2.id;"""
        exception "delete command on with using clause only supports unique key model"
    }

    test {
        sql "delete from aggtbl where v1=6;"
        exception "delete predicate on value column only supports Unique table with merge-on-write enabled and Duplicate table, but Table[aggtbl] is an Aggregate table."
    }

    test {
        sql "update aggtbl set v1=1 where k1=1;"
        exception "Only unique table could be updated."
    }

    test {
        sql "update duptbl set v1=1 where k1=1;"
        exception "Only unique table could be updated."
    }
}
