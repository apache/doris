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

suite("test_materialized_view_load_open", "rollup") {

    def tbName1 = "test_materialized_view_load_open"
    def tbName2 = "test_materialized_view_load_open_dynamic_partition"
    def tbName3 = "test_materialized_view_load_open_schema_change"
    def tbName4 = "test_materialized_view_load_open_dynamic_partition_schema_change"

    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1; """
        return jobStateResult[0][8]
    }

    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1}(
                k1 DATE,
                k2 DECIMAL(10, 2),
                k3 CHAR(10),
                k4 INT NOT NULL
            )
            DUPLICATE KEY(k1, k2)
            PARTITION BY RANGE(k1)
            (
               PARTITION p1 VALUES LESS THAN ("2000-01-01"),
               PARTITION p2 VALUES LESS THAN ("2010-01-01"),
               PARTITION p3 VALUES LESS THAN ("2020-01-01")
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 32 
            properties(
                "light_schema_change" = "false",
                "replication_num" = "1"
            );
        """

    sql "DROP TABLE IF EXISTS ${tbName2}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName2}(
                k1 DATE,
                k2 DECIMAL(10, 2),
                k3 CHAR(10),
                k4 INT NOT NULL
            )
            PARTITION BY RANGE(k1)
            (
               PARTITION p1 VALUES LESS THAN ("2000-01-01"),
               PARTITION p2 VALUES LESS THAN ("2010-01-01"),
               PARTITION p3 VALUES LESS THAN ("2020-01-01")
            )
            DISTRIBUTED BY HASH(k1)
            PROPERTIES
            (
                "dynamic_partition.enable" = "true",
                "dynamic_partition.time_unit" = "DAY",
                "dynamic_partition.start" = "-2147483648",
                "dynamic_partition.end" = "3",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.buckets" = "32",
                "light_schema_change" = "false",
                "replication_num"="1"
            );
        """
    
    sql "DROP TABLE IF EXISTS ${tbName3}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName3}(
                k1 DATE,
                k2 DECIMAL(10, 2),
                k3 CHAR(10),
                k4 INT NOT NULL
            )
            DUPLICATE KEY(k1, k2)
            PARTITION BY RANGE(k1)
            (
               PARTITION p1 VALUES LESS THAN ("2000-01-01"),
               PARTITION p2 VALUES LESS THAN ("2010-01-01"),
               PARTITION p3 VALUES LESS THAN ("2020-01-01")
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 32 

            properties(
                "light_schema_change" = "true",
                "replication_num" = "1"
            );
        """
    
    sql "DROP TABLE IF EXISTS ${tbName4}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName4}(
                k1 DATE,
                k2 DECIMAL(10, 2),
                k3 CHAR(10),
                k4 INT NOT NULL
            )
            PARTITION BY RANGE(k1)
            (
               PARTITION p1 VALUES LESS THAN ("2000-01-01"),
               PARTITION p2 VALUES LESS THAN ("2010-01-01"),
               PARTITION p3 VALUES LESS THAN ("2020-01-01")
            )
            DISTRIBUTED BY HASH(k1)
            PROPERTIES
            (
                "dynamic_partition.enable" = "true",
                "dynamic_partition.time_unit" = "DAY",
                "dynamic_partition.start" = "-2147483648",
                "dynamic_partition.end" = "3",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.buckets" = "32",
                "light_schema_change" = "true",
                "replication_num"="1"
            );
        """
    
    createMV("CREATE materialized VIEW test_load_open AS SELECT k1 FROM ${tbName1} GROUP BY k1;")
    createMV("CREATE materialized VIEW test_load_open_dynamic_partition AS SELECT k1 FROM ${tbName2} GROUP BY k1;")
    createMV("CREATE materialized VIEW test_load_open_schema_change AS SELECT k1 FROM ${tbName3} GROUP BY k1;")
    createMV("CREATE materialized VIEW test_load_open_dynamic_partition_schema_change AS SELECT k1 FROM ${tbName4} GROUP BY k1;")

    sql "insert into ${tbName1} values('2000-05-20', 1.5, 'test', 1);"
    sql "insert into ${tbName1} values('2010-05-20', 1.5, 'test', 1);"

    sql "insert into ${tbName2} values('2000-05-20', 1.5, 'test', 1);"
    sql "insert into ${tbName2} values('2010-05-20', 1.5, 'test', 1);"

    sql "insert into ${tbName3} values('2000-05-20', 1.5, 'test', 1);"
    sql "ALTER table ${tbName3} ADD COLUMN new_column INT;"
    sql "insert into ${tbName3} values('2010-05-20', 1.5, 'test', 1, 1);"

    sql "insert into ${tbName4} values('2000-05-20', 1.5, 'test', 1);"
    sql "ALTER table ${tbName4} ADD COLUMN new_column INT;"
    sql "insert into ${tbName4} values('2010-05-20', 1.5, 'test', 1, 1);"

    qt_select "select * from test_materialized_view_load_open_schema_change order by 1,2,3,4;"

    streamLoad {
        table "test_materialized_view_load_open_schema_change"
        set 'column_separator', ','
        file 'a.csv'
        time 10000 // limit inflight 10s
    }

    qt_select "select * from test_materialized_view_load_open_schema_change order by 1,2,3,4;"
}