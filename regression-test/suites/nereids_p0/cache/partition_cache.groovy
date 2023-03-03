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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("partition_cache") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def tableName = "test_partition_cache"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` date NOT NULL COMMENT "",
              `k2` int(11) NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`)
            COMMENT "OLAP"
            PARTITION BY RANGE(`k1`)
            (PARTITION p202205 VALUES [('2022-05-01'), ('2022-06-01')),
            PARTITION p202206 VALUES [('2022-06-01'), ('2022-07-01')))
            DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 32
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
        """

    sql "sync"

    sql """ INSERT INTO ${tableName} VALUES 
                    ("2022-05-27",0),
                    ("2022-05-28",0),
                    ("2022-05-29",0),
                    ("2022-05-30",0),
                    ("2022-06-01",0),
                    ("2022-06-02",0)
        """
    sql " set enable_partition_cache=true "

    qt_partition_cache """
                        select
                          k1,
                          sum(k2) as total_pv 
                        from
                          ${tableName} 
                        where
                          k1 between '2022-05-28' and '2022-06-30' 
                        group by
                          k1 
                        order by
                          k1;
                       """
    qt_partition_cache """
                        select
                          k1,
                          sum(k2) as total_pv 
                        from
                          ${tableName} 
                        where
                          k1 between '2022-05-28' and '2022-06-30' 
                        group by
                          k1 
                        order by
                          k1;
                       """
    sql " set enable_partition_cache=false "

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` datev2 NOT NULL COMMENT "",
              `k2` int(11) NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`)
            COMMENT "OLAP"
            PARTITION BY RANGE(`k1`)
            (PARTITION p202205 VALUES [('2022-05-01'), ('2022-06-01')),
            PARTITION p202206 VALUES [('2022-06-01'), ('2022-07-01')))
            DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 32
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName} VALUES
                    ("2022-05-27",0),
                    ("2022-05-28",0),
                    ("2022-05-29",0),
                    ("2022-05-30",0),
                    ("2022-06-01",0),
                    ("2022-06-02",0)
        """
    sql " set enable_partition_cache=true "

    qt_partition_cache """
                        select
                          k1,
                          sum(k2) as total_pv
                        from
                          ${tableName}
                        where
                          k1 between '2022-05-28' and '2022-06-30'
                        group by
                          k1
                        order by
                          k1;
                       """
    qt_partition_cache """
                        select
                          k1,
                          sum(k2) as total_pv
                        from
                          ${tableName}
                        where
                          k1 between '2022-05-28' and '2022-06-30'
                        group by
                          k1
                        order by
                          k1;
                       """
    sql " set enable_partition_cache=false "

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` datetimev2(3) NOT NULL COMMENT "",
              `k2` int(11) NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`)
            COMMENT "OLAP"
            PARTITION BY RANGE(`k1`)
            (PARTITION p202205 VALUES [('2022-05-01 11:11:11.111'), ('2022-06-01 11:11:11.111')),
            PARTITION p202206 VALUES [('2022-06-01 11:11:11.111'), ('2022-07-01 11:11:11.111')))
            DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 32
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName} VALUES
                    ("2022-05-27 11:11:11.111",0),
                    ("2022-05-28 11:11:11.111",0),
                    ("2022-05-29 11:11:11.111",0),
                    ("2022-05-30 11:11:11.111",0),
                    ("2022-06-01 11:11:11.111",0),
                    ("2022-06-02 11:11:11.111",0)
        """
    sql " set enable_partition_cache=true "

    qt_partition_cache """
                        select
                          k1,
                          sum(k2) as total_pv
                        from
                          ${tableName}
                        where
                          k1 between '2022-05-28 11:11:11.111' and '2022-06-30 11:11:11.111'
                        group by
                          k1
                        order by
                          k1;
                       """
    qt_partition_cache """
                        select
                          k1,
                          sum(k2) as total_pv
                        from
                          ${tableName}
                        where
                          k1 between '2022-05-28 11:11:11.111' and '2022-06-30 11:11:11.111'
                        group by
                          k1
                        order by
                          k1;
                       """
    sql " set enable_partition_cache=false "

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` datetimev2(3) NOT NULL COMMENT "",
              `k2` int(11) NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`)
            COMMENT "OLAP"
            PARTITION BY RANGE(`k1`)
            (PARTITION p202205 VALUES [('2022-05-01 11:11:11.111111'), ('2022-06-01 11:11:11.111111')),
            PARTITION p202206 VALUES [('2022-06-01 11:11:11.111111'), ('2022-07-01 11:11:11.111111')))
            DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 32
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName} VALUES
                    ("2022-05-27 11:11:11.111",0),
                    ("2022-05-28 11:11:11.111",0),
                    ("2022-05-29 11:11:11.111",0),
                    ("2022-05-30 11:11:11.111",0),
                    ("2022-06-01 11:11:11.111",0),
                    ("2022-06-02 11:11:11.111",0)
        """
    sql " set enable_partition_cache=true "

    qt_partition_cache """
                        select
                          k1,
                          sum(k2) as total_pv
                        from
                          ${tableName}
                        where
                          k1 between '2022-05-28 11:11:11.111' and '2022-06-30 11:11:11.111'
                        group by
                          k1
                        order by
                          k1;
                       """
    qt_partition_cache """
                        select
                          k1,
                          sum(k2) as total_pv
                        from
                          ${tableName}
                        where
                          k1 between '2022-05-28 11:11:11.111' and '2022-06-30 11:11:11.111'
                        group by
                          k1
                        order by
                          k1;
                       """
    sql " set enable_partition_cache=false "

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` datetimev2(6) NOT NULL COMMENT "",
              `k2` int(11) NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`)
            COMMENT "OLAP"
            PARTITION BY RANGE(`k1`)
            (PARTITION p202205 VALUES [('2022-05-01 11:11:11.111'), ('2022-06-01 11:11:11.111')),
            PARTITION p202206 VALUES [('2022-06-01 11:11:11.111'), ('2022-07-01 11:11:11.111')))
            DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 32
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName} VALUES
                    ("2022-05-27 11:11:11.111111",0),
                    ("2022-05-28 11:11:11.111111",0),
                    ("2022-05-29 11:11:11.111111",0),
                    ("2022-05-30 11:11:11.111111",0),
                    ("2022-06-01 11:11:11.111111",0),
                    ("2022-06-02 11:11:11.111111",0)
        """
    sql " set enable_partition_cache=true "

    qt_partition_cache """
                        select
                          k1,
                          sum(k2) as total_pv
                        from
                          ${tableName}
                        where
                          k1 between '2022-05-28 11:11:11.111' and '2022-06-30 11:11:11.111'
                        group by
                          k1
                        order by
                          k1;
                       """
    qt_partition_cache """
                        select
                          k1,
                          sum(k2) as total_pv
                        from
                          ${tableName}
                        where
                          k1 between '2022-05-28 11:11:11.111' and '2022-06-30 11:11:11.111'
                        group by
                          k1
                        order by
                          k1;
                       """
    sql " set enable_partition_cache=false "
}
