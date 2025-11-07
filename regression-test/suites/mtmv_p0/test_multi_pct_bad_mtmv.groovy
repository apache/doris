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

import org.junit.Assert;

suite("test_multi_pct_bad_mtmv","mtmv") {
    String suiteName = "test_multi_pct_bad_mtmv"
    String dbName = context.config.getDbNameByFile(context.file)
    String tableName1 = "${suiteName}_table1"
    String tableName2 = "${suiteName}_table2"
    String tableName3 = "${suiteName}_table3"
    String mvName = "${suiteName}_mv"
    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop table if exists `${tableName3}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName1}
        (
            k1 DATE not null,
            k2 INT not null
        )
        PARTITION BY RANGE(`k1`)
        (
            PARTITION `p201701` VALUES [("2017-01-01"),  ("2017-02-01")),
            PARTITION `p201702` VALUES [("2017-02-01"), ("2017-03-01"))
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        CREATE TABLE ${tableName2}
        (
            k1 DATE not null,
            k2 INT not null
        )
        PARTITION BY RANGE(`k1`)
        (
            PARTITION `p201702` VALUES [("2017-01-01"),  ("2017-03-01"))
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    String mvSql = "SELECT t1.k1,t1.k2,t2.k2 as k3 from ${tableName1} t1 inner join ${tableName2} t2 on t1.k1=t2.k1;";

    test {
        sql """
                CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by(k1)
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES (
                'replication_num' = '1'
                )
                AS
                ${mvSql}
                """
        exception "intersected"
    }

    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop table if exists `${tableName3}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName1}
        (
            k1 DATE not null,
            k2 INT not null
        )
        PARTITION BY RANGE(`k1`)
        (
            PARTITION `p201701` VALUES [("2017-01-01"),  ("2017-02-01")),
            PARTITION `p201702` VALUES [("2017-02-01"), ("2017-03-01"))
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        CREATE TABLE ${tableName2}
        (
            k1 DATE not null,
            k2 INT not null
        )
        PARTITION BY RANGE(`k1`)
        (
            PARTITION `p201702` VALUES [("2017-02-01"),  ("2017-03-01")),
            PARTITION `p201703` VALUES [("2017-03-01"), ("2017-04-01"))
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        CREATE TABLE ${tableName3}
        (
            k3 INT not null,
            k4 INT not null
        )
        DISTRIBUTED BY HASH(k3) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    test {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(k1)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            select * from  ${tableName1}
            union all
            select * from  ${tableName2}
            union all
            select * from  ${tableName3}
            """
        exception "suitable"
    }

    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop table if exists `${tableName3}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName1}
        (
            k1 DATE,
            k2 INT not null
        )
        PARTITION BY RANGE(`k1`)
        (
            PARTITION `p201701` VALUES LESS THAN ("2017-01-01"),
            PARTITION `p201702` VALUES LESS THAN ("2017-02-01"),
            PARTITION `pmax` VALUES LESS THAN (maxvalue)
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        CREATE TABLE ${tableName2}
        (
            k1 DATE,
            k2 INT not null
        )
        PARTITION BY RANGE(`k1`)
        (
            PARTITION `p201701` VALUES LESS THAN ("2017-01-01"),
            PARTITION `p201703` VALUES LESS THAN ("2017-03-01"),
            PARTITION `pmax` VALUES LESS THAN (maxvalue)
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    test {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(k1)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            select * from  ${tableName1}
            union all
            select * from  ${tableName2}
            """
        exception "intersected"
    }

    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop table if exists `${tableName3}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName1}
        (
            k1 INT not null,
            k2 INT not null
        )
        PARTITION BY LIST(`k1`)
        (
            PARTITION `p1` VALUES IN ("1","2"),
            PARTITION `p2` VALUES IN ("3")
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        CREATE TABLE ${tableName2}
        (
            k1 INT not null,
            k2 INT not null
        )
        PARTITION BY LIST(`k1`)
        (
            PARTITION `p2` VALUES IN ("2"),
            PARTITION `p3` VALUES IN ("4")
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    test {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(k1)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            select * from  ${tableName1}
            union all
            select * from  ${tableName2}
            """
        exception "repeat"
    }

    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop table if exists `${tableName3}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
         CREATE TABLE ${tableName1}
            (
                k1 DATE not null,
                k2 INT not null
            )
            PARTITION BY RANGE(`k1`)
            (
                 PARTITION `p201701` VALUES [("2017-01-01"),  ("2017-02-01")),
                 PARTITION `p201702` VALUES [("2017-02-01"), ("2017-03-01"))
            )
            DISTRIBUTED BY HASH(k2) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """

    sql """
        CREATE TABLE ${tableName2}
        (
            k1 DATE not null,
            k2 INT not null
        )
        PARTITION BY LIST(`k1`)
        (
            PARTITION `p2` VALUES IN ("2017-01-01"),
            PARTITION `p3` VALUES IN ("2017-02-01")
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    test {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(k1)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            select * from  ${tableName1}
            union all
            select * from  ${tableName2}
            """
        exception "same"
    }

    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop table if exists `${tableName3}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName1}
        (
            k1 INT not null,
            k2 INT not null
        )
        PARTITION BY LIST(`k1`)
        (
            PARTITION `p1` VALUES IN ("1"),
            PARTITION `p2` VALUES IN ("2")
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        CREATE TABLE ${tableName2}
        (
            k1 bigint not null,
            k2 INT not null
        )
        PARTITION BY LIST(`k1`)
        (
            PARTITION `p1` VALUES IN ("1"),
            PARTITION `p2` VALUES IN ("2")
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    test {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(k1)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            select * from  ${tableName1}
            union all
            select * from  ${tableName2}
            """
        exception "suitable"
    }
}
