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

// this suite is for creating table with timestamp datatype in defferent 
// case. For example: 'year' and 'Year' datatype should also be valid in definition

import org.apache.doris.regression.suite.ClusterOptions

suite("sql_create_table_view_when_exceed_limit", "docker") {
    if (isCloudMode()) {
        return;
    }
    def options = new ClusterOptions()
    options.feConfigs += [
        'sys_log_verbose_modules=org',
        'dynamic_partition_check_interval_seconds=1'
    ]
    options.beConfigs += [
        'sys_log_verbose_modules=*',
        'be_tablet_num_upper_limit=1000000',
    ]

    options.enableDebugPoints()

    docker(options) {
        sql """
        CREATE TABLE base_table
        (
            k2 TINYINT,
            k3 INT not null
        )
        COMMENT "base"
        PARTITION BY LIST(`k3`)
        (
            PARTITION `p1` VALUES IN ('1'),
            PARTITION `p2` VALUES IN ('2'),
            PARTITION `p3` VALUES IN ('3')
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
        sql """insert into base_table values(1,1),(2,2),(3,3);"""

        GetDebugPoint().enableDebugPointForAllBEs("WorkPoolCreateTablet.check_tablet_limit.change_tablet_num_in_bvar")
        // with partition table
        test {
            sql """
                create table test_time_range_table_exceed_limit (
                `actorid` varchar(128),
                `gameid` varchar(128),
                `eventtime` datetimev2(3)
                )
                engine=olap
                duplicate key(actorid, gameid, eventtime)
                partition by range(eventtime)(
                    from ("2000-01-01") to ("2021-01-01") interval 1 year,
                    from ("2021-01-01") to ("2022-01-01") interval 1 MONth,
                    from ("2022-01-01") to ("2023-01-01") interval 1 WEEK,
                    from ("2023-01-01") TO ("2023-02-01") interval 1 DAY
                )
                distributed by hash(actorid) buckets 1
                properties(
                    "replication_num"="1",
                    "light_schema_change"="true",
                    "compression"="zstd"
                );
                """
                exception ("""[DATA_QUALITY_ERROR]Error: The current number of tablets in BE (1000001) exceeds the allowed limit (1000000).
This may include unused tablets that were dropped from the frontend but not asynchronously dropped from the backend.
Please check your usage of mtmv (e.g., refreshing too frequently) or any misuse of auto partition.
You can resolve this issue by:
1. Increasing the value of 'be_tablet_num_upper_limit'.
2. Executing the SQL command 'ADMIN CLEAN TRASH' to clean up trash data.
3. Optionally, you can set 'trash_file_expire_time_sec' to 0 to disable trash file expiration.]""")
        }

        // without partition table
        test {
            sql "create table varchar_0_char_0 (id int, a varchar(0), b char(0)) distributed by hash(id) properties(\"replication_num\"=\"1\")\n"
            exception ("""[DATA_QUALITY_ERROR]Error: The current number of tablets in BE (1000001) exceeds the allowed limit (1000000).
This may include unused tablets that were dropped from the frontend but not asynchronously dropped from the backend.
Please check your usage of mtmv (e.g., refreshing too frequently) or any misuse of auto partition.
You can resolve this issue by:
1. Increasing the value of 'be_tablet_num_upper_limit'.
2. Executing the SQL command 'ADMIN CLEAN TRASH' to clean up trash data.
3. Optionally, you can set 'trash_file_expire_time_sec' to 0 to disable trash file expiration.]""")
        }

        // dynamic patition table
        test {
            sql """
            CREATE TABLE IF NOT EXISTS dy_par ( k1 date NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL )
            AGGREGATE KEY(k1,k2)
            PARTITION BY RANGE(k1) ( )
            DISTRIBUTED BY HASH(k1) BUCKETS 3
            PROPERTIES (
                "dynamic_partition.enable"="true",
                "dynamic_partition.end"="3",
                "dynamic_partition.buckets"="10",
                "dynamic_partition.start"="-3",
                "dynamic_partition.prefix"="p",
                "dynamic_partition.time_unit"="DAY",
                "dynamic_partition.create_history_partition"="false",
                "dynamic_partition.replication_allocation" = "tag.location.default: 1")
            """
            exception ("""[DATA_QUALITY_ERROR]Error: The current number of tablets in BE (1000001) exceeds the allowed limit (1000000).
This may include unused tablets that were dropped from the frontend but not asynchronously dropped from the backend.
Please check your usage of mtmv (e.g., refreshing too frequently) or any misuse of auto partition.
You can resolve this issue by:
1. Increasing the value of 'be_tablet_num_upper_limit'.
2. Executing the SQL command 'ADMIN CLEAN TRASH' to clean up trash data.
3. Optionally, you can set 'trash_file_expire_time_sec' to 0 to disable trash file expiration.]""")
        }

        def result = sql """
            CREATE TABLE IF NOT EXISTS dy_par ( k1 date NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL )
            AGGREGATE KEY(k1,k2)
            PARTITION BY RANGE(k1) ( )
            DISTRIBUTED BY HASH(k1) BUCKETS 3
            PROPERTIES (
                "dynamic_partition.enable"="false",
                "dynamic_partition.end"="3",
                "dynamic_partition.buckets"="10",
                "dynamic_partition.start"="-3",
                "dynamic_partition.prefix"="p",
                "dynamic_partition.time_unit"="DAY",
                "dynamic_partition.create_history_partition"="false",
                "dynamic_partition.replication_allocation" = "tag.location.default: 1")
        """
        assertEquals(0, result[0][0])
        result = sql """alter table dy_par set ("dynamic_partition.enable" = "true");"""
        assertEquals(0, result[0][0])
        
        Thread.sleep(5 * 1000)
        result = sql_return_maparray """show dynamic partition tables;"""
        logger.info("show dynamic partition tables result {}", result.LastCreatePartitionMsg[0])
        assertTrue(result.LastCreatePartitionMsg[0].contains("The current number of tablets in BE"))

        // auto partition table
        test {
            sql """
            create table auto_dynamic(
                k0 datetime(6) NOT NULL
            )
            auto partition by range (date_trunc(k0, 'hour'))
            (
            )
            DISTRIBUTED BY HASH(`k0`) BUCKETS 2
            properties(
                "dynamic_partition.enable" = "true",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.create_history_partition" = "true",
                "dynamic_partition.start" = "-5",
                "dynamic_partition.end" = "0",
                "dynamic_partition.time_unit" = "hour",
                "replication_num" = "1"
            );
            """
            exception ("""[DATA_QUALITY_ERROR]Error: The current number of tablets in BE (1000001) exceeds the allowed limit (1000000).
This may include unused tablets that were dropped from the frontend but not asynchronously dropped from the backend.
Please check your usage of mtmv (e.g., refreshing too frequently) or any misuse of auto partition.
You can resolve this issue by:
1. Increasing the value of 'be_tablet_num_upper_limit'.
2. Executing the SQL command 'ADMIN CLEAN TRASH' to clean up trash data.
3. Optionally, you can set 'trash_file_expire_time_sec' to 0 to disable trash file expiration.]""")
        }


        result = sql """
        CREATE TABLE `list_table1` (
            `str` varchar
        ) ENGINE=OLAP
        DUPLICATE KEY(`str`)
        COMMENT 'OLAP'
        AUTO PARTITION BY LIST (`str`)
        (
        )
        DISTRIBUTED BY HASH(`str`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
        assertEquals(0, result[0][0])
        test {
            sql """ insert into list_table1 values ("Beijing"), ("XXX"), ("xxx"), ("Beijing"), ("Abc"), (null) """
            exception ("""create partition failed""")
        }
        // view
        test {
            sql = """CREATE MATERIALIZED VIEW base_table_view
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by(`k3`)
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES (
                'replication_num' = '1',
                'refresh_partition_num' = '2'
                )
                AS
                SELECT * from base_table;
                """
            exception ("""[DATA_QUALITY_ERROR]Error: The current number of tablets in BE (1000001) exceeds the allowed limit (1000000).
This may include unused tablets that were dropped from the frontend but not asynchronously dropped from the backend.
Please check your usage of mtmv (e.g., refreshing too frequently) or any misuse of auto partition.
You can resolve this issue by:
1. Increasing the value of 'be_tablet_num_upper_limit'.
2. Executing the SQL command 'ADMIN CLEAN TRASH' to clean up trash data.
3. Optionally, you can set 'trash_file_expire_time_sec' to 0 to disable trash file expiration.]""")
            }

            // Increasing be_tablet_num_upper_limit
            set_be_param("be_tablet_num_upper_limit", "1000010")
            result = sql "create table varchar_0_char_0 (id int, a varchar(0), b char(0)) distributed by hash(id) properties(\"replication_num\"=\"1\")\n"
            // succ
            assertEquals(0, result[0][0])

            GetDebugPoint().disableDebugPointForAllBEs("WorkPoolCreateTablet.check_tablet_limit.change_tablet_num_in_bvar")
            result = sql """
                create table test_time_range_table_exceed_limit (
                `actorid` varchar(128),
                `gameid` varchar(128),
                `eventtime` datetimev2(3)
                )
                engine=olap
                duplicate key(actorid, gameid, eventtime)
                partition by range(eventtime)(
                    from ("2000-01-01") to ("2021-01-01") interval 1 year,
                    from ("2021-01-01") to ("2022-01-01") interval 1 MONth,
                    from ("2022-01-01") to ("2023-01-01") interval 1 WEEK,
                    from ("2023-01-01") TO ("2023-02-01") interval 1 DAY
                )
                distributed by hash(actorid) buckets 1
                properties(
                    "replication_num"="1",
                    "light_schema_change"="true",
                    "compression"="zstd"
                );
                """
                // disable debug point success
                assertEquals(0, result[0][0])
    }
}
