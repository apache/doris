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

// ===================================================================================
// Query Cache base table type and partition hit tests
//
// Verify Query Cache hit, partial-hit, and miss behavior for different table models:
//   1. Single-column Range partitioned table
//   2. Multi-column Range partitioned table
//   3. List partitioned table
//   4. Non-partitioned table
//
// Each table model covers:
//   - The basic miss => hit path
//   - Existing partition cache remains valid after adding a partition
//   - Whether inserting new data causes partial hits or full misses
//   - Behavior after inserting null values
//
// Note: single-column Range partitions can extract and normalize partition predicate ranges;
// multi-column Range and List partitions cannot extract partition predicate ranges, but still build
// cache keys by tablet/version. Therefore, adding partitions or tablet data may still produce
// partial hits where old tablets hit and new tablets miss. Non-partitioned tables have no partition-level
// isolation, so data changes produce full misses or full hits.
// ===================================================================================

suite("query_cache_base_table_types_test") {
    // ---------- Helper methods ----------

    // Set session variables: enable Nereids and Query Cache, and disable SQL Cache
    def setSessionVariables = {
        sql "set enable_nereids_planner=true"
        sql "set enable_fallback_to_original_planner=false"
        sql "set enable_sql_cache=false"
        sql "set enable_query_cache=true"
    }

    // Assert that the query fully hits cache (HitCache=1 and no HitCache=0 in the profile)
    def assertHasCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                assertTrue(profileString.contains("HitCache:  1"))
                assertFalse(profileString.contains("HitCache:  0"))
            }
        }
    }

    // Assert that the query partially hits cache (both HitCache=1 and HitCache=0 in the profile)
    def assertPartHasCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                assertTrue(profileString.contains("HitCache:  1"))
                assertTrue(profileString.contains("HitCache:  0"))
            }
        }
    }

    // Assert that the query fully misses cache (HitCache=0 and no HitCache=1 in the profile)
    def assertNoCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                assertTrue(profileString.contains("HitCache:  0"))
                assertFalse(profileString.contains("HitCache:  1"))
            }
        }
    }

    // Assert that the query does not enter the Query Cache path (no HitCache markers in the profile)
    def noQueryCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                assertFalse(profileString.contains("HitCache:  0"))
                assertFalse(profileString.contains("HitCache:  1"))
            }
        }
    }

    String dbName = context.config.getDbNameByFile(context.file)
    withGlobalLock("cache_last_version_interval_second") {
        setFeConfigTemporary(["cache_last_version_interval_second": "0"], {

    combineFutures(
            extraThread("testRangeOneKeyTable", {
                def tb_name = "query_cache_range_one_key_table"
                sql """drop table if exists ${tb_name}"""
                sql """
                    create table ${tb_name}
                    (
                        id int,
                        value int
                    )
                    partition by range(id)
                    (
                        PARTITION p0 VALUES LESS THAN ('1'),
                        partition p1 values[('1'), ('2')),
                        partition p2 values[('2'), ('3')),
                        partition p3 values[('3'), ('4')),
                        partition p4 values[('4'), ('5')),
                        partition p5 values[('5'), ('6'))
                    )
                    distributed by hash(id)
                    properties(
                        'replication_num'='1'
                    );
                """
                sql """
                insert into ${tb_name}
                    values 
                    (1, 1), (1, 2),
                    (2, 1), (2, 2), 
                    (3, 1), (3, 2),
                    (4, 1), (4, 2),
                    (5, 1), (5, 2);
                """
                sql "sync"

                setSessionVariables()

                def sql_str = "select id, sum(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql """alter table ${tb_name} add partition p6 values[('6'),('7'))"""
                assertHasCache sql_str

                sql "insert into ${tb_name} values(6, 1)"
                assertPartHasCache sql_str
                assertHasCache sql_str

                sql "insert into ${tb_name} values(null, null)"
                assertPartHasCache sql_str
                assertHasCache sql_str
            }),
            extraThread("testRangeTwoKeyTable", {
                def tb_name = "query_cache_range_two_key_table"
                sql """drop table if exists ${tb_name}"""
                sql """
                    create table ${tb_name}
                    (
                        id int,
                        value int
                    )
                    partition by range(id, value)
                    (
                        PARTITION p0 VALUES LESS THAN ('1', '1'),
                        partition p1 values[('1', '1'), ('2', '1')),
                        partition p2 values[('2', '1'), ('3', '1')),
                        partition p3 values[('3', '1'), ('4', '1')),
                        partition p4 values[('4', '1'), ('5', '1')),
                        partition p5 values[('5', '1'), ('6', '1'))
                    )
                    distributed by hash(id)
                    properties(
                        'replication_num'='1'
                    );
                """
                sql """
                insert into ${tb_name}
                    values 
                    (1, 1), (1, 2),
                    (2, 1), (2, 2), 
                    (3, 1), (3, 2),
                    (4, 1), (4, 2),
                    (5, 1), (5, 2);
                """
                sql "sync"

                setSessionVariables()

                def sql_str = "select id, sum(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql """alter table ${tb_name} add partition p6 values[('6', '1'),('7', '1'))"""
                assertHasCache sql_str

                sql "insert into ${tb_name} values(6, 1)"
                assertPartHasCache sql_str
                assertHasCache sql_str

                sql "insert into ${tb_name} values(null, null)"
                assertPartHasCache sql_str
                assertHasCache sql_str
            }),
            extraThread("testListOneKeyTable", {
                def tb_name = "query_cache_list_one_key_table"
                sql """drop table if exists ${tb_name}"""
                sql """
                    create table ${tb_name}
                    (
                        id int,
                        value int
                    )
                    partition by list(id)
                    (
                        PARTITION p0 VALUES IN ((NULL)),
                        partition p1 VALUES IN ("1"),
                        partition p2 VALUES IN ("2"),
                        partition p3 VALUES IN ("3"),
                        partition p4 VALUES IN ("4"),
                        partition p5 VALUES IN ("5")
                    )
                    distributed by hash(id)
                    properties(
                        'replication_num'='1'
                    );
                """
                sql """
                insert into ${tb_name}
                    values 
                    (1, 1), (1, 2),
                    (2, 1), (2, 2), 
                    (3, 1), (3, 2),
                    (4, 1), (4, 2),
                    (5, 1), (5, 2);
                """
                sql "sync"

                setSessionVariables()

                def sql_str = "select id, sum(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                sql """alter table ${tb_name} add partition p6 values in ('6');"""
                assertHasCache sql_str

                sql "insert into ${tb_name} values(6, 1)"
                assertPartHasCache sql_str
                assertHasCache sql_str

                sql "insert into ${tb_name} values(null, null)"
                assertPartHasCache sql_str
                assertHasCache sql_str
            }),
            extraThread("testNonPartitionTable", {
                def tb_name = "query_cache_non_partition_table"
                sql """drop table if exists ${tb_name}"""
                sql """
                    create table ${tb_name}
                    (
                        id int,
                        value int
                    )
                    distributed by hash(id)
                    properties(
                        'replication_num'='1'
                    );
                """
                sql """
                insert into ${tb_name}
                    values 
                    (1, 1), (1, 2),
                    (2, 1), (2, 2), 
                    (3, 1), (3, 2),
                    (4, 1), (4, 2),
                    (5, 1), (5, 2);
                """
                sql "sync"

                setSessionVariables()

                def sql_str = "select id, sum(value) from ${tb_name} group by id;"
                assertNoCache sql_str
                assertHasCache sql_str

                // Non-partitioned tables cannot add partitions
                test {
                    sql """alter table ${tb_name} add partition p6 values[('6'),('7'))"""
                    exception "failed"
                }
                assertHasCache sql_str

                sql "insert into ${tb_name} values(6, 1)"
                assertNoCache sql_str
                assertHasCache sql_str

                sql "insert into ${tb_name} values(null, null)"
                assertNoCache sql_str
                assertHasCache sql_str
            }),

    ).get()

        })
    }
}
