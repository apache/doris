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


suite("query_cache_with_rec_cte_test", "rec_cte") {

    def setSessionVariables = {
        sql "set enable_nereids_planner=true"
        sql "set enable_fallback_to_original_planner=false"
        sql "set enable_sql_cache=false"
        sql "set enable_query_cache=true"
    }

    def assertHasCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                assertTrue(profileString.contains("HitCache:  1")) && assertFalse(profileString.contains("HitCache:  0"))
            }
        }
    }

    def assertNoCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                assertTrue(profileString.contains("HitCache:  0")) && assertFalse(profileString.contains("HitCache:  1"))
            }
        }
    }

    def noQueryCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                assertFalse(profileString.contains("HitCache:  0")) && assertFalse(profileString.contains("HitCache:  1"))
            }
        }
    }

    String dbName = context.config.getDbNameByFile(context.file)
    sql "ADMIN SET FRONTEND CONFIG ('cache_last_version_interval_second' = '0')"

    combineFutures(
            // When scan+agg appears in the non-main part of a recursive section, the query cache can work
            extraThread("nonMainRecPartTest", {
                def tb_name1 = "non_main_rec_part_table1"
                def tb_name2 = "non_main_rec_part_table2"
                sql """drop table if exists ${tb_name1}"""
                sql """CREATE TABLE ${tb_name1} (
                            dept_id INT,
                            parent_id INT,
                            dept_name VARCHAR(50)
                        ) DISTRIBUTED BY HASH(dept_id) BUCKETS 1 
                        PROPERTIES ("replication_num" = "1");"""

                sql """drop table if exists ${tb_name2}"""
                sql """CREATE TABLE ${tb_name2} (
                            emp_id INT,
                            dept_id INT,
                            salary DOUBLE
                        ) DISTRIBUTED BY HASH(emp_id) BUCKETS 1 
                        PROPERTIES ("replication_num" = "1");"""
                sql """INSERT INTO ${tb_name1} VALUES (1, 0, '总部'), (2, 1, '研发部'), (3, 1, '市场部'), (4, 2, '后端组'), (5, 2, '前端组');"""
                sql """INSERT INTO ${tb_name2} VALUES (1, 1, 5000), (2, 2, 4000), (3, 3, 3500), (4, 4, 3000), (5, 5, 2500);"""

                sql "sync"

                setSessionVariables()

                def sql_str = """WITH RECURSIVE dept_hierarchy AS (
                        SELECT dept_id, parent_id, dept_name 
                        FROM ${tb_name1} WHERE dept_id = 1
                        
                        UNION ALL
                        
                        SELECT d.dept_id, d.parent_id, d.dept_name
                        FROM ${tb_name1} d
                        JOIN dept_hierarchy dh ON d.parent_id = dh.dept_id
                        JOIN (
                            SELECT dept_id, SUM(salary) as total_sal 
                            FROM ${tb_name2} 
                            GROUP BY dept_id
                        ) emp_agg ON d.dept_id = emp_agg.dept_id
                    )
                    SELECT * FROM dept_hierarchy;"""
                assertNoCache sql_str
                assertHasCache sql_str
            }),

            // scan+agg appears in the main part of the recursive section, and the query cache cannot take effect
            extraThread("mainRecPartTest", {
                def tb_name = "main_rec_part_table"
                sql """drop table if exists ${tb_name}"""
                sql """CREATE TABLE ${tb_name} (
                            id INT,
                            val INT
                        ) DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES ("replication_num" = "1");"""

                sql """INSERT INTO ${tb_name} VALUES (1, 100);"""

                sql "sync"

                setSessionVariables()

                def sql_str = """WITH RECURSIVE cte AS (
                        SELECT cast(id as int) as id, cast(val as int) as val, cast(1 as int) AS depth FROM ${tb_name}
                        UNION ALL
                        SELECT 
                            cast(depth + 1 as int),
                            CAST(avg_val AS INT),
                            cast(depth + 1 as int)
                        FROM (
                            SELECT AVG(val) as avg_val, MAX(depth) as depth
                            FROM cte
                            GROUP BY depth
                        ) t
                        WHERE depth < 5
                    )
                    SELECT * FROM cte;"""
                noQueryCache sql_str
                noQueryCache sql_str
            }),
            // scan+agg appears in the non-recursive part, and the query cache is effective
            extraThread("nonRecPartTest", {
                def tb_name1 = "non_rec_part_table1"
                def tb_name2 = "non_rec_part_table2"
                sql """drop table if exists ${tb_name1}"""
                sql """CREATE TABLE ${tb_name1} (
                            k1 INT,
                            v1 DOUBLE
                        ) 
                        DISTRIBUTED BY HASH(k1) BUCKETS 3
                        PROPERTIES ("replication_num" = "1");"""
                sql """INSERT INTO ${tb_name1} VALUES (1, 10.5), (1, 20.5), (2, 100.0), (3, 200.0);"""

                sql """drop table if exists ${tb_name2}"""
                sql """CREATE TABLE ${tb_name2} (
                            id INT,
                            parent_id INT
                        ) DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES ("replication_num" = "1");"""

                sql """INSERT INTO ${tb_name2} VALUES (1, 0), (2, 1), (3, 2), (4, 3);"""

                sql "sync"

                setSessionVariables()

                def sql_str = """WITH RECURSIVE hierarchical_sum AS (
                        SELECT 
                            m.k1 AS id, 
                            SUM(m.v1) AS total_val, 
                            cast(0 as int) AS level
                        FROM ${tb_name1} m
                        GROUP BY m.k1
                        HAVING SUM(m.v1) > 10
                        
                        UNION ALL
                        
                        SELECT 
                            r.id, 
                            hs.total_val, 
                            cast(hs.level + 1 as int)
                        FROM ${tb_name2} r
                        JOIN hierarchical_sum hs ON r.parent_id = hs.id
                        WHERE hs.level < 5
                    )
                    SELECT * FROM hierarchical_sum;"""
                assertNoCache sql_str
                assertHasCache sql_str
            }),

    ).get()

}
