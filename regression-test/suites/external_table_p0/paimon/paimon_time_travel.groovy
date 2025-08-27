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

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.ZoneId



suite("paimon_time_travel", "p0,external,doris,external_docker,external_docker_doris") {
    logger.info("start paimon test")
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }
    // Create date time formatter

    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String catalog_name = "test_paimon_time_travel_catalog"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    DateTimeFormatter iso_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
    DateTimeFormatter standard_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    String db_name = "test_paimon_time_travel_db"
    String tableName = "tbl_time_travel"
    try {
        sql """drop catalog if exists ${catalog_name}"""

        sql """
                CREATE CATALOG ${catalog_name} PROPERTIES (
                        'type' = 'paimon',
                        'warehouse' = 's3://warehouse/wh',
                        's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
                        's3.access_key' = 'admin',
                        's3.secret_key' = 'password',
                        's3.path.style.access' = 'true'
                );
            """
        logger.info("catalog " + catalog_name + " created")
        sql """switch `${catalog_name}`"""
        logger.info("switched to catalog " + catalog_name)
        sql """use ${db_name}"""
        //system table snapshots to get create time.
        List<List<Object>> snapshotRes = sql """ select snapshot_id,commit_time from ${tableName}\$snapshots order by snapshot_id;"""
        logger.info("Query result from ${tableName}\$snapshots: ${snapshotRes}")
        assertTrue(snapshotRes.size()==4)
        assertTrue(snapshotRes[0].size()==2)

        snapshotRes.eachWithIndex { snapshotRow, index ->
            int snapshotId = snapshotRow[0] as int
            String commitTime = snapshotRow[1] as String
            String tagName = "t_${snapshotId}"

            logger.info("Processing snapshot ${index + 1}: ID=${snapshotId}, commit_time=${commitTime}")

            try {
                LocalDateTime dateTime;
                if (commitTime.contains("T")){
                    dateTime = LocalDateTime.parse(commitTime, iso_formatter)
                }else {
                    dateTime = LocalDateTime.parse(commitTime, standard_formatter)
                }

                String snapshotTime = dateTime.atZone(ZoneId.systemDefault()).format(standard_formatter);
                long timestamp = dateTime.atZone(ZoneId.systemDefault())
                        .toInstant()
                        .toEpochMilli()

                // Execute various types of time travel queries
                String baseQueryName = "qt_time_travel_snapshot_${index + 1}"

                // 1. Time travel by snapshot ID
                "${baseQueryName}_version_count" """select count(*) from ${tableName} FOR VERSION AS OF ${snapshotId} ;"""
                "${baseQueryName}_version" """select * from ${tableName} FOR VERSION AS OF ${snapshotId} order by order_id;"""
                "${baseQueryName}_version_select_columns" """select is_paid,total_amount,order_id,customer_id from ${tableName} FOR VERSION AS OF ${snapshotId} order by order_id;"""
                "${baseQueryName}_version_filter_order_status" """select * from ${tableName} FOR VERSION AS OF ${snapshotId} where order_status = 'COMPLETED' order by order_id;"""
                "${baseQueryName}_version_agg" """select 
                                                    cast(MIN(total_amount) as decimal(9,3)) AS min_price,
                                                    cast(MAX(total_amount) as decimal(9,3)) AS max_price,
                                                    cast(AVG(total_amount) as decimal(9,3)) AS avg_price,
                                                    COUNT(*) AS total_count,
                                                    cast(SUM(total_amount) as decimal(9,3)) AS total_price from ${tableName} FOR VERSION AS OF ${snapshotId} ;"""
                "${baseQueryName}_version_group_by_is_paid" """select is_paid, count(*) as cnt from ${tableName} FOR VERSION AS OF ${snapshotId} GROUP BY is_paid order by cnt,is_paid;"""



                // 2. Time travel by tag
                "${baseQueryName}_tag_count" """select count(*) from ${tableName} FOR VERSION AS OF '${tagName}';"""
                "${baseQueryName}_tag" """select * from ${tableName} FOR VERSION AS OF '${tagName}' order by order_id;"""
                "${baseQueryName}_tag_select_columns" """select is_paid,total_amount,order_id,customer_id from ${tableName} FOR VERSION AS OF '${tagName}' order by order_id;"""
                "${baseQueryName}_tag_filter_order_status" """select * from ${tableName} FOR VERSION AS OF '${tagName}' where order_status = 'COMPLETED' order by order_id;"""
                "${baseQueryName}_tag_agg" """select 
                                       cast(MIN(total_amount) as decimal(9,3)) AS min_price,
                                       cast(MAX(total_amount) as decimal(9,3)) AS max_price,
                                       cast(AVG(total_amount) as decimal(9,3)) AS avg_price,
                                       COUNT(*) AS total_count,
                                       cast(SUM(total_amount) as decimal(9,3)) AS total_price from ${tableName} FOR VERSION AS OF '${tagName}' ;"""
                "${baseQueryName}_tag_group_by_is_paid" """select is_paid, count(*) as cnt from ${tableName} FOR VERSION AS OF '${tagName}' GROUP BY is_paid order by cnt,is_paid;"""

                // 3. Time travel by time string
                "${baseQueryName}_time_string_count" """select count(*) from ${tableName} FOR TIME AS OF \"${snapshotTime}\" ;"""
                "${baseQueryName}_time_string" """select * from ${tableName} FOR TIME AS OF \"${snapshotTime}\" order by order_id"""
                "${baseQueryName}_time_string_select_columns" """select is_paid,total_amount,order_id,customer_id from ${tableName} FOR TIME AS OF \"${snapshotTime}\" order by order_id"""
                "${baseQueryName}_time_string_filter_order_status" """select * from ${tableName} FOR TIME AS OF \"${snapshotTime}\" where order_status = 'COMPLETED' order by order_id;"""
                "${baseQueryName}_time_string_agg" """select 
                                              cast(MIN(total_amount) as decimal(9,3)) AS min_price,
                                              cast(MAX(total_amount) as decimal(9,3)) AS max_price,
                                              cast(AVG(total_amount) as decimal(9,3)) AS avg_price,
                                              COUNT(*) AS total_count,
                                              cast(SUM(total_amount) as decimal(9,3)) AS total_price from ${tableName} FOR TIME AS OF \"${snapshotTime}\" ;"""
                "${baseQueryName}_time_string_group_by_is_paid" """select is_paid, count(*) as cnt from ${tableName} FOR TIME AS OF \"${snapshotTime}\" GROUP BY is_paid order by cnt,is_paid;"""

                // 4. Time travel by millisecond timestamp
                "${baseQueryName}_time_millis_count" """select count(*) from ${tableName} FOR TIME AS OF ${timestamp};"""
                "${baseQueryName}_time_millis" """select * from ${tableName} FOR TIME AS OF ${timestamp} order by order_id"""
                "${baseQueryName}_time_millis_select_columns" """select is_paid,total_amount,order_id,customer_id from ${tableName} FOR TIME AS OF ${timestamp} order by order_id"""
                "${baseQueryName}_time_millis_filter_order_status" """select * from ${tableName} FOR TIME AS OF ${timestamp} where order_status = 'COMPLETED' order by order_id;"""
                "${baseQueryName}_time_millis_agg" """select 
                                             cast(MIN(total_amount) as decimal(9,3)) AS min_price,
                                             cast(MAX(total_amount) as decimal(9,3)) AS max_price,
                                             cast(AVG(total_amount) as decimal(9,3)) AS avg_price,
                                             COUNT(*) AS total_count,
                                             cast(SUM(total_amount) as decimal(9,3)) AS total_price from ${tableName} FOR TIME AS OF ${timestamp} ;"""
                "${baseQueryName}_time_millis_group_by_is_paid" """select is_paid, count(*) as cnt from ${tableName} FOR TIME AS OF ${timestamp} GROUP BY is_paid order by cnt,is_paid;"""


                logger.info("Completed queries for snapshot ${snapshotId} with timestamp ${timestamp}")

            } catch (Exception e) {
                logger.error("Failed to process snapshot ${snapshotId}: ${e.message}")
                throw e
            }
        }

        List<List<Object>> branchesResult = sql """ select branch_name from ${tableName}\$branches order by branch_name;"""
        logger.info("Query result from ${tableName}\$branches: ${branchesResult}")
        assertTrue(branchesResult.size()==2)
        assertTrue(branchesResult[0].size()==1)

        branchesResult.eachWithIndex { branchRow, index ->
            String branchName = branchRow[0] as String
            logger.info("Processing branch ${index + 1}: ${branchName}")
            String baseQueryName = "qt_branch_${index + 1}"

            try {
                "${baseQueryName}_count_list" """select count(*) from ${tableName}@branch(${branchName});"""
                "${baseQueryName}_count_map" """select count(*) from ${tableName}@branch(\"name\"="${branchName}");"""

                "${baseQueryName}_count_list" """select * from ${tableName}@branch(${branchName}) order by order_id;"""
                "${baseQueryName}_count_map" """select * from ${tableName}@branch(\"name\"="${branchName}") order by order_id;"""

                "${baseQueryName}_select_columns_list" """select customer_id,is_paid,order_status from ${tableName}@branch(${branchName}) order by order_id;"""
                "${baseQueryName}_select_columns_map" """select customer_id,is_paid,order_status from ${tableName}@branch(\"name\"="${branchName}") order by order_id;"""

                "${baseQueryName}_filter_amount_list" """select * from ${tableName}@branch(${branchName}) where total_amount > 100.00 order by order_id;"""
                "${baseQueryName}_filter_amount_map" """select * from ${tableName}@branch(\"name\"="${branchName}") where total_amount > 100.00 order by order_id;"""

                "${baseQueryName}_agg_list" """select 
                                                    cast(MIN(total_amount) as decimal(9,3)) AS min_price,
                                                    cast(MAX(total_amount) as decimal(9,3)) AS max_price,
                                                    cast(AVG(total_amount) as decimal(9,3)) AS avg_price,
                                                    COUNT(*) AS total_count,
                                                    cast(SUM(total_amount) as decimal(9,3)) AS total_price from  ${tableName}@branch(${branchName}) ;"""
                "${baseQueryName}_agg_map" """select 
                                                    cast(MIN(total_amount) as decimal(9,3)) AS min_price,
                                                    cast(MAX(total_amount) as decimal(9,3)) AS max_price,
                                                    cast(AVG(total_amount) as decimal(9,3)) AS avg_price,
                                                    COUNT(*) AS total_count,
                                                    cast(SUM(total_amount) as decimal(9,3)) AS total_price from ${tableName}@branch(\"name\"="${branchName}") ;"""
                "${baseQueryName}_group_by_is_paid_list" """select is_paid, count(*) as cnt from ${tableName}@branch(${branchName}) GROUP BY is_paid order by cnt,is_paid;"""
                "${baseQueryName}_group_by_is_paid_map" """select is_paid, count(*) as cnt from ${tableName}@branch(\"name\"="${branchName}") GROUP BY is_paid order by cnt,is_paid;"""
                logger.info("Completed queries for branch: ${branchName}")

            } catch (Exception e) {
                logger.error("Failed to process branch ${branchName}: ${e.message}")
                throw e
            }
        }


        List<List<Object>> tagsResult = sql """ select snapshot_id,tag_name from ${tableName}\$tags order by snapshot_id;"""
        logger.info("Query result from ${tableName}\$tags: ${tagsResult}")
        assertTrue(tagsResult.size()==4)
        assertTrue(tagsResult[0].size()==2)

        tagsResult.eachWithIndex { tagRow, index ->
            String snapshotId = tagRow[0] as String
            String tagName = tagRow[1] as String
            logger.info("Processing tag ${index + 1}: ${tagName} (snapshot: ${snapshotId})")
            String baseQueryName = "qt_tag_${index + 1}"

            try {
                "${baseQueryName}_count_list" """select count(*) from ${tableName}@tag(${tagName});"""
                "${baseQueryName}_count_map" """select count(*) from ${tableName}@tag(\"name\"="${tagName}");"""

                "${baseQueryName}_select_all_list" """select * from ${tableName}@tag(${tagName}) order by order_id;"""
                "${baseQueryName}_select_all_map" """select * from ${tableName}@tag(\"name\"="${tagName}") order by order_id;"""

                "${baseQueryName}_select_columns_list" """select customer_id,is_paid,order_status from ${tableName}@tag(${tagName}) order by order_id;"""
                "${baseQueryName}_select_columns_map" """select customer_id,is_paid,order_status from ${tableName}@tag(\"name\"="${tagName}") order by order_id;"""

                "${baseQueryName}_filter_amount_list" """select * from ${tableName}@tag(${tagName}) where total_amount > 100.00 order by order_id;"""
                "${baseQueryName}_filter_amount_map" """select * from ${tableName}@tag(\"name\"="${tagName}") where total_amount > 100.00 order by order_id;"""

                "${baseQueryName}_agg_list" """select 
                                        cast(MIN(total_amount) as decimal(9,3)) AS min_price,
                                        cast(MAX(total_amount) as decimal(9,3)) AS max_price,
                                        cast(AVG(total_amount) as decimal(9,3)) AS avg_price,
                                        COUNT(*) AS total_count,
                                        cast(SUM(total_amount) as decimal(9,3)) AS total_price from ${tableName}@tag(${tagName});"""
                "${baseQueryName}_agg_map" """select 
                                        cast(MIN(total_amount) as decimal(9,3)) AS min_price,
                                        cast(MAX(total_amount) as decimal(9,3)) AS max_price,
                                        cast(AVG(total_amount) as decimal(9,3)) AS avg_price,
                                        COUNT(*) AS total_count,
                                        cast(SUM(total_amount) as decimal(9,3)) AS total_price from ${tableName}@tag(\"name\"="${tagName}");"""
                "${baseQueryName}_group_by_is_paid_list" """select is_paid, count(*) as cnt from ${tableName}@tag(${tagName}) GROUP BY is_paid order by cnt,is_paid;"""
                "${baseQueryName}_group_by_is_paid_map" """select is_paid, count(*) as cnt from ${tableName}@tag(\"name\"="${tagName}") GROUP BY is_paid order by cnt,is_paid;"""

                logger.info("Completed queries for tag: ${tagName} (snapshot: ${snapshotId})")

            } catch (Exception e) {
                logger.error("Failed to process tag ${tagName}: ${e.message}")
                throw e
            }
        }

        test {
            sql """ select * from ${tableName}@branch('name'='not_exists_branch'); """
            exception "Branch 'not_exists_branch' does not exist"
        }
        test {
            sql """ select * from ${tableName}@branch(not_exists_branch); """
            exception "Branch 'not_exists_branch' does not exist"
        }
        test {
            sql """ select * from ${tableName}@tag('name'='not_exists_tag'); """
            exception "Tag 'not_exists_tag' does not exist"
        }
        test {
            sql """ select * from ${tableName}@tag(not_exists_tag); """
            exception "Tag 'not_exists_tag' does not exist"
        }
        test {
            sql """ select * from ${tableName} for version as of 'not_exists_tag'; """
            exception "Tag 'not_exists_tag' does not exist"
        }

        // Use branch function to query tags
        test {
            sql """ select * from ${tableName}@tag('na'='not_exists_tag'); """
            exception "must contain key 'name' in params"
        }
        test {
            sql """ select * from ${tableName}@branch('nme'='not_exists_branch'); """
            exception "must contain key 'name' in params"
        }

        test {
            sql """ select * from ${tableName}@brand('nme'='not_exists_branch'); """
            exception "Invalid param type: brand"
        }

    } finally {
         // sql """drop catalog if exists ${catalog_name}"""
    }
}


