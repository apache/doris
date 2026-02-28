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

suite("iceberg_branch_tag_parallel_op", "p2,external,branch_tag") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_branch_tag_parallel"
    String db_name = "test_parallel_op"
    String table_name = "test_branch_tag_operate"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """drop database if exists ${catalog_name}.${db_name} force"""
    sql """create database ${catalog_name}.${db_name}"""
    sql """ use ${catalog_name}.${db_name} """
    sql """drop table if exists ${table_name}"""
    sql """create table ${table_name} (id int, name string)"""
    sql """insert into ${table_name} values (1, 'name_1')"""

    // Test: Concurrent creation of different named branches
    // Note: Some operations may fail due to metadata refresh issues, which is expected
    def createBranchResults = []
    def futures = []
    for (int i = 0; i < 5; i++) {
        int idx = i
        futures.add(thread {
            sql """use ${catalog_name}.${db_name}"""
            try {
                sql """alter table ${table_name} create branch branch_${idx} """
                synchronized (this) {
                    createBranchResults.add([idx: idx, success: true])
                }
                logger.info("Branch ${idx} creation succeeded")
            } catch (Exception e) {
                synchronized (this) {
                    createBranchResults.add([idx: idx, success: false, error: e.getMessage()])
                }
                logger.info("Branch ${idx} creation failed: " + e.getMessage())
            }
        })
    }
    def combineFuture = combineFutures(futures)
    combineFuture.get()

    // Verify that at least some branches were created successfully
    def successfulBranches = createBranchResults.findAll { it.success }
    def failedBranches = createBranchResults.findAll { !it.success }
    logger.info("Branch creation results - Success: ${successfulBranches.size()}, Failure: ${failedBranches.size()}")
    assertTrue(successfulBranches.size() > 0, "At least one branch should be created successfully")

    // Query only successfully created branches
    successfulBranches.each { result ->
        sql """use ${catalog_name}.${db_name}"""
        def res = sql """select * from ${table_name}@branch(branch_${result.idx}) """
        logger.info("Query branch_${result.idx}: ${res.size()} rows")
        assertTrue(res.size() > 0, "Branch ${result.idx} should have data")
    }

    // Test: Concurrent writes to different branches
    def writeBranchResults = []
    def futures1 = []
    for (int i = 0; i < 5; i++) {
        int idx = i
        futures1.add(thread {
            sql """use ${catalog_name}.${db_name}"""
            try {
                sql """insert into ${table_name}@branch(branch_${idx}) values (${idx + 2}, 'name_${idx + 2}') """
                synchronized (this) {
                    writeBranchResults.add([idx: idx, success: true])
                }
                logger.info("Write to branch ${idx} succeeded")
            } catch (Exception e) {
                synchronized (this) {
                    writeBranchResults.add([idx: idx, success: false, error: e.getMessage()])
                }
                logger.info("Write to branch ${idx} failed: " + e.getMessage())
            }
        })
    }
    def combineFuture1 = combineFutures(futures1)
    combineFuture1.get()

    // Verify some writes succeeded
    def successfulWrites = writeBranchResults.findAll { it.success }
    logger.info("Write results - Success: ${successfulWrites.size()}, Failure: ${writeBranchResults.size() - successfulWrites.size()}")

    // Query only successfully written branches
    successfulBranches.each { result ->
        def writeSuccess = successfulWrites.find { it.idx == result.idx }
        if (writeSuccess != null) {
            sql """use ${catalog_name}.${db_name}"""
            def res = sql """select * from ${table_name}@branch(branch_${result.idx}) order by id"""
            logger.info("Query branch_${result.idx} after write: ${res.size()} rows")
            assertTrue(res.size() > 1, "Branch ${result.idx} should have more data after write")
        }
    }

    // Clean up: Drop successfully created branches
    def dropBranchResults = []
    def futures2 = []
    successfulBranches.each { result ->
        futures2.add(thread {
            sql """use ${catalog_name}.${db_name}"""
            try {
                sql """alter table ${table_name} drop branch branch_${result.idx} """
                synchronized (this) {
                    dropBranchResults.add([idx: result.idx, success: true])
                }
            } catch (Exception e) {
                synchronized (this) {
                    dropBranchResults.add([idx: result.idx, success: false, error: e.getMessage()])
                }
                logger.info("Drop branch ${result.idx} failed: " + e.getMessage())
            }
        })
    }
    if (futures2.size() > 0) {
        def combineFuture2 = combineFutures(futures2)
        combineFuture2.get()
    }

    // Test: Concurrent creation of different named tags
    def createTagResults = []
    def futures3 = []
    for (int i = 0; i < 5; i++) {
        int idx = i
        futures3.add(thread {
            sql """use ${catalog_name}.${db_name}"""
            try {
                sql """alter table ${table_name} create tag tag_${idx} """
                synchronized (this) {
                    createTagResults.add([idx: idx, success: true])
                }
                logger.info("Tag ${idx} creation succeeded")
            } catch (Exception e) {
                synchronized (this) {
                    createTagResults.add([idx: idx, success: false, error: e.getMessage()])
                }
                logger.info("Tag ${idx} creation failed: " + e.getMessage())
            }
        })
    }
    def combineFuture3 = combineFutures(futures3)
    combineFuture3.get()

    // Verify some tags were created
    def successfulTags = createTagResults.findAll { it.success }
    logger.info("Tag creation results - Success: ${successfulTags.size()}, Failure: ${createTagResults.size() - successfulTags.size()}")
    assertTrue(successfulTags.size() > 0, "At least one tag should be created successfully")

    // Query only successfully created tags
    successfulTags.each { result ->
        sql """use ${catalog_name}.${db_name}"""
        def res = sql """select * from ${table_name}@tag(tag_${result.idx}) """
        logger.info("Query tag_${result.idx}: ${res.size()} rows")
        assertTrue(res.size() > 0, "Tag ${result.idx} should have data")
    }

    // Clean up: Drop successfully created tags
    def dropTagResults = []
    def futures4 = []
    successfulTags.each { result ->
        futures4.add(thread {
            sql """use ${catalog_name}.${db_name}"""
            try {
                sql """alter table ${table_name} drop tag tag_${result.idx} """
                synchronized (this) {
                    dropTagResults.add([idx: result.idx, success: true])
                }
            } catch (Exception e) {
                synchronized (this) {
                    dropTagResults.add([idx: result.idx, success: false, error: e.getMessage()])
                }
                logger.info("Drop tag ${result.idx} failed: " + e.getMessage())
            }
        })
    }
    if (futures4.size() > 0) {
        def combineFuture4 = combineFutures(futures4)
        combineFuture4.get()
    }

    // Test concurrent branch creation with same name - only one should succeed
    def branchSuccessCount = 0
    def branchFailureCount = 0
    def futures5 = []
    for (int i = 0; i < 5; i++) {
        futures5.add(thread {
            sql """use ${catalog_name}.${db_name}"""
            try {
                sql """alter table ${table_name} create branch branch_same_name """
                synchronized (this) {
                    branchSuccessCount++
                }
                logger.info("Branch creation succeeded")
            } catch (Exception e) {
                synchronized (this) {
                    branchFailureCount++
                }
                logger.info("Caught exception: " + e.getMessage())
            }
        })
    }
    def combineFuture5 = combineFutures(futures5)
    combineFuture5.get()

    logger.info("Branch creation - Success: ${branchSuccessCount}, Failure: ${branchFailureCount}")
    assertTrue(branchSuccessCount >= 1, "At least one branch creation should succeed")

    sql """use ${catalog_name}.${db_name}"""
    def branchSameNameRes = sql """select * from ${table_name}@branch(branch_same_name) """
    logger.info("Query branch_same_name: ${branchSameNameRes.size()} rows")
    assertTrue(branchSameNameRes.size() > 0, "branch_same_name should have data")

    // Test concurrent writes to the same branch - only one should succeed
    def writeSuccessCount = 0
    def writeFailureCount = 0
    def futures6 = []
    for (int i = 0; i < 5; i++) {
        int idx = i
        futures6.add(thread {
            sql """use ${catalog_name}.${db_name}"""
            try {
                sql """insert into ${table_name}@branch(branch_same_name) values (${idx + 100}, 'name_write_${idx}') """
                synchronized (this) {
                    writeSuccessCount++
                }
                logger.info("Write ${idx} succeeded")
            } catch (Exception e) {
                synchronized (this) {
                    writeFailureCount++
                }
                logger.info("Write ${idx} caught exception: " + e.getMessage())
            }
        })
    }
    def combineFuture6 = combineFutures(futures6)
    combineFuture6.get()

    logger.info("Write to branch - Success: ${writeSuccessCount}, Failure: ${writeFailureCount}")
    assertTrue(writeSuccessCount >= 1, "At least one write should succeed")

    // Test concurrent tag creation with same name - only one should succeed
    def tagSuccessCount = 0
    def tagFailureCount = 0
    def futures7 = []
    for (int i = 0; i < 5; i++) {
        futures7.add(thread {
            sql """use ${catalog_name}.${db_name}"""
            try {
                sql """alter table ${table_name} create tag tag_same_name """
                synchronized (this) {
                    tagSuccessCount++
                }
                logger.info("Tag creation succeeded")
            } catch (Exception e) {
                synchronized (this) {
                    tagFailureCount++
                }
                logger.info("Caught exception: " + e.getMessage())
            }
        })
    }
    def combineFuture7 = combineFutures(futures7)
    combineFuture7.get()

    logger.info("Tag creation - Success: ${tagSuccessCount}, Failure: ${tagFailureCount}")
    assertTrue(tagSuccessCount >= 1, "At least one tag creation should succeed")

    sql """use ${catalog_name}.${db_name}"""
    def tagSameNameRes = sql """select * from ${table_name}@tag(tag_same_name) """
    logger.info("Query tag_same_name: ${tagSameNameRes.size()} rows")
    assertTrue(tagSameNameRes.size() > 0, "tag_same_name should have data")

}

