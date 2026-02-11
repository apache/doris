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

import org.codehaus.groovy.runtime.dgmimpl.arrays.LongArrayGetAtMetaMethod

import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;

final String ERROR_SQL_SUCCEED_MSG = "SQL should have failed but succeeded"
final String SET_SESSION_VARIABLE_FAILED_MSG = "SQL set session variable failed"

suite("test_file_cache_query_limit_config", "external_docker,hive,external_docker_hive,p0,external,nonConcurrent") {

    sql """set file_cache_query_limit_percent = 1"""
    def fileCacheQueryLimitPercentResult = sql """show variables like 'file_cache_query_limit_percent';"""
    logger.info("file_cache_query_limit_percent configuration: " + fileCacheQueryLimitPercentResult)
    assertFalse(fileCacheQueryLimitPercentResult.size() == 0 || Double.valueOf(fileCacheQueryLimitPercentResult[0][1]) != 1.0,
            SET_SESSION_VARIABLE_FAILED_MSG)

    sql """set file_cache_query_limit_percent = 20"""
    fileCacheQueryLimitPercentResult = sql """show variables like 'file_cache_query_limit_percent';"""
    logger.info("file_cache_query_limit_percent configuration: " + fileCacheQueryLimitPercentResult)
    assertFalse(fileCacheQueryLimitPercentResult.size() == 0 || Double.valueOf(fileCacheQueryLimitPercentResult[0][1]) != 20.0,
            SET_SESSION_VARIABLE_FAILED_MSG)

    sql """set file_cache_query_limit_percent = 51"""
    fileCacheQueryLimitPercentResult = sql """show variables like 'file_cache_query_limit_percent';"""
    logger.info("file_cache_query_limit_percent configuration: " + fileCacheQueryLimitPercentResult)
    assertFalse(fileCacheQueryLimitPercentResult.size() == 0 || Double.valueOf(fileCacheQueryLimitPercentResult[0][1]) != 51.0,
            SET_SESSION_VARIABLE_FAILED_MSG)


    sql """set file_cache_query_limit_percent = 100"""
    fileCacheQueryLimitPercentResult = sql """show variables like 'file_cache_query_limit_percent';"""
    logger.info("file_cache_query_limit_percent configuration: " + fileCacheQueryLimitPercentResult)
    assertFalse(fileCacheQueryLimitPercentResult.size() == 0 || Double.valueOf(fileCacheQueryLimitPercentResult[0][1]) != 100.0,
            SET_SESSION_VARIABLE_FAILED_MSG)

    try {
        sql """set file_cache_query_limit_percent = -1"""
        assertTrue(false, ERROR_SQL_SUCCEED_MSG)
    } catch (Exception e) {
        logger.info("SQL failed as expected: ${e.message}")
    }

    try {
        sql """set file_cache_query_limit_percent = 0"""
        assertTrue(false, ERROR_SQL_SUCCEED_MSG)
    } catch (Exception e) {
        logger.info("SQL failed as expected: ${e.message}")
    }

    try {
        sql """set file_cache_query_limit_percent = 101"""
        assertTrue(false, ERROR_SQL_SUCCEED_MSG)
    } catch (Exception e) {
        logger.info("SQL failed as expected: ${e.message}")
    }

    try {
        sql """set file_cache_query_limit_percent = 1000000"""
        assertTrue(false, ERROR_SQL_SUCCEED_MSG)
    } catch (Exception e) {
        logger.info("SQL failed as expected: ${e.message}")
    }

    // Set frontend configuration parameters for file_cache_query_limit_max_percent
    setFeConfigTemporary([
            "file_cache_query_limit_max_percent": "50"
    ]) {
        // Execute test logic with modified configuration for file_cache_query_limit_max_percent
        logger.info("Backend configuration set - file_cache_query_limit_max_percent: 50")

        sql """set file_cache_query_limit_percent = 1"""
        fileCacheQueryLimitPercentResult = sql """show variables like 'file_cache_query_limit_percent';"""
        logger.info("file_cache_query_limit_percent configuration: " + fileCacheQueryLimitPercentResult)
        assertFalse(fileCacheQueryLimitPercentResult.size() == 0 || Double.valueOf(fileCacheQueryLimitPercentResult[0][1]) != 1.0,
                SET_SESSION_VARIABLE_FAILED_MSG)

        sql """set file_cache_query_limit_percent = 20"""
        fileCacheQueryLimitPercentResult = sql """show variables like 'file_cache_query_limit_percent';"""
        logger.info("file_cache_query_limit_percent configuration: " + fileCacheQueryLimitPercentResult)
        assertFalse(fileCacheQueryLimitPercentResult.size() == 0 || Double.valueOf(fileCacheQueryLimitPercentResult[0][1]) != 20.0,
                SET_SESSION_VARIABLE_FAILED_MSG)

        try {
            sql """set file_cache_query_limit_percent = -1"""
            assertTrue(false, ERROR_SQL_SUCCEED_MSG)
        } catch (Exception e) {
            logger.info("SQL failed as expected: ${e.message}")
        }

        try {
            sql """set file_cache_query_limit_percent = 0"""
            assertTrue(false, ERROR_SQL_SUCCEED_MSG)
        } catch (Exception e) {
            logger.info("SQL failed as expected: ${e.message}")
        }

        try {
            sql """set file_cache_query_limit_percent = 51"""
            assertTrue(false, ERROR_SQL_SUCCEED_MSG)
        } catch (Exception e) {
            logger.info("SQL failed as expected: ${e.message}")
        }
    }

    return true;
}

