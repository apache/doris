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

import java.util.concurrent.TimeUnit

suite("test_external_catalog_maxcompute_fault_tolerance", "p2,external") {
    // draft
    return
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable MaxCompute test.")
        return
    }

    String ak = context.config.otherConfigs.get("ak")
    String sk = context.config.otherConfigs.get("sk")

    String baseEndpoint = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api"
    String connectFailureEndpoint = "http://203.0.113.1:81/api"
    String queryProject = "mc_datalake"
    String writeProject = "mc_doris_test_write"

    String baseCatalog = "test_external_mc_fault_tolerance"
    String connectTimeoutCatalog = "${baseCatalog}_connect_timeout"
    String readSetupCatalog = "${baseCatalog}_read_setup"
    String readTimeoutCatalog = "${baseCatalog}_read_timeout"
    String retryZeroCatalog = "${baseCatalog}_retry_zero"
    String invalidCredentialCatalog = "${baseCatalog}_invalid_credentials"

    String largeReadDb = "mc_fault_tolerance_db"
    String largeReadTable = "mc_fault_tolerance_large_read"
    String internalDb = "mc_fault_tolerance_internal"
    String internalTable = "mc_fault_tolerance_src"

    def baseCatalogProperties = { String project, String endpoint ->
        new LinkedHashMap<String, String>([
                "type": "max_compute",
                "mc.default.project": project,
                "mc.access_key": ak,
                "mc.secret_key": sk,
                "mc.endpoint": endpoint
        ])
    }

    def renderCatalogProperties = { Map<String, String> properties ->
        properties.collect { key, value -> "\"${key}\" = \"${value}\"" }.join(",\n                ")
    }

    def createCatalog = { String catalogName, Map<String, String> properties ->
        sql """switch internal"""
        sql """drop catalog if exists ${catalogName}"""
        sql """
            create catalog if not exists ${catalogName} properties (
                ${renderCatalogProperties(properties)}
            );
        """
    }

    def switchToCatalogDb = { String catalogName, String dbName ->
        sql """switch `${catalogName}`"""
        sql """use `${dbName}`"""
    }

    def expectFailureWithin = { String caseId, long maxElapsedMs, Closure action ->
        long startMs = System.currentTimeMillis()
        Throwable failure = null
        try {
            action.call()
        } catch (Throwable t) {
            failure = t
        }
        long elapsedMs = System.currentTimeMillis() - startMs
        assertTrue(failure != null, "${caseId} should fail")
        assertTrue(elapsedMs < maxElapsedMs,
                "${caseId} should fail within ${maxElapsedMs} ms, actual ${elapsedMs} ms, message: ${failure.getMessage()}")
        [failure, elapsedMs]
    }

    def assertMessageContainsAny = { String caseId, Throwable failure, List<String> fragments ->
        String message = failure.toString()
        assertTrue(fragments.any { fragment -> message.toLowerCase().contains(fragment.toLowerCase()) },
                "${caseId} expected message containing one of ${fragments}, actual: ${message}")
    }

    // F-01: Validate the short connect timeout path with a connect-family endpoint failure.
    Map<String, String> connectTimeoutProperties = baseCatalogProperties(queryProject, connectFailureEndpoint)
    connectTimeoutProperties.put("mc.connect_timeout", "1")
    connectTimeoutProperties.put("mc.read_timeout", "1")
    connectTimeoutProperties.put("mc.retry_count", "1")
    createCatalog(connectTimeoutCatalog, connectTimeoutProperties)
    def (connectFailure, connectElapsedMs) = expectFailureWithin(
            "F-01", TimeUnit.SECONDS.toMillis(20)) {
        sql """show databases from ${connectTimeoutCatalog}"""
    }
    assertMessageContainsAny("F-01", connectFailure, [
            "timed out",
            "timeout",
            "connection refused",
            "no route to host",
            "network is unreachable",
            "endpoint"
    ])
    logger.info("F-01 failed in ${connectElapsedMs} ms with message: ${connectFailure.getMessage()}")

    // F-02: Prepare a large MaxCompute table and validate the read-timeout path on the scan chain.
    sql """CREATE DATABASE IF NOT EXISTS internal.${internalDb}"""
    sql """DROP TABLE IF EXISTS internal.${internalDb}.${internalTable}"""
    sql """
        CREATE TABLE internal.${internalDb}.${internalTable} (
            id INT,
            name STRING,
            val DOUBLE,
            ds STRING
        )
        DISTRIBUTED BY HASH(id) BUCKETS 8
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        INSERT INTO internal.${internalDb}.${internalTable}
        SELECT
            number AS id,
            concat('name_', cast(number AS STRING)) AS name,
            number * 0.01 AS val,
            concat('2025010', cast((number % 8 + 1) AS STRING)) AS ds
        FROM numbers("number"="2000000")
    """

    Map<String, String> readSetupProperties = baseCatalogProperties(writeProject, baseEndpoint)
    readSetupProperties.put("mc.quota", "pay-as-you-go")
    readSetupProperties.put("mc.enable.namespace.schema", "true")
    createCatalog(readSetupCatalog, readSetupProperties)

    sql """switch `${readSetupCatalog}`"""
    sql """drop database if exists ${largeReadDb}"""
    sql """refresh catalog ${readSetupCatalog}"""
    sql """create database ${largeReadDb}"""
    switchToCatalogDb(readSetupCatalog, largeReadDb)
    sql """drop table if exists ${largeReadTable}"""
    sql """
        CREATE TABLE ${largeReadTable} (
            id INT,
            name STRING,
            val DOUBLE,
            ds STRING
        )
    """
    sql """INSERT INTO ${largeReadTable} SELECT * FROM internal.${internalDb}.${internalTable}"""
    List<List<Object>> normalReadCount = sql """SELECT count(*) FROM ${largeReadTable}"""
    assertEquals("2000000", normalReadCount[0][0].toString())

    Map<String, String> readTimeoutProperties = baseCatalogProperties(writeProject, baseEndpoint)
    readTimeoutProperties.put("mc.quota", "pay-as-you-go")
    readTimeoutProperties.put("mc.enable.namespace.schema", "true")
    readTimeoutProperties.put("mc.read_timeout", "1")
    readTimeoutProperties.put("mc.retry_count", "1")
    createCatalog(readTimeoutCatalog, readTimeoutProperties)
    def (readFailure, readElapsedMs) = expectFailureWithin(
            "F-02", TimeUnit.SECONDS.toMillis(30)) {
        switchToCatalogDb(readTimeoutCatalog, largeReadDb)
        sql """select count(*) from ${largeReadTable}"""
    }
    assertMessageContainsAny("F-02", readFailure, [
            "timed out",
            "timeout",
            "read timed out",
            "sockettimeoutexception"
    ])
    logger.info("F-02 failed in ${readElapsedMs} ms with message: ${readFailure.getMessage()}")

    // F-03: The current implementation rejects retry_count=0 at DDL validation time.
    test {
        sql """switch internal"""
        sql """drop catalog if exists ${retryZeroCatalog}"""
        sql """
            create catalog if not exists ${retryZeroCatalog} properties (
                "type" = "max_compute",
                "mc.default.project" = "${queryProject}",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.endpoint" = "${baseEndpoint}",
                "mc.retry_count" = "0"
            );
        """
        exception "mc.retry_count must be greater than 0"
    }

    // F-04: Cover the authentication failure path with invalid AK/SK credentials.
    Map<String, String> invalidCredentialProperties = baseCatalogProperties(queryProject, baseEndpoint)
    invalidCredentialProperties.put("mc.access_key", "invalid_ak_for_regression")
    invalidCredentialProperties.put("mc.secret_key", "invalid_sk_for_regression")
    createCatalog(invalidCredentialCatalog, invalidCredentialProperties)
    def (authFailure, authElapsedMs) = expectFailureWithin(
            "F-04", TimeUnit.SECONDS.toMillis(20)) {
        sql """show databases from ${invalidCredentialCatalog}"""
    }
    assertMessageContainsAny("F-04", authFailure, [
            "invalid credentials",
            "odps-0410051",
            "accesskeyid not found",
            "authentication"
    ])
    logger.info("F-04 failed in ${authElapsedMs} ms with message: ${authFailure.getMessage()}")

    // F-05: Validate multi-session concurrent reads on the same MaxCompute catalog.
    createCatalog(baseCatalog, baseCatalogProperties(queryProject, baseEndpoint))
    List<List<String>> concurrentResults = Collections.synchronizedList(new ArrayList<List<String>>())
    List<String> concurrentErrors = Collections.synchronizedList(new ArrayList<String>())
    List<Thread> queryThreads = []
    int sessionCount = 4
    (1..sessionCount).each { sessionId ->
        queryThreads.add(Thread.start {
            try {
                List<String> sessionResult = connect(context.config.jdbcUser,
                        context.config.jdbcPassword, context.config.jdbcUrl) {
                    sql """switch `${baseCatalog}`"""
                    sql """use `${queryProject}`"""
                    List<List<Object>> webSiteCount = sql """select count(*) from web_site"""
                    List<List<Object>> partitionCount =
                            sql """select count(*) from mc_parts where dt >= '2023-08-03'"""
                    [
                            webSiteCount[0][0].toString(),
                            partitionCount[0][0].toString()
                    ]
                }
                concurrentResults.add(sessionResult)
            } catch (Throwable t) {
                concurrentErrors.add("session ${sessionId}: ${t.getMessage()}")
            }
        })
    }
    queryThreads.each { thread -> thread.join() }
    assertTrue(concurrentErrors.isEmpty(), "F-05 session errors: ${concurrentErrors}")
    assertEquals(sessionCount, concurrentResults.size())
    concurrentResults.each { result ->
        assertEquals(["5", "3"], result)
    }

    sql """switch internal"""
}
