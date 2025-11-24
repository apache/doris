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

import groovyjarjarantlr4.v4.codegen.model.ExceptionClause

import java.util.Date
import java.text.SimpleDateFormat
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpPut
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.RedirectStrategy
import org.apache.http.protocol.HttpContext
import org.apache.http.HttpRequest
import org.apache.http.impl.client.LaxRedirectStrategy
import org.apache.http.client.methods.RequestBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.util.EntityUtils
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_group_commit_schema_change", "nonConcurrent") {
    def tableName3 = "test_group_commit_schema_change"

    onFinish {
        GetDebugPoint().clearDebugPointsForAllBEs()
        GetDebugPoint().clearDebugPointsForAllFEs()
    }

    def getJobState = { tableName ->
        def jobStateResult = sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        logger.info("jobStateResult: ${jobStateResult}")
        return jobStateResult[0][9]
    }

    def getRowCount = { expectedRowCount ->
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until(
            {
                def result = sql "select count(*) from ${tableName3}"
                logger.info("table: ${tableName3}, rowCount: ${result}")
                return result[0][0] == expectedRowCount
            }
        )
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    sql """ DROP TABLE IF EXISTS ${tableName3} """
    sql """
        CREATE TABLE ${tableName3} (
            `id` int(11) NOT NULL,
            `name` varchar(50) NULL,
            `score` varchar(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "group_commit_interval_ms" = "200"
        );
    """

    GetDebugPoint().enableDebugPointForAllFEs("FE.FrontendServiceImpl.initHttpStreamPlan.block")
    GetDebugPoint().enableDebugPointForAllFEs("FE.SchemaChangeJobV2.createShadowIndexReplica.addShadowIndexToCatalog.block")

    // write data
    Thread thread = new Thread(() -> {
        sql """ set group_commit = async_mode; """
        for (int i = 0; i < 10; i++) {
            try {
                sql """ insert into ${tableName3} values (1, 'a', 100) """
                break
            } catch (Exception e) {
                logger.info("insert error: ${e}")
                if (e.getMessage().contains("schema version not match")) {
                    continue
                } else {
                    throw e
                }
            }
        }
    })
    thread.start()
    sleep(1000)
    def result = sql "select count(*) from ${tableName3}"
    logger.info("rowCount 0: ${result}")
    assertEquals(0, result[0][0])

    // schema change
    sql """ alter table ${tableName3} modify column score int NULL"""
    GetDebugPoint().enableDebugPointForAllFEs("FE.SchemaChangeJobV2.runRunning.block")
    GetDebugPoint().disableDebugPointForAllFEs("FE.SchemaChangeJobV2.createShadowIndexReplica.addShadowIndexToCatalog.block")
    for (int i = 0; i < 10; i++) {
        def job_state = getJobState(tableName3)
        if (job_state == "RUNNING") {
            break
        }
        sleep(100)
    }

    GetDebugPoint().disableDebugPointForAllFEs("FE.FrontendServiceImpl.initHttpStreamPlan.block")
    thread.join()
    getRowCount(1)
    qt_sql """ select id, name, score from ${tableName3} """
    def job_state = getJobState(tableName3)
    assertEquals("RUNNING", job_state)
    GetDebugPoint().disableDebugPointForAllFEs("FE.SchemaChangeJobV2.runRunning.block")
    for (int i = 0; i < 30; i++) {
        job_state = getJobState(tableName3)
        if (job_state == "FINISHED") {
            break
        }
        sleep(500)
    }
    assertEquals("FINISHED", job_state)
}

