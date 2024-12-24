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

suite("test_enable_light_schema_change", "p0") {
    def tableName1 = "test_enable_lsc"
    def tableName2 = "test_enable_lsc_normal"

    def getJobState = { tableName ->
        def jobStateResult = sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return jobStateResult[0][9]
    }

    def getCreateViewState = { tableName ->
        def createViewStateResult = sql """ SHOW ALTER TABLE MATERIALIZED VIEW WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return createViewStateResult[0][8]
    }

    sql """ DROP TABLE IF EXISTS ${tableName1} """

    sql """
        CREATE TABLE ${tableName1}
        (
            k1 DATE,
            k2 int NOT NULL DEFAULT "1",
            k3 CHAR(10) COMMENT "string column",
            k4 INT NOT NULL DEFAULT "1" COMMENT "int column"
        )
        unique KEY(k1, k2)
        COMMENT "my first table"
        PARTITION BY RANGE(k1)
        (
            PARTITION p1 VALUES LESS THAN ("2020-02-01"),
            PARTITION p2 VALUES LESS THAN ("2020-03-01"),
            PARTITION p3 VALUES LESS THAN ("2020-04-01")
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "false"
        );
    """

    sql """ insert into ${tableName1} values ('2020-02-10', 2, 'a', 4) """

    sql """ alter table ${tableName1} order by (k1, k2, k4, k3) """

    def max_try_num = 60
    while (max_try_num--) {
        String res = getJobState(tableName1)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            sleep(1000)
            if (max_try_num < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }

    sql """ alter table ${tableName1} ADD PARTITION p4 VALUES LESS THAN ("2020-05-01") """
    sql """ insert into ${tableName1} values ('2020-04-10', 2, 5, 'b') """

    test {
        sql """ alter table ${tableName1} set ("light_schema_change"="true") """
        exception "errCode = 2, detailMessage = failed to enable light schema change for table"
    }

    sql """ select * from ${tableName1} """

    sql """ DROP TABLE IF EXISTS ${tableName2} """
    sql """
        CREATE TABLE ${tableName2}
        (
            k1 DATE,
            k2 int NOT NULL DEFAULT "1",
            k3 CHAR(10) COMMENT "string column",
            k4 INT NOT NULL DEFAULT "1" COMMENT "int column"
        )
        duplicate KEY(k1, k2)
        COMMENT "my first table"
        PARTITION BY RANGE(k1)
        (
            PARTITION p1 VALUES LESS THAN ("2020-02-01"),
            PARTITION p2 VALUES LESS THAN ("2020-03-01"),
            PARTITION p3 VALUES LESS THAN ("2020-04-01")
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "false"
        );
    """

    sql """ insert into ${tableName2} values ('2020-02-10', 2, 'a', 4) """

    sql """ alter table ${tableName2} ADD COLUMN k5 string after k4"""

    max_try_num = 60
    while (max_try_num--) {
        String res = getJobState(tableName2)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            sleep(1000)
            if (max_try_num < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }

    sql """ alter table ${tableName2} ADD PARTITION p4 VALUES LESS THAN ("2020-05-01") """
    sql """ insert into ${tableName2} values ('2020-04-10', 2, 'b', 5, 'test') """

    sql """ alter table ${tableName2} set ("light_schema_change"="true") """

    sql """ select * from ${tableName2} """
}

