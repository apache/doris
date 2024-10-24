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

suite("test_schema_change_duplicate", "p0") {
    def tableName3 = "test_all_duplicate"

    def getJobState = { tableName ->
        def jobStateResult = sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return jobStateResult[0][9]
    }

    def getCreateViewState = { tableName ->
        def createViewStateResult = sql """ SHOW ALTER TABLE MATERIALIZED VIEW WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return createViewStateResult[0][8]
    }

    def execStreamLoad = {
        streamLoad {
            table "${tableName3}"

            set 'column_separator', ','

            file 'all_types.csv'

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(2500, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }
    }

    sql """ DROP TABLE IF EXISTS ${tableName3} """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName3} (
      `k1` int(11) NULL,
      `k2` tinyint(4) NULL,
      `k3` smallint(6) NULL,
      `k4` int(30) NULL,
      `k5` largeint(40) NULL,
      `k6` float NULL,
      `k7` double NULL,
      `k8` decimal(9, 0) NULL,
      `k9` char(10) NULL,
      `k10` varchar(1024) NULL,
      `k11` text NULL,
      `k12` date NULL,
      `k13` datetime NULL
    ) ENGINE=OLAP
    duplicate KEY(k1, k2, k3)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """

    execStreamLoad()

    sql """ alter table ${tableName3} modify column k4 string NULL"""
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    execStreamLoad();

    sql """ alter table ${tableName3} modify column k2 bigint(11) key NULL"""
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    execStreamLoad()

    /*
    sql """ create materialized view view_1 as select k2, k1, k4, k5 from ${tableName3} """
    sleep(10)
    max_try_num = 60
    while (max_try_num--) {
        String res = getCreateViewState(tableName3)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            execStreamLoad()
            if (max_try_num < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    */

    sql """ alter table ${tableName3} modify column k5 string NULL"""
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    execStreamLoad()

    sql """ alter table ${tableName3} add column v14 int NOT NULL default "1" after k13 """
    sql """ insert into ${tableName3} values (10001, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00', 10086) """

    sql """ alter table ${tableName3} modify column v14 int NULL default "1" """
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tableName3}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    int val = 100000 + max_try_num
    sql """ insert into ${tableName3} values (${val}, 2, 3, 4, 5, 6.6, 1.7, 8.8,
'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00', 9527) """


    sql """ alter table ${tableName3} drop column v14 """
    execStreamLoad()

    sql """ alter table ${tableName3} add column v14 int NOT NULL default "1" after k13 """

    sql """ insert into ${tableName3} values (10002, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00', 10086) """

    sql """ alter table ${tableName3} drop column v14 """
    
    sql """ alter table ${tableName3} add column v14 bitmap after k13 """

    sql """ insert into ${tableName3} values (10002, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00', to_bitmap(243)) """

    sql """ alter table ${tableName3} drop column v14 """

    List<List<Object>> result  = sql """ select * from ${tableName3} """
    for (row : result) {
        assertEquals(2, row[1]);
        assertEquals(3, row[2]);
        assertEquals("4", row[3]);
        assertEquals("5", row[4]);
    }
}

