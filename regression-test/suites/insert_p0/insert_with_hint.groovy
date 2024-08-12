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

import groovy.json.JsonSlurper

suite('insert_with_hint') {
    sql 'set enable_nereids_planner=false'
    sql 'set enable_fallback_to_original_planner=true'
    sql 'set enable_nereids_dml=false'

    def tableName = "nereids_insert_with_hint7"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """create table ${tableName} (
        k int null,
        v int null,
        v2 int null,
        v3 int null
    ) unique key (k) distributed by hash(k) buckets 1
    properties("replication_num" = "1",
    "enable_unique_key_merge_on_write"="true",
    "disable_auto_compaction"="true"); """
    sql "insert into ${tableName} values(1,1,3,4),(2,2,4,5),(3,3,2,3),(4,4,1,2);"
    qt_5 "select * from ${tableName} order by k;"

    sql "set enable_profile=true;"
    sql "sync;"

    def httpGet = { url ->
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + url).openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

    def getProfile = { stmt -> 
        def profileUrl = '/rest/v1/query_profile/'
        def profiles = httpGet(profileUrl)
        // log.info("profiles:{}", profiles);
        profiles = new JsonSlurper().parseText(profiles)
        assertEquals(0, profiles.code)

        def profileId = null;
        for (def profile in profiles["data"]["rows"]) {
            if (profile["Sql Statement"].contains("${stmt}")) {
                profileId = profile["Profile ID"]
                break;
            }
        }
        log.info("profileId=${profileId}")
        def profileDetail = httpGet("/rest/v1/query_profile/" + profileId)
        return profileDetail
    }

    def sql_stmt = "insert /*+ SET_VAR(enable_memtable_on_sink_node=true) */into ${tableName}(k,v) select v2,v3 from ${tableName};"
    sql sql_stmt
    def profileDetail = getProfile(sql_stmt)
    assertTrue(profileDetail.contains("DeltaWriterV2"))

    sql_stmt = "insert /*+ SET_VAR(enable_memtable_on_sink_node=false) */into ${tableName}(k,v) select v2,v3 from ${tableName};"
    sql sql_stmt
    def profileDetail2 = getProfile(sql_stmt)
    assertTrue(!profileDetail2.contains("DeltaWriterV2"))
    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql "set enable_profile=false;"
    sql "sync"
}
