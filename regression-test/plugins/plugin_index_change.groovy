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

import org.apache.doris.regression.suite.Suite

import groovy.json.JsonSlurper
import java.util.regex.Matcher;
import java.util.regex.Pattern;

def delta_time = 1000

Suite.metaClass.wait_for_last_build_index_finish = {table_name, OpTimeout ->
    def useTime = 0
    for(int t = delta_time; t <= OpTimeout; t += delta_time){
        def alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
        alter_res = alter_res.toString()
        if(alter_res.contains("FINISHED")) {
            logger.info(table_name + " latest alter job finished, detail: " + alter_res)
            break
        } else if (alter_res.contains("CANCELLED")) {
            logger.info(table_name + " latest alter job failed, detail: " + alter_res)
            assertTrue(false)
        }
        useTime = t
        sleep(delta_time)
    }
    assertTrue(useTime <= OpTimeout, "wait for last build index finish timeout")
}

Suite.metaClass.build_index_on_table = {index_name, table_name ->
    if (isCloudMode()) {
        sql """build index on ${table_name}"""
    } else {
        sql """build index ${index_name} on ${table_name}"""
    }

}

Suite.metaClass.wait_for_last_col_change_finish = { table_name, OpTimeout ->
    def useTime = 0

    for (int t = delta_time; t <= OpTimeout; t += delta_time) {
        def alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
        alter_res = alter_res.toString()
        if (alter_res.contains("FINISHED")) {
            sleep(3000) // wait change table state to normal
            logger.info(table_name + " latest alter job finished, detail: " + alter_res)
            break
        }
        useTime = t
        sleep(delta_time)
    }
    assertTrue(useTime <= OpTimeout, "wait_for_last_col_change_finish timeout")
}

Suite.metaClass.wait_for_last_schema_change_finish = {table_name, OpTimeout ->
    wait_for_last_col_change_finish(table_name, OpTimeout)
    wait_for_last_build_index_finish(table_name, OpTimeout)
}


Suite.metaClass.http_get = { url ->
    def dst = 'http://' + context.config.feHttpAddress
    def conn = new URL(dst + url).openConnection()
    conn.setRequestMethod("GET")
    def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" +
            (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
    conn.setRequestProperty("Authorization", "Basic ${encoding}")
    return conn.getInputStream().getText()
}

Suite.metaClass.check_inverted_index_filter_rows = { sql, expectedRowsInvertedIndexFiltered ->
    order_qt_sql sql
    def profileUrl = '/rest/v1/query_profile/'
    def profiles = http_get(profileUrl)
    log.debug("profiles:{}", profiles);
    profiles = new JsonSlurper().parseText(profiles)
    assertEquals(0, profiles.code)

    def profileId = null;
    for (def profile in profiles["data"]["rows"]) {
        if (profile["Sql Statement"].contains(sql)) {
            profileId = profile["Profile ID"]
            break;
        }
    }
    log.info("profileId:{}", profileId);
    def profileDetail = http_get("/rest/v1/query_profile/" + profileId)
    String regex = "RowsInvertedIndexFiltered:&nbsp;&nbsp;(\\d+)"
    Pattern pattern = Pattern.compile(regex)
    Matcher matcher = pattern.matcher(profileDetail)
    while (matcher.find()) {
        int number = Integer.parseInt(matcher.group(1))
        log.info("filter number:{}", number)
        assertEquals(expectedRowsInvertedIndexFiltered, number)
    }
}


Suite.metaClass.check_bf_index_filter_rows = { sql, expectedRowsBfFiltered ->
    order_qt_sql sql
    def profileUrl = '/rest/v1/query_profile/'
    def profiles = http_get(profileUrl)
    log.debug("profiles:{}", profiles);
    profiles = new JsonSlurper().parseText(profiles)
    assertEquals(0, profiles.code)

    def profileId = null;
    for (def profile in profiles["data"]["rows"]) {
        if (profile["Sql Statement"].contains(sql)) {
            profileId = profile["Profile ID"]
            break;
        }
    }
    log.info("profileId:{}", profileId);
    def profileDetail = http_get("/rest/v1/query_profile/" + profileId)
    String regex = "RowsBloomFilterFiltered:&nbsp;&nbsp;(\\d+)"
    Pattern pattern = Pattern.compile(regex)
    Matcher matcher = pattern.matcher(profileDetail)
    while (matcher.find()) {
        int number = Integer.parseInt(matcher.group(1))
        log.info("filter number:{}", number)
        assertEquals(expectedRowsBfFiltered, number)
    }
}