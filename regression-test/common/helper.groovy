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

import com.google.common.collect.Maps

import java.util.Map
import java.util.List

class Describe {
    String index
    String field
    String type
    Boolean is_key

    Describe(String index, String field, String type, Boolean is_key) {
        this.index = index
        this.field = field
        this.type = type
        this.is_key = is_key
    }

    String toString() {
        return "index: ${index}, field: ${field}, type: ${type}, is_key: ${is_key}"
    }
}

class Helper {
    def suite
    def context
    def logger
    String alias = null

    // the configurations about ccr syncer.
    def sync_gap_time = 5000
    def syncerAddress = "127.0.0.1:9180"

    Helper(suite) {
        this.suite = suite
        this.context = suite.context
        this.logger = suite.logger

        // Disable fuzzy config for the ccr test suites.
        suite.try_sql """ ADMIN SET FRONTEND CONFIG ("random_add_cluster_keys_for_mow" = "false") """
        suite.try_target_sql """ ADMIN SET FRONTEND CONFIG ("random_add_cluster_keys_for_mow" = "false") """
    }

    void set_alias(String alias) {
        this.alias = alias
    }

    String randomSuffix() {
        def hashCode = UUID.randomUUID().toString().replace("-", "").hashCode()
        if (hashCode < 0) {
            hashCode *= -1;
        }
        return Integer.toString(hashCode)
    }

    def get_backup_label_prefix(String table = "") {
        return "ccrs_" + get_ccr_job_name(table)
    }

    def get_ccr_job_name(String table = "") {
        def name = context.suiteName
        if (!table.equals("")) {
            name = name + "_" + table
        }
        return name
    }

    def get_ccr_body(String table, String db = null) {
        if (db == null) {
            db = context.dbName
        }

        def gson = new com.google.gson.Gson()

        Map<String, Object> srcSpec = context.getSrcSpec(db)
        srcSpec.put("table", table)

        Map<String, Object> destSpec = context.getDestSpec(db)
        if (alias != null) {
            destSpec.put("table", alias)
        } else {
            destSpec.put("table", table)
        }

        Map<String, Object> body = Maps.newHashMap()
        String name = context.suiteName
        if (!table.equals("")) {
            name = name + "_" + table
        }
        body.put("name", name)
        body.put("src", srcSpec)
        body.put("dest", destSpec)

        return gson.toJson(body)
    }

    void ccrJobDelete(table = "") {
        def bodyJson = get_ccr_body "${table}"
        suite.httpTest {
            uri "/delete"
            endpoint syncerAddress
            body "${bodyJson}"
            op "post"
        }
    }

    void ccrJobCreate(table = "") {
        def bodyJson = get_ccr_body "${table}"
        suite.httpTest {
            uri "/create_ccr"
            endpoint syncerAddress
            body "${bodyJson}"
            op "post"
            check { code, body ->
                if (!"${code}".toString().equals("200")) {
                    throw new Exception("request failed, code: ${code}, body: ${body}")
                }
                def jsonSlurper = new groovy.json.JsonSlurper()
                def object = jsonSlurper.parseText "${body}"
                if (!object.success) {
                    throw new Exception("request failed, error msg: ${object.error_msg}")
                }
            }
        }
    }

    void ccrJobCreateAllowTableExists(table = "") {
        def bodyJson = get_ccr_body "${table}"
        def jsonSlurper = new groovy.json.JsonSlurper()
        def object = jsonSlurper.parseText "${bodyJson}"
        object['allow_table_exists'] = true
        logger.info("json object ${object}")

        bodyJson = new groovy.json.JsonBuilder(object).toString()
        suite.httpTest {
            uri "/create_ccr"
            endpoint syncerAddress
            body "${bodyJson}"
            op "post"
            check { code, body ->
                if (!"${code}".toString().equals("200")) {
                    throw new Exception("request failed, code: ${code}, body: ${body}")
                }
                def value = jsonSlurper.parseText "${body}"
                if (!value.success) {
                    throw new Exception("request failed, error msg: ${value.error_msg}")
                }
            }
        }
    }

    void ccrJobPause(table = "") {
        def bodyJson = get_ccr_body "${table}"
        suite.httpTest {
            uri "/pause"
            endpoint syncerAddress
            body "${bodyJson}"
            op "post"
            check { code, body ->
                if (!"${code}".toString().equals("200")) {
                    throw new Exception("request failed, code: ${code}, body: ${body}")
                }
                def jsonSlurper = new groovy.json.JsonSlurper()
                def object = jsonSlurper.parseText "${body}"
                if (!object.success) {
                    throw new Exception("request failed, error msg: ${object.error_msg}")
                }
            }
        }
    }

    void ccrJobResume(table = "") {
        def bodyJson = get_ccr_body "${table}"
        suite.httpTest {
            uri "/resume"
            endpoint syncerAddress
            body "${bodyJson}"
            op "post"
            check { code, body ->
                if (!"${code}".toString().equals("200")) {
                    throw new Exception("request failed, code: ${code}, body: ${body}")
                }
                def jsonSlurper = new groovy.json.JsonSlurper()
                def object = jsonSlurper.parseText "${body}"
                if (!object.success) {
                    throw new Exception("request failed, error msg: ${object.error_msg}")
                }
            }
        }
    }

    void ccrJobDesync(table = "") {
        def bodyJson = get_ccr_body "${table}"
        suite.httpTest {
            uri "/desync"
            endpoint syncerAddress
            body "${bodyJson}"
            op "post"
            check { code, body ->
                if (!"${code}".toString().equals("200")) {
                    throw new Exception("request failed, code: ${code}, body: ${body}")
                }
                def jsonSlurper = new groovy.json.JsonSlurper()
                def object = jsonSlurper.parseText "${body}"
                if (!object.success) {
                    throw new Exception("request failed, error msg: ${object.error_msg}")
                }
            }
        }
    }

    void ccrJobSync(table = "") {
        def bodyJson = get_ccr_body "${table}"
        suite.httpTest {
            uri "/sync"
            endpoint syncerAddress
            body "${bodyJson}"
            op "post"
            check { code, body ->
                if (!"${code}".toString().equals("200")) {
                    throw new Exception("request failed, code: ${code}, body: ${body}")
                }
                def jsonSlurper = new groovy.json.JsonSlurper()
                def object = jsonSlurper.parseText "${body}"
                if (!object.success) {
                    throw new Exception("request failed, error msg: ${object.error_msg}")
                }
            }
        }
    }

    // lag info
    // "lag" "first_commit_seq" "last_commit_seq" "first_binlog_timestamp" "last_binlog_timestamp" "time_interval_secs"
    Object get_job_lag(tableName = "") {
        def request_body = get_ccr_body(tableName)
        def get_job_lag_url = { check_func ->
            suite.httpTest {
                uri "/get_lag"
                endpoint syncerAddress
                body request_body
                op "post"
                check check_func
            }
        }

        def result = null
        get_job_lag_url.call() { code, body ->
            if (!"${code}".toString().equals("200")) {
                throw "request failed, code: ${code}, body: ${body}"
            }
            def jsonSlurper = new groovy.json.JsonSlurper()
            def object = jsonSlurper.parseText "${body}"
            if (!object.success) {
                throw "request failed, error msg: ${object.error_msg}"
            }
            result = object
            logger.info("job lag info: ${object}")
        }
        return result
    }

    void enableDbBinlog() {
        suite.sql """
            ALTER DATABASE ${context.dbName} SET properties ("binlog.enable" = "true")
            """
    }

    void disableDbBinlog() {
        suite.sql """
            ALTER DATABASE ${context.dbName} SET properties ("binlog.enable" = "false")
            """
    }

    Boolean checkShowTimesOf(sqlString, myClosure, times, func = "sql") {
        Boolean ret = false
        List<List<Object>> res
        while (times > 0) {
            try {
                if (func == "sql") {
                    res = suite.sql "${sqlString}"
                } else {
                    res = suite.target_sql "${sqlString}"
                }
                if (myClosure.call(res)) {
                    ret = true
                }
            } catch (Exception e) {
                logger.info("exception", e)
            }

            if (ret) {
                break
            } else if (--times > 0) {
                sleep(sync_gap_time)
            }
        }

        if (!ret) {
            logger.info("last select result: ${res}")
        }

        return ret
    }

    Boolean checkShowMapArrayResult(sqlString, myClosure, times, func = "sql") {
        Boolean ret = false
        List<List<Object>> res
        while (times > 0) {
            try {
                if (func == "sql") {
                    res = suite.sql_return_maparray "${sqlString}"
                } else {
                    res = suite.target_sql_return_maparray "${sqlString}"
                }
                if (myClosure.call(res)) {
                    ret = true
                }
            } catch (Exception e) {
                logger.info("exception", e)
            }

            if (ret) {
                break
            } else if (--times > 0) {
                sleep(sync_gap_time)
            }
        }

        if (!ret) {
            logger.info("last select result: ${res}")
        }

        return ret
    }

    // wait until all restore tasks of the dest cluster are finished.
    Boolean checkRestoreFinishTimesOf(checkTable, times) {
        Boolean ret = false
        while (times > 0) {
            def sqlInfo = suite.target_sql "SHOW RESTORE FROM TEST_${context.dbName}"
            for (List<Object> row : sqlInfo) {
                if ((row[10] as String).contains(checkTable)) {
                    logger.info("SHOW RESTORE result: ${row}")
                    ret = (row[4] as String) == "FINISHED"
                }
            }

            if (ret) {
                break
            } else if (--times > 0) {
                sleep(sync_gap_time)
            }
        }

        return ret
    }

    // Check N times whether the num of rows of the downstream data is expected.
    Boolean checkSelectTimesOf(sqlString, rowSize, times) {
        def tmpRes = []
        while (times-- > 0) {
            try {
                tmpRes = suite.target_sql "${sqlString}"
                if (tmpRes.size() == rowSize) {
                    return true
                }
            } catch (Exception e) {
                logger.info("exception", e)
            }
            sleep(sync_gap_time)
        }

        logger.info("last select result: ${tmpRes}")
        logger.info("expected row size: ${rowSize}, actual row size: ${tmpRes.size()}")
        return false
    }

    Boolean checkSelectColTimesOf(sqlString, colSize, times) {
        def tmpRes = suite.target_sql "${sqlString}"
        while (tmpRes.size() == 0 || tmpRes[0].size() != colSize) {
            sleep(sync_gap_time)
            if (--times > 0) {
                tmpRes = suite.target_sql "${sqlString}"
            } else {
                break
            }
        }
        return tmpRes.size() > 0 && tmpRes[0].size() == colSize
    }

    Boolean checkData(data, beginCol, value) {
        if (data.size() < beginCol + value.size()) {
            return false
        }

        for (int i = 0; i < value.size(); ++i) {
            if ((data[beginCol + i]) as int != value[i]) {
                return false
            }
        }

        return true
    }

    Integer getRestoreRowSize(checkTable) {
        def result = suite.target_sql "SHOW RESTORE FROM TEST_${context.dbName}"
        def size = 0
        for (List<Object> row : result) {
            if ((row[10] as String).contains(checkTable)) {
                size += 1
            }
        }

        return size
    }

    Boolean checkRestoreNumAndFinishedTimesOf(checkTable, expectedRestoreRows, times) {
        while (times > 0) {
            def restore_size = getRestoreRowSize(checkTable)
            if (restore_size >= expectedRestoreRows) {
                return checkRestoreFinishTimesOf(checkTable, times)
            }
            if (--times > 0) {
                sleep(sync_gap_time)
            }
        }

        return false
    }

    void force_fullsync(tableName = "") {
        def bodyJson = get_ccr_body "${tableName}"
        suite.httpTest {
            uri "/force_fullsync"
            endpoint syncerAddress
            body "${bodyJson}"
            op "post"
        }
    }

    Object get_job_progress(tableName = "") {
        def request_body = get_ccr_body(tableName)
        def get_job_progress_uri = { check_func ->
            suite.httpTest {
                uri "/job_progress"
                endpoint syncerAddress
                body request_body
                op "post"
                check check_func
            }
        }

        def result = null
        get_job_progress_uri.call() { code, body ->
            if (!"${code}".toString().equals("200")) {
                throw "request failed, code: ${code}, body: ${body}"
            }
            def jsonSlurper = new groovy.json.JsonSlurper()
            def object = jsonSlurper.parseText "${body}"
            if (!object.success) {
                throw "request failed, error msg: ${object.error_msg}"
            }
            logger.info("job progress: ${object.job_progress}")
            result = object.job_progress
        }
        return result
    }

    // test whether the ccr syncer has set a feature flag?
    Boolean has_feature(name) {
        def features_uri = { check_func ->
            suite.httpTest {
                uri "/features"
                endpoint syncerAddress
                body ""
                op "get"
                check check_func
            }
        }

        def result = null
        features_uri.call() { code, body ->
            if (!"${code}".toString().equals("200")) {
                throw "request failed, code: ${code}, body: ${body}"
            }
            def jsonSlurper = new groovy.json.JsonSlurper()
            def object = jsonSlurper.parseText "${body}"
            if (!object.success) {
                throw "request failed, error msg: ${object.error_msg}"
            }
            logger.info("features: ${object.flags}")
            result = object.flags
        }

        for (def flag in result) {
            if (flag.feature == name && flag.value) {
                return true
            }
        }
        return false
    }

    String upstream_version() {
        def version_variables = suite.sql_return_maparray "show variables like 'version_comment'"
        return version_variables[0].Value
    }

    Boolean is_version_supported(versions) {
        def version_variables = suite.sql_return_maparray "show variables like 'version_comment'"
        def matcher = version_variables[0].Value =~ /doris-(\d+\.\d+\.\d+)/
        if (matcher.find()) {
            def parts = matcher.group(1).tokenize('.')
            def major = parts[0].toLong()
            def minor = parts[1].toLong()
            def patch = parts[2].toLong()
            def version = String.format("%d%02d%02d", major, minor, patch).toLong()
            for (long expect : versions) {
                logger.info("current version ${version}, expect version ${expect}")
                def expect_version_set = expect.intdiv(100)
                def got_version_set = version.intdiv(100)
                if (expect_version_set == got_version_set && version < expect) {
                    return false
                }
            }
        }
        return true
    }

    Map<String, List<Describe>> get_table_describe(String table, String source = "sql") {
        def res
        if (source == "sql") {
            res = suite.sql_return_maparray "DESC ${table} ALL"
        } else {
            if (alias != null) {
                table = alias
            }
            res = suite.target_sql_return_maparray "DESC ${table} ALL"
        }

        def map = Maps.newHashMap()
        def index = ""
        for (def row : res) {
            if (row.IndexName != "" && row.IndexName != table) {
                index = row.IndexName
            }
            if (row.Field == "") {
                continue
            }

            if (!map.containsKey(index)) {
                map.put(index, [])
            }
            def is_key = false
            if (row.Key == "true" || row.Key == "YES") {
                is_key = true
            }
            map.get(index).add(new Describe(index, row.Field, row.Type, is_key))
        }
        return map
    }

    Boolean check_describes(Map<String, List<Describe>> expect, Map<String, List<Describe>> actual) {
        if (actual.size() != expect.size()) {
            return false
        }

        for (def key : expect.keySet()) {
            if (!actual.containsKey(key)) {
                return false
            }
            def expect_list = expect.get(key)
            def actual_list = actual.get(key)
            if (expect_list.size() != actual_list.size()) {
                return false
            }
            for (int i = 0; i < expect_list.size(); ++i) {
                if (expect_list[i].toString() != actual_list[i].toString()) {
                    return false
                }
            }
        }
        return true
    }

    Boolean check_table_describe_times(String table, times = 30) {
        return check_table_with_alias_describe_times(table, table, times)
    }

    Boolean check_table_with_alias_describe_times(String table, String alias, times = 30) {
        while (times > 0) {
            def upstream_describe = get_table_describe(table)
            def downstream_describe = get_table_describe(alias, "target")
            if (check_describes(upstream_describe, downstream_describe)) {
                return true
            }
            sleep(sync_gap_time)
            times--
        }

        def upstream_describe = get_table_describe(table)
        def downstream_describe = get_table_describe(alias, "target")
        logger.info("upstream describe: ${upstream_describe}")
        logger.info("downstream describe: ${downstream_describe}")
        return false
    }

    Boolean check_table_exists(String table, times = 30) {
        while (times > 0) {
            def res = suite.target_sql "SHOW TABLES LIKE '${table}'"
            if (res.size() > 0) {
                return true
            }
            sleep(sync_gap_time)
            times--
        }
        return false
    }

    void addFailpoint(String failpoint, def value, String tableName = "") {
        def gson = new com.google.gson.Gson()
        def request_body = [
            name: get_ccr_job_name(tableName),
            failpoint: failpoint,
        ]
        if (value != null) {
            request_body.put("value", value)
        }
        def add_failpoint_uri = { check_func ->
            suite.httpTest {
                uri "/failpoint"
                endpoint syncerAddress
                body gson.toJson(request_body)
                op "post"
                check check_func
            }
        }

        add_failpoint_uri.call() { code, body ->
            if (!"${code}".toString().equals("200")) {
                throw "request failed, code: ${code}, body: ${body}"
            }
            def jsonSlurper = new groovy.json.JsonSlurper()
            def object = jsonSlurper.parseText "${body}"
            if (!object.success) {
                throw "request failed, error msg: ${object.error_msg}"
            }
        }
    }

    void enableDebugpoint() {
        def gson = new com.google.gson.Gson()
        def request_body = [
            name: "",
            op: "enable",
        ]
        def enable_debugpoint_uri = { check_func ->
            suite.httpTest {
                uri "/debugpoint"
                endpoint syncerAddress
                body gson.toJson(request_body)
                op "post"
                check check_func
            }
        }

        enable_debugpoint_uri.call() { code, body ->
            if (!"${code}".toString().equals("200")) {
                throw "request failed, code: ${code}, body: ${body}"
            }
            def jsonSlurper = new groovy.json.JsonSlurper()
            def object = jsonSlurper.parseText "${body}"
            if (!object.success) {
                throw "request failed, error msg: ${object.error_msg}"
            }
        }
    }

    void disableDebugpoint() {
        def gson = new com.google.gson.Gson()
        def request_body = [
            name: "",
            op: "disable",
        ]
        def disable_debugpoint_uri = { check_func ->
            suite.httpTest {
                uri "/debugpoint"
                endpoint syncerAddress
                body gson.toJson(request_body)
                op "post"
                check check_func
            }
        }

        disable_debugpoint_uri.call() { code, body ->
            if (!"${code}".toString().equals("200")) {
                throw "request failed, code: ${code}, body: ${body}"
            }
            def jsonSlurper = new groovy.json.JsonSlurper()
            def object = jsonSlurper.parseText "${body}"
            if (!object.success) {
                throw "request failed, error msg: ${object.error_msg}"
            }
        }
    }

    void openDebugpoint(String name) {
        def gson = new com.google.gson.Gson()
        def request_body = [
            name: name,
            op: "open",
        ]
        def open_debugpoint_uri = { check_func ->
            suite.httpTest {
                uri "/debugpoint"
                endpoint syncerAddress
                body gson.toJson(request_body)
                op "post"
                check check_func
            }
        }

        open_debugpoint_uri.call() { code, body ->
            if (!"${code}".toString().equals("200")) {
                throw "request failed, code: ${code}, body: ${body}"
            }
            def jsonSlurper = new groovy.json.JsonSlurper()
            def object = jsonSlurper.parseText "${body}"
            if (!object.success) {
                throw "request failed, error msg: ${object.error_msg}"
            }
        }
    }

    void closeDebugpoint(String name) {
        def gson = new com.google.gson.Gson()
        def request_body = [
            name: name,
            op: "close",
        ]
        def close_debugpoint_uri = { check_func ->
            suite.httpTest {
                uri "/debugpoint"
                endpoint syncerAddress
                body gson.toJson(request_body)
                op "post"
                check check_func
            }
        }

        close_debugpoint_uri.call() { code, body ->
            if (!"${code}".toString().equals("200")) {
                throw "request failed, code: ${code}, body: ${body}"
            }
            def jsonSlurper = new groovy.json.JsonSlurper()
            def object = jsonSlurper.parseText "${body}"
            if (!object.success) {
                throw "request failed, error msg: ${object.error_msg}"
            }
        }
    }

    void removeFailpoint(String failpoint, String tableName = "") {
        addFailpoint(failpoint, null, tableName)
    }

    void forceSkipBinlogBy(String skipBy, Long commitSeq = 0, String tableName = "") {
        def gson = new com.google.gson.Gson()
        def request_body = [
            name: get_ccr_job_name(tableName),
            skip_commit_seq: commitSeq,
            skip_by: skipBy,
        ]
        def skip_binlog_uri = { check_func ->
            suite.httpTest {
                uri "/job_skip_binlog"
                endpoint syncerAddress
                body gson.toJson(request_body)
                op "post"
                check check_func
            }
        }

        skip_binlog_uri.call() { code, body ->
            if (!"${code}".toString().equals("200")) {
                throw "request failed, code: ${code}, body: ${body}"
            }
            def jsonSlurper = new groovy.json.JsonSlurper()
            def object = jsonSlurper.parseText "${body}"
            if (!object.success) {
                throw "request failed, error msg: ${object.error_msg}"
            }
        }
    }

    void forceSkipBinlogByPartialSync(String table, Long tableId, String tableName = "") {
        def gson = new com.google.gson.Gson()
        def request_body = [
            name: get_ccr_job_name(tableName),
            skip_by: "partialsync",
            skip_table: table,
            skip_table_id: tableId,
        ]
        def skip_binlog_uri = { check_func ->
            suite.httpTest {
                uri "/job_skip_binlog"
                endpoint syncerAddress
                body gson.toJson(request_body)
                op "post"
                check check_func
            }
        }

        skip_binlog_uri.call() { code, body ->
            if (!"${code}".toString().equals("200")) {
                throw "request failed, code: ${code}, body: ${body}"
            }
            def jsonSlurper = new groovy.json.JsonSlurper()
            def object = jsonSlurper.parseText "${body}"
            if (!object.success) {
                throw "request failed, error msg: ${object.error_msg}"
            }
        }
    }

    Boolean checkJobInIncrementalSync(Integer times = 30, String tableName = "") {
        def DB_TABLES_INCREMENTAL_SYNC = 1
        def DB_INCREMENTAL_SYNC = 3
        def TABLE_INCREMENTAL_SYNC = 501

        def job_progress
        for (int i = 0; i < times; i++) {
            job_progress = get_job_progress(tableName)
            if (job_progress != null && (job_progress.sync_state == DB_TABLES_INCREMENTAL_SYNC ||
                    job_progress.sync_state == DB_INCREMENTAL_SYNC ||
                    job_progress.sync_state == TABLE_INCREMENTAL_SYNC)) {
                return true
            }
            sleep(sync_gap_time)
        }
        logger.info("last job progress: ${job_progress}")
        return false
    }
}

new Helper(suite)
