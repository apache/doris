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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_txn_commit_inject", "docker") {
    if (!isCloudMode()) {
        return
    }

    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1'
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true

    def token = "greedisgood9999"

    def enable_ms_inject_api = { msHttpPort, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=enable"
            check check_func
        }
    }

    def clear_ms_inject_api = { msHttpPort, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=clear"
            check check_func
        }
    }
    
    def inject_change_args_to_ms = { msHttpPort, key, value, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=set&name=${key}&behavior=change_args&value=${value}"
            check check_func
        }
    }

    def inject_suite_to_ms = { msHttpPort, suiteName, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=apply_suite&name=${suiteName}"
            check check_func
        }
    }

    docker(options) {
        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        logger.info("ms addr={}, port={}, ms endpoint={}", ms.host, ms.httpPort, msHttpPort)
        
        def dbName = "regression_test_fault_injection_p0_cloud"
        def table1 = dbName + ".test_txn_commit_inject"

        sql "DROP TABLE IF EXISTS ${table1} FORCE;"
        sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                `k1` int NOT NULL,
                `c1` int,
                `c2` int
                )UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true",
                "replication_num" = "1"); """

        // curl "ms_ip:ms_port/MetaService/http/v1/injection_point?token=greedisgood9999&op=apply_suite&name=Transaction::commit.enable_inject"
        inject_suite_to_ms.call(msHttpPort, "Transaction::commit.enable_inject") {
            respCode, body ->
                log.info("inject Transaction::commit.enable_inject suite resp: ${body} ${respCode}".toString()) 
        }

        // curl "ms_ip:ms_port/MetaService/http/v1/injection_point?token=greedisgood9999&op=enable"
        enable_ms_inject_api.call(msHttpPort) { respCode, body ->
            log.info("enable inject resp: ${body} ${respCode}".toString()) 
        }

        // with default fault injection possibility 1%, insert should be successful
        sql "insert into ${table1} values(3,3,3),(4,4,4);"
        sql "sync;"
        qt_sql "select * from ${table1} order by k1;"

        // set txn->commit() fault injection possibility to 100%
        // curl "ms_ip:ms_port/MetaService/http/v1/injection_point?token=greedisgood9999&op=set&name=Transaction::commit.inject_random_fault.set_p&behavior=change_args&value=%5B1.0%5D"
        inject_change_args_to_ms.call(msHttpPort, "Transaction::commit.inject_random_fault.set_p", URLEncoder.encode('[1.0]', "UTF-8")) {
            respCode, body ->
                log.info("inject Transaction::commit.inject_random_fault.set_p resp: ${body} ${respCode}".toString()) 
        }

        test {
            sql "insert into ${table1} values(1,1,1),(2,2,2);"
            exception ""
        }

        // curl "ms_ip:port/MetaService/http/v1/injection_point?token=greedisgood9999&op=clear"
        clear_ms_inject_api.call(msHttpPort) { respCode, body ->
            log.info("clear all inject resp: ${body} ${respCode}".toString()) 
        }

        sql "insert into ${table1} values(1,1,1),(2,2,2);"
        sql "sync;"
        qt_sql "select * from ${table1} order by k1;"
    }
}