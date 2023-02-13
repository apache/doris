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

def initConnection(uri, method) {
    def dst = "http://" + context.config.otherConfigs.get("beHttpAddress")
    def conn = new URL(dst + uri).openConnection()
    conn.setRequestMethod(method)
    conn.setRequestProperty("Content-Type", "application/text")
    return conn
}

/**
 * @Params uri is "/xxx", data is request body
 * @Return response body
 */
def doPost(uri, data = null) {
    def conn = initConnection(uri, "POST")
    if (data) {
        //
        logger.info("query body: " + data)
        conn.doOutput = true
        def writer = new OutputStreamWriter(conn.outputStream)
        writer.write(data)
        writer.flush()
        writer.close()
    }
    conn.connect()
    logger.info(conn.content.text)
    return conn
}

def doGet(uri) {
    def conn = initConnection(uri, "GET")
    conn.connect()
    logger.info(conn.content.text)
    return conn
}

suite("test_check_rpc_channel") {
    def uri = "/api/check_rpc_channel/" + context.config.otherConfigs.get("beAddress") + "/" + context.config.otherConfigs.get("brpcPort") + "/1024000"
    def conn = doGet(uri)
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}

suite("test_reset_rpc_channel") {
    def conn = doGet("/api/reset_rpc_channel/all")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}

suite("test_check_tablet_segment") {
    def conn = doPost("/api/check_tablet_segment_lost?repair=true", null)
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}

suite("test_check_sum") {
    def conn = doGet("/api/checksum?tablet_id=12562&schema_hash=1296025096&version=1")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}

suite("test_compaction_action") {
    def conn = doGet("/api/compaction/run_status")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)

    conn = doGet("/api/compaction/show?tablet_id=12562")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)

    conn = doPost("/api/compaction/run?tablet_id=12562&compact_type=cumulative")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)

    conn = doGet("/api/compaction/run_status?tablet_id=12562")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}

suite("test_config") {
    def conn = doGet("/api/show_config")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)

    conn = doPost("/api/update_config?&persist=false&slave_replica_writer_rpc_timeout_sec=90")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}

suite("test_health") {
    def conn = doGet("/api/health")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}

suite("test_jeprofile") {
    def conn = doGet("/jeheap/dump")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}

suite("test_meta") {
    def conn = doGet("/api/meta/header/12562")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}

suite("test_metrics") {
    def conn = doGet("/metrics?type=core")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)

    conn = doGet("/metrics?type=json?with_tablet=true")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)

    conn = doGet("/metrics?with_tablet=false")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}

suite("test_pad_rowset") {
    def conn = doPost("/api/pad_rowset?tablet_id=12562&start_version=1&end_version=2")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}

suite("test_pprof") {
    def conn = doGet("/api/pprof/heap")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)

    conn = doGet("/api/pprof/growth")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)

    conn = doGet("/api/pprof/profile")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)

    conn = doGet("/api/pprof/cmdline")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)

    conn = doGet("/api/pprof/symbol")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)

    conn = doPost("/api/pprof/symbol")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}

suite("test_reload_tablet") {
    //FIXME: certainly failed test
    def conn = doGet("/api/reload_tablet")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}

suite("test_restore_tablet") {
    //FIXME: certainly failed test
    def conn = doPost("/api/restore_tablet?tablet_id=12562&schema_hash=1296025096")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}

suite("test_reset_rpc_channel") {
    def conn = doGet("/api/reset_rpc_channel/all")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}

suite("test_snapshot") {
    def conn = doGet("/api/snapshot?tablet_id=12560&schema_hash=1296025096")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}



suite("test_distribution") {
    def conn = doGet("/api/tablets_distribution?group_by=partition&partition_id=1")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}

suite("test_tablets_info") {
    def conn = doGet("/tablets_json")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}

suite("test_be_version") {
    def conn = doGet("/api/be_version_info")
    assertTrue(conn.responseCode == 200 || conn.responseCode == 201)
}