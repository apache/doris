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

def initConnection(dst, uri, method) {
    def conn = new URL(dst + uri).openConnection()
    conn.setRequestMethod(method)
    conn.setRequestProperty("Content-Type", "application/text")
    return conn
}

/**
 * @Params uri is "/xxx", data is request body
 * @Return response body
 */
def doPost(dst, uri, data = null) {
    def conn = initConnection(dst, uri, "POST")
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
    return conn
}

def doGet(dst, uri) {
    def conn = initConnection(dst, uri, "GET")
    conn.connect()
    return conn
}

suite("test_be_http") {
    String[][] backends = sql """ show backends; """
    assertTrue(backends.size() > 0)
    String backendId
    def beIdToIp = [:]
    def beIdToPort = [:]
    for (String[] backend in backends) {
        beIdToIp.put(backend[0], backend[2])
        beIdToPort.put(backend[0], backend[6])
    }
    backendId = beIdToIp.keySet()[0]
    String beIp = beIdToIp.get(backendId)
    String bePort = beIdToPort.get(backendId)
    String dst = "http://${beIp}:${bePort}"

    def tableName = "test_be_http"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (c1 int) DISTRIBUTED BY HASH(c1) PROPERTIES('replication_num'='1')  """
    for (i in 0..<10) {
        sql """ INSERT INTO ${tableName} (c1) VALUES (${i}) """
    }
    // fetch tablet_id and schema_hash for testing
    def tabletsInfo = sql """ SHOW TABLETS FROM test_be_http """
    def tabletId = tabletsInfo[0][0]
    def schemaHash = tabletsInfo[0][3]
    def version = tabletsInfo[0][4]

    //test_check_rpc_channel
    def uri = "/api/check_rpc_channel/${beIp}/${bePort}/1024000"
    def conn1 = doGet(dst, uri)
    assertTrue(conn1.responseCode == 200 || conn1.responseCode == 201)

    //test_reset_rpc_channel
    def conn2 = doGet(dst,"/api/reset_rpc_channel/all")
    assertTrue(conn2.responseCode == 200 || conn2.responseCode == 201)

    //test_check_tablet_segment
    def conn3 = doPost(dst,"/api/check_tablet_segment_lost?repair=false", null)
    assertTrue(conn3.responseCode == 200 || conn3.responseCode == 201)

    //test_checksum
    def conn4 = doGet(dst,"/api/checksum?tablet_id=${tabletId}&schema_hash=${schemaHash}&version=${version}")
    assertTrue(conn4.responseCode == 200 || conn4.responseCode == 201)


    //test_compaction
    def conn5 = doGet(dst,"/api/compaction/run_status")
    assertTrue(conn5.responseCode == 200 || conn5.responseCode == 201)

    def conn6 = doGet(dst,"/api/compaction/show?tablet_id=${tabletId}")
    assertTrue(conn6.responseCode == 200 || conn6.responseCode == 201)

    def conn7 = doPost(dst,"/api/compaction/run?tablet_id=${tabletId}&compact_type=cumulative")
    assertTrue(conn7.responseCode == 200 || conn7.responseCode == 201)

    def conn8 = doGet(dst,"/api/compaction/run_status?tablet_id=${tabletId}")
    assertTrue(conn8.responseCode == 200 || conn8.responseCode == 201)


    //test_config
    def conn9 = doGet(dst,"/api/show_config")
    assertTrue(conn9.responseCode == 200 || conn9.responseCode == 201)

    def conn10 = doPost(dst,"/api/update_config?&persist=false&slave_replica_writer_rpc_timeout_sec=90")
    assertTrue(conn10.responseCode == 200 || conn10.responseCode == 201)

    //test_health
    def conn11 = doGet(dst,"/api/health")
    assertTrue(conn11.responseCode == 200 || conn11.responseCode == 201)

    //test_jeprofile
    def conn12 = doGet(dst,"/jeheap/dump")
    assertTrue(conn12.responseCode == 200 || conn12.responseCode == 201)

    //test_meta
    def conn13 = doGet(dst,"/api/meta/header/${tabletId}")
    assertTrue(conn13.responseCode == 200 || conn13.responseCode == 201)

    //test_metrics
    def conn14 = doGet(dst,"/metrics?type=core")
    assertTrue(conn14.responseCode == 200 || conn14.responseCode == 201)

    def conn15 = doGet(dst,"/metrics?type=json?with_tablet=true")
    assertTrue(conn15.responseCode == 200 || conn15.responseCode == 201)

    def conn16 = doGet(dst,"/metrics?with_tablet=false")
    assertTrue(conn16.responseCode == 200 || conn16.responseCode == 201)


    //test_pprof
    def conn17 = doGet(dst,"/api/pprof/heap")
    assertTrue(conn17.responseCode == 200 || conn17.responseCode == 201)

    def conn18 = doGet(dst,"/api/pprof/growth")
    assertTrue(conn18.responseCode == 200 || conn18.responseCode == 201)

    def conn19 = doGet(dst,"/api/pprof/profile")
    assertTrue(conn19.responseCode == 200 || conn19.responseCode == 201)

    def conn20 = doGet(dst,"/api/pprof/cmdline")
    assertTrue(conn20.responseCode == 200 || conn20.responseCode == 201)

    def conn21 = doGet(dst,"/api/pprof/symbol")
    assertTrue(conn21.responseCode == 200 || conn21.responseCode == 201)

    def conn22 = doPost(dst,"/api/pprof/symbol")
    assertTrue(conn22.responseCode == 200 || conn22.responseCode == 201)


    //test_snapshot
    def conn23 = doGet(dst,"/api/snapshot?tablet_id=${tabletId}&schema_hash=${schemaHash}")
    assertTrue(conn23.responseCode == 200 || conn23.responseCode == 201)

    //test_reset_rpc_channel
    def conn24 = doGet(dst,"/api/reset_rpc_channel/all")
    assertTrue(conn24.responseCode == 200 || conn24.responseCode == 201)

    //test_distribution
    def conn25 = doGet(dst,"/api/tablets_distribution?group_by=partition&partition_id=1")
    assertTrue(conn25.responseCode == 200 || conn25.responseCode == 201)

    //test_tablets_info
    def conn26 = doGet(dst,"/tablets_json")
    assertTrue(conn26.responseCode == 200 || conn26.responseCode == 201)

    //test_be_info
    def conn27 = doGet(dst,"/api/be_version_info")
    assertTrue(conn27.responseCode == 200 || conn27.responseCode == 201)

    def partitionInfo = sql """ SHOW PARTITIONS FROM ${tableName} """
    def partitionId = partitionInfo[0][0]
    def conn28 = doGet(dst,"/api/tablets_distribution?group_by=partition&partition_id=${partitionId}")
    assertTrue(conn28.responseCode == 200 || conn28.responseCode == 201)

    //test_pad_rowset
    def conn29 = doPost(dst,"/api/pad_rowset?tablet_id=${tabletId}&start_version=1&end_version=2")
    assertTrue(conn29.responseCode == 200 || conn29.responseCode == 201)


    sql """ DROP TABLE IF EXISTS ${tableName} """
}