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

import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.memory.RootAllocator
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.doris.regression.util.Http
import org.apache.doris.thrift.BackendService
import org.apache.doris.thrift.TScanCloseParams
import org.apache.doris.thrift.TScanNextBatchParams
import org.apache.doris.thrift.TScanOpenParams
import org.apache.doris.thrift.TStatusCode
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

// The Spark and Flink Doris connectors read a table through FE's _query_plan API and the BE
// Thrift scanner (open_scanner / get_next / close_scanner), and the BE builds the batches with
// the Arrow Flight convertor. A scan that fails on the BE, here on values that convertor
// rejects, must reach the reader as an error instead of a normal end after the rows fetched so
// far (none here).
suite("test_query_plan_scan_validation", "arrow_flight_sql") {
    def db = context.dbName
    sql "DROP TABLE IF EXISTS query_plan_scan_input"
    sql """CREATE TABLE query_plan_scan_input (id INT, encoded STRING, event_time STRING)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES ("replication_num" = "1")"""
    sql """INSERT INTO query_plan_scan_input VALUES
           (1, '616263', '2024-01-01 00:00:00'), (2, '84', '0000-12-31 00:00:00')"""

    // Reads a _query_plan scan the way the connectors do. Returns the number of rows read and
    // the error get_next reported, or null when it reported the end of the scan.
    def scan = { String query ->
        def plan = Http.POST(
                "http://${context.config.feHttpAddress}/api/${db}/query_plan_scan_input/_query_plan",
                [sql: query], true, context.config.feHttpUser, context.config.feHttpPassword)
        assertEquals(0, plan.code, plan.toString())
        assertEquals(200, plan.data.status, plan.data.toString())
        // One bucket and one replica: the plan has a single tablet on a single BE.
        def tablets = plan.data.partitions.keySet().collect { it as long }
        String routing = plan.data.partitions.values().first().routings.first()
        String host = routing.substring(0, routing.lastIndexOf(':')).replace('[', '').replace(']', '')
        int port = routing.substring(routing.lastIndexOf(':') + 1) as int

        def socket = new TSocket(host, port, 60000)
        socket.open()
        def allocator = new RootAllocator()
        try {
            def client = new BackendService.Client(new TBinaryProtocol(socket))
            def params = new TScanOpenParams()
            params.setCluster("default_cluster")
            params.setDatabase(db)
            params.setTable("query_plan_scan_input")
            params.setTabletIds(tablets)
            params.setOpaquedQueryPlan(plan.data.opaqued_query_plan)
            params.setUser(context.config.feHttpUser)
            params.setPasswd(context.config.feHttpPassword ?: "")
            def opened = client.openScanner(params)
            assertEquals(TStatusCode.OK, opened.getStatus().getStatusCode(), opened.toString())
            long rows = 0
            String error = null
            try {
                while (true) {
                    def batch = client.getNext(new TScanNextBatchParams()
                            .setContextId(opened.getContextId()).setOffset(rows))
                    if (batch.getStatus().getStatusCode() != TStatusCode.OK) {
                        error = batch.getStatus().getErrorMsgs().join("; ")
                        break
                    }
                    if (batch.isEos()) {
                        break
                    }
                    def reader = new ArrowStreamReader(new ByteArrayInputStream(batch.getRows()), allocator)
                    try {
                        while (reader.loadNextBatch()) {
                            rows += reader.getVectorSchemaRoot().getRowCount()
                        }
                    } finally {
                        reader.close()
                    }
                }
            } finally {
                client.closeScanner(new TScanCloseParams().setContextId(opened.getContextId()))
            }
            return [rows, error]
        } finally {
            allocator.close()
            socket.close()
        }
    }

    // Values the convertor accepts are read in full and the scan ends normally.
    def (validRows, validError) = scan("""SELECT id, unhex(encoded), CAST(event_time AS DATETIME(6))
            FROM ${db}.query_plan_scan_input WHERE id = 1""")
    assertEquals(1L, validRows)
    assertNull(validError)

    // Invalid UTF-8 in a STRING result.
    def (utf8Rows, utf8Error) = scan("SELECT id, unhex(encoded) FROM ${db}.query_plan_scan_input")
    assertTrue(utf8Error != null && utf8Error.contains("Invalid UTF8"),
            "Expected an invalid UTF-8 scan error, read ${utf8Rows} rows, error: ${utf8Error}")

    // A DATETIME before year 1.
    def (timeRows, timeError) = scan("""SELECT id, CAST(event_time AS DATETIME(6))
            FROM ${db}.query_plan_scan_input""")
    assertTrue(timeError != null && timeError.contains("outside the supported 0001-9999 range"),
            "Expected an out-of-range timestamp scan error, read ${timeRows} rows, error: ${timeError}")
}
