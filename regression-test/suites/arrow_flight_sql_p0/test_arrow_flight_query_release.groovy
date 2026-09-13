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

// Regression for https://github.com/apache/doris/issues/67503
//
// Over Arrow Flight SQL a query runs in two phases: GetFlightInfo (plan and start it on the BE)
// and DoGet (the client pulls the results from the BE). Only an external-table scan in batch mode
// needs its FE coordinator after GetFlightInfo (#62259). Every other query has to release its
// coordinator, and with it the workload group queue slot and the active_queries entry, at the end
// of GetFlightInfo: most Flight clients never close their session, so a coordinator that waited
// for the session's next query kept one queue slot per finished query until wait_timeout.
//
// The framework's Flight session behaves like such a client: it is reused across statements and
// never closed.
suite("test_arrow_flight_query_release", "arrow_flight_sql") {
    def tableName = "test_arrow_flight_query_release_tbl"
    def wgName = "test_arrow_flight_query_release_wg"

    def forComputeGroupStr = ""
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        forComputeGroupStr = " for ${clusters[0][0]} "
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (id int, name varchar(20))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """
    sql "INSERT INTO ${tableName} VALUES (1, 'a'), (2, 'b'), (3, 'c')"

    sql "ADMIN SET FRONTEND CONFIG ('enable_workload_group' = 'true')"
    sql "DROP WORKLOAD GROUP IF EXISTS ${wgName} ${forComputeGroupStr}"
    // One running query at a time and no waiting queue: while a query still holds the slot, the
    // next scanning query in the group fails at once with "query waiting queue is full".
    sql """
        CREATE WORKLOAD GROUP ${wgName} ${forComputeGroupStr}
        PROPERTIES ('max_concurrency' = '1', 'max_queue_size' = '0', 'queue_timeout' = '0')
        """
    try {
        // The Flight session is a session of its own, so it is bound to the group separately.
        sql "SET workload_group = '${wgName}'"
        arrow_flight_sql "SET workload_group = '${wgName}'"

        // A scanning query over Arrow Flight SQL. The session stays open afterwards.
        def flightRows = arrow_flight_sql "SELECT id, name FROM ${tableName} ORDER BY id"
        assertEquals(3, flightRows.size())

        // Its coordinator was released at the end of GetFlightInfo, so the query is gone from
        // active_queries. The LIKE pattern is assembled with CONCAT so that this statement's own
        // text does not match it.
        def registered = sql """
            SELECT QUERY_ID, SQL FROM information_schema.active_queries
            WHERE SQL LIKE CONCAT('%FROM ${tableName}', ' ORDER BY id%')
            """
        assertTrue(registered.isEmpty(), "finished Arrow Flight query is still registered: ${registered}")

        // ... and its queue slot is free again: a scanning query in the same group runs instead
        // of failing with "query waiting queue is full".
        def mysqlRows = sql "SELECT id FROM ${tableName} ORDER BY id"
        assertEquals(3, mysqlRows.size())
    } finally {
        sql "SET workload_group = 'normal'"
        try {
            arrow_flight_sql "SET workload_group = 'normal'"
        } catch (Throwable ignore) {
            // best effort: the Flight session must not keep pointing at the dropped group
        }
        sql "DROP WORKLOAD GROUP IF EXISTS ${wgName} ${forComputeGroupStr}"
        sql "DROP TABLE IF EXISTS ${tableName}"
    }
}
