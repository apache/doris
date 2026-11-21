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

// A statement that throws before it executes must fail, over MySQL and over Arrow Flight SQL. It used to be
// answered as a success: an OK packet over MySQL, and over Arrow Flight a SELECT got the one-row
// StatusResult table meant for SET/USE instead of its rows.
suite("test_statement_failed_before_execution", "nonConcurrent") {
    def debugPoint = "ConnectProcessor.executeQuery.failBeforeExecution"
    def marker = "pre_exec_failure_marker"
    def originalConnection = context.threadLocalConn.get()
    try {
        GetDebugPoint().enableDebugPointForAllFEs(debugPoint, [stmt_contains: marker])

        // MySQL protocol.
        test {
            sql "SELECT 1 AS ${marker}"
            exception "debug point ${debugPoint}"
        }
        // A statement without the marker is not affected.
        order_qt_mysql_unaffected "SELECT 2 AS unaffected"

        // Arrow Flight SQL. TestAction uses getConnection(), so run it on the suite's Flight connection.
        context.getArrowFlightSqlConnection()
        context.threadLocalConn.set(context.threadArrowFlightSqlConn.get())
        test {
            sql "SELECT 1 AS ${marker}"
            exception "debug point ${debugPoint}"
        }
        order_qt_flight_unaffected "SELECT 2 AS unaffected"
    } finally {
        if (originalConnection == null) {
            context.threadLocalConn.remove()
        } else {
            context.threadLocalConn.set(originalConnection)
        }
        GetDebugPoint().disableDebugPointForAllFEs(debugPoint)
    }
}
