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

suite("test_flight_query_error_after_cancel", "arrow_flight_sql") {
    def originalConnection = context.threadLocalConn.get()
    context.getArrowFlightSqlConnection()
    // TestAction uses getConnection() even when the suite runs in the Flight group.
    // Reuse the owned Flight connection so exception assertions always exercise Flight.
    context.threadLocalConn.set(context.threadArrowFlightSqlConn.get())
    try {
        for (def parallelResultSink : [false, true]) {
            sql "SET enable_parallel_result_sink=${parallelResultSink}"
            for (int iteration = 0; iteration < 20; ++iteration) {
                for (def quantile : [-1, 3000]) {
                    test {
                        sql "SELECT percentile(number, ${quantile}) FROM numbers(\"number\"=\"1000\")"
                        exception "quantile in func percentile should in [0, 1], but real data is:${quantile}"
                    }
                }
                // A failed query must not poison the connection's next BE result stream.
                sql "SELECT count(*) FROM numbers(\"number\"=\"1000\")"
            }
        }
    } finally {
        if (originalConnection == null) {
            context.threadLocalConn.remove()
        } else {
            context.threadLocalConn.set(originalConnection)
        }
    }
}
