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

suite("test_stream_load_send_batch_parallelism", "p0") {
    sql "DROP TABLE IF EXISTS test_stream_load_send_batch_parallelism"
    sql """
        CREATE TABLE test_stream_load_send_batch_parallelism (id INT)
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    // Both writer versions must validate the HTTP override at the FE planning boundary.
    for (def memtableOnSinkNode : ["false", "true"]) {
        for (def parallelism : [null, "-2147483648", "-1", "0", "1", "256", "257", "2147483647"]) {
            streamLoad {
                table "test_stream_load_send_batch_parallelism"
                set "memtable_on_sink_node", memtableOnSinkNode
                if (parallelism != null) {
                    set "send_batch_parallelism", parallelism
                }
                inputText "1\n"
                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    def json = parseJson(result)
                    def limitError = "send_batch_parallelism value should less than or equal 256, " +
                            "you set value is: ${parallelism}"
                    "qt_${memtableOnSinkNode}_${parallelism ?: 'default'}"(
                            "SELECT '${json.Status}', ${json.NumberLoadedRows}, ${json.Message.contains(limitError)}")
                }
            }
        }
    }
    sql "sync"
    qt_loaded_rows "SELECT count(*) FROM test_stream_load_send_batch_parallelism"
}
