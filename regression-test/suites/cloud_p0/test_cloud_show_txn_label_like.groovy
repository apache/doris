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

suite("test_cloud_show_txn_label_like") {
    if (!isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS test_cloud_show_txn_label_like"
    sql """
        CREATE TABLE test_cloud_show_txn_label_like (id INT)
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    def dbName = context.config.getDbNameByFile(context.file)
    def labelPrefix = "show_txn_" + UUID.randomUUID().toString().replace("-", "") + "_"
    def labels = [labelPrefix + "first_done", labelPrefix + "second_done", labelPrefix + "other"]
    labels.eachWithIndex { loadLabel, index ->
        streamLoad {
            table "test_cloud_show_txn_label_like"
            set "label", loadLabel
            inputText "${index + 1}\n"
            time 10000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                assertEquals("Success", parseJson(result).Status)
            }
        }
    }

    def expected = [labels[1], labels[0]]
    for (def pattern : ["${labelPrefix}%done", "%${labelPrefix}%done"]) {
        def rows = sql_return_maparray "SHOW TRANSACTION FROM ${dbName} WHERE LABEL LIKE '${pattern}'"
        assertEquals(expected, rows.collect { it.Label })
    }
}
