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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.NodeType

suite("insert_close_wait_timeout", 'docker') {
    def options = new ClusterOptions()
    options.setBeNum(3)
    options.enableDebugPoints()
    docker(options) {
        // ---------- test restart fe ----------
        def result = sql 'SELECT DATABASE()'
        def dbName = result[0][0]
        def tableName = 'tbl_insert_close_wait_timeout'

        sql """ CREATE TABLE ${tableName} (k1 INT, k2 INT) DISTRIBUTED BY HASH(k1) BUCKETS 10 PROPERTIES ("replication_num" = "3") """
        sql """ SET insert_timeout = 5 """

        // inject close wait timeout (stream) to one backend
        cluster.injectDebugPoints(NodeType.BE_ANY_ONE, ['LoadStreamStub::close_wait.long_wait':null])
        expectExceptionLike({
            sql """ INSERT INTO tbl_1 VALUES (1, 11) (2, 12) (3, 13) (4, 14) (5, 15) (6, 16) (7, 17) """
        }, "stream close_wait timeout")
        cluster.clearBackendDebugPoints()

        // inject close wait timeout (sinkv2) to one backend
        cluster.injectDebugPoints(NodeType.BE_ANY_ONE, ['VTabletWriterV2._close_wait.load_timeout':null])
        expectExceptionLike({
            sql """ INSERT INTO tbl_1 VALUES (1, 11) (2, 12) (3, 13) (4, 14) (5, 15) (6, 16) (7, 17) """
        }, "load timed out before close waiting")
        cluster.clearBackendDebugPoints()
    }
}
