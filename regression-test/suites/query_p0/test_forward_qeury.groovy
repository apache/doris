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
import org.apache.doris.regression.util.NodeType

suite("test_forward_query") {
    def options = new ClusterOptions()
    options.enableDebugPoints()
    options.setFeNum(2)
    options.connectToFollower = true

    docker(options) {
        def tbl = "test_forward_query"
        sql """ DROP TABLE IF EXISTS ${tbl} """
        sql """
            CREATE TABLE ${tbl}
            (
                k1 int
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
               "replication_num" = "1"
            );
        """

        sql """ INSERT INTO ${tbl} VALUES(1);"""

        cluster.injectDebugPoints(NodeType.FE, ['StmtExecutor.forward_all_queries' : [forwardAllQueries:true]])

        try {
            sql """ SELECT * FROM ${tbl} """
        } catch (Exception ignored) {
            assertTrue(false)
        }
    }
}