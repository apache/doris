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

suite("test_streamload_perfomance", "performance") {
    def tableName = "test_streamload_performance1"

    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id int,
            name varchar(255)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        def rowCount = 10000
        def rowIt = java.util.stream.LongStream.range(0, rowCount)
                .mapToObj({i -> [i, "a_" + i]})
                .iterator()

        streamLoad {
            table tableName
            time 5000
            inputIterator rowIt
        }
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName}"
    }
}
