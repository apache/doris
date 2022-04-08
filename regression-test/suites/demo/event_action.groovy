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

suite("event_action", "demo") {
    def createTable = { tableName ->
        sql """
            create table ${tableName}
            (id int)
            distributed by hash(id)
            properties
            (
              "replication_num"="1"
            )
            """
    }

    def tableName = "test_events_table1"
    createTable(tableName)

    // lazy drop table when execute this suite finished
    onFinish {
        try_sql "drop table if exists ${tableName}"
    }



    // all event: success, fail, finish
    // and you can listen event multiple times

    onSuccess {
        try_sql "drop table if exists ${tableName}"
    }

    onSuccess {
        try_sql "drop table if exists ${tableName}_not_exist"
    }

    onFail {
        try_sql "drop table if exists ${tableName}"
    }

    onFail {
        try_sql "drop table if exists ${tableName}_not_exist"
    }
}
