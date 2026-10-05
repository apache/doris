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

suite("query") {
    def tableName = "test_snappy"
    order_qt_sql1 "select * from ${tableName} order by k1, k2"
    tableName = "test_LZ4"
    order_qt_sql2 "select * from ${tableName} order by k1, k2"
    tableName = "test_LZ4F"
    order_qt_sql3 "select * from ${tableName} order by k1, k2"
    tableName = "test_LZ4HC"
    order_qt_sql4 "select * from ${tableName} order by k1, k2"
    tableName = "test_ZLIB"
    order_qt_sql5 "select * from ${tableName} order by k1, k2"
    tableName = "test_ZSTD"
    order_qt_sql6 "select * from ${tableName} order by k1, k2"
}