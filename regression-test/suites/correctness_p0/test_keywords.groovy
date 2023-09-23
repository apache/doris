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

// Test some keywords that may conflict.
// For example, "bin" is used for function "bin",
// and also used "show catalog recycle bin"
suite("test_keywords") {
    def table = "test_keywords"
    sql """
        drop table if exists $table
    """
    
    sql """
        create table $table ( k1 int, k2 varchar(1024) )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into $table values(1, "abc"), (2, "xyz");
    """

    sql "sync"
    order_qt_select """
        select bin(k1) from $table
    """
}
