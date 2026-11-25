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

suite("test_bitmap_nullable_element", "p0") {
    // Two rows are required: one hidden -1 payload and one non-NULL element in the same block.
    sql "drop table if exists test_bitmap_nullable_element"
    sql """create table test_bitmap_nullable_element (k bigint not null)
        duplicate key(k) distributed by hash(k) buckets 1
        properties ("replication_num" = "1")"""
    sql "insert into test_bitmap_nullable_element values (-1), (1)"

    // NULLIF retains k below its null map. Both functions must ignore that hidden -1.
    order_qt_contains """select k, bitmap_contains(to_bitmap(1), nullif(k, -1))
        from test_bitmap_nullable_element"""
    order_qt_remove """select k, bitmap_to_string(bitmap_remove(to_bitmap(1), nullif(k, -1)))
        from test_bitmap_nullable_element"""
}
