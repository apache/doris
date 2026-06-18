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

// When a query only needs a column's length (length(v), or v='' which is rewritten to
// length=0), a dict-encoded varchar column is read in OFFSET_ONLY mode, which only
// materializes lengths and leaves the char content empty. If that same column also
// carries a DELETE condition, the storage-side delete predicate compares the empty
// content, matches nothing, and the deleted rows leak back into the scan output.
suite("test_offset_only_delete_leak", "p0") {
    sql "drop table if exists test_offset_only_delete_leak"
    sql """
        create table test_offset_only_delete_leak (
            k int,
            v varchar(50)
        ) duplicate key(k)
        distributed by hash(k) buckets 1
        properties("replication_num" = "1")
    """

    // low cardinality -> the column gets dictionary encoded
    sql """
        insert into test_offset_only_delete_leak values
        (1,'--'),(2,'aa'),(3,'--'),(4,'bb'),(5,''),
        (6,'aa'),(7,'--'),(8,'cc'),(9,''),(10,'aa')
    """

    // delete the three '--' rows (k = 1,3,7); kept as a delete predicate applied at read time.
    sql "delete from test_offset_only_delete_leak where v = '--'"

    // plain count does not read v.
    qt_count "select count(*) from test_offset_only_delete_leak"

    // length(v) only needs v's length -> OFFSET_ONLY read; the delete on v must still apply,
    // so the deleted '--' rows must not be counted.
    qt_length_ge_0 "select count(*) from test_offset_only_delete_leak where length(v) >= 0"
    qt_length_gt_0 "select count(*) from test_offset_only_delete_leak where length(v) > 0"

    // v = '' is rewritten to length(v) = 0 -> also OFFSET_ONLY.
    qt_empty_string "select count(*) from test_offset_only_delete_leak where v = ''"

    // outputting v forces a full read; the deleted rows must never appear either way.
    order_qt_full_read "select k, v from test_offset_only_delete_leak where length(v) >= 0 order by k, v"
}
