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

suite("cast_ignore") {
    sql "drop table if exists tdate"
    sql """
    create table tdate(
        k1 int,
        kdate date,
        kdatetime datetime
) distributed by hash (k1) buckets 1
properties ("replication_num"="1");
    """
    sql """
insert into tdate values(1,'2023-10-01','2023-10-01 01:00:00'),
(2,'2023-10-02','2023-10-02 01:00:00'),
(3,'2023-10-03','2023-10-03 01:00:00');
"""
    sql "set disable_nereids_expression_rules='SIMPLIFY_COMPARISON_PREDICATE';"

    qt_test "select k1,kdate,kdatetime from tdate where cast(cast(kdatetime as date) as datetime)='2023-10-01';"
    qt_test "select k1,kdate,kdatetime from tdate where kdatetime='2023-10-01';"
}
