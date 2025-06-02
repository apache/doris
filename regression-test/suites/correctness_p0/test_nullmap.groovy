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

suite("test_nullmap", "query") {
    sql """
    drop table if exists tblx;
    create table tblx(k1 int, k2 string) distributed by hash(k1) buckets 1 properties("replication_num" = "1");
    insert into tblx values(1, null), (2, "2022-10-10"), (3, "2023-10-10 11:00:11"), (4, "abc");
    """
    qt_sql """select k2, unix_timestamp(k2, 'yyyy-MM-dd HH:mm:ss'), k21, unix_timestamp(k21, 'yyyy-MM-dd HH:mm:ss')
    from (select k2, from_unixtime(unix_timestamp(k2, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as k21 from tblx) y
    order by 1,2,3,4"""
}