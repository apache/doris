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

suite("left_anti_with_other") {
    sql """ DROP TABLE IF EXISTS lhs_table; """
    sql """ DROP TABLE IF EXISTS rhs_table; """
    sql """
create table lhs_table (
	k1 int not null,
	k2 date not null
)
duplicate key (k1)
distributed BY hash(k1) buckets 3
properties("replication_num" = "1");
        """

    sql """
create table rhs_table (
	k1 int not null,
	k2 date not null
)
duplicate key (k1)
distributed BY hash(k1) buckets 3
properties("replication_num" = "1");
        """

    sql "insert into lhs_table values(1, '2025-06-01');"

    sql "set disable_join_reorder=true;"

    qt_test """select * from lhs_table left anti join rhs_table on lhs_table.k1 = rhs_table.k1 and MONTHS_ADD(rhs_table.k2, 2) >= lhs_table.k2;"""
}
