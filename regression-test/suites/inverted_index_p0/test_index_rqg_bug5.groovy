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

suite("test_index_rqg_bug5", "test_index_rqg_bug"){
    def table = "test_index_rqg_bug5"
    sql "drop table if exists ${table}"

    sql """
      create table ${table} (
        pk int,
        col1 int not null,
        col2 bigint not null,
        INDEX col1_idx (`col1`) USING INVERTED,
        INDEX col2_idx (`col2`) USING INVERTED
      ) engine=olap
      DUPLICATE KEY(pk, col1)
      distributed by hash(pk)
      properties("replication_num" = "1");;
    """

    sql """ insert into ${table} values (10, 20, 30); """

    qt_sql """ select count() from ${table} where col2 + col1 > 20 or col1 > 20; """
}
