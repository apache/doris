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

suite("test_decimalv3_key") {

    sql "drop table if exists d_table"

    sql """
    create table d_table (
      k1 decimal null,
      k2 decimal not null
    )
    duplicate key (k1)
    distributed BY hash(k1) buckets 3
    properties("replication_num" = "1");
    """

    sql """
    insert into d_table values(999999999,999999999);
    """

    qt_test "select count(*) from d_table;"
}
