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

suite("test_show_partitions") {
    sql "drop table if exists test_show_part"
    sql """
        create table test_show_part (
            month varchar (255),
            id int,
            code varchar (255),
            name varchar (255)
        ) ENGINE = OLAP DUPLICATE KEY(month, id)
        AUTO PARTITION BY LIST (month)()
        distributed by hash (id) buckets auto
        PROPERTIES(
        "replication_allocation" = "tag.location.default: 1"
        )
    """
    sql """ insert into test_show_part(month, id, code, name) values ('2024-12', 10001, 'test10001', 'test') """
    test {
        sql "show partitions from test_show_part where PartitionName = auto_partition_name('list','2024-12')"
        exception "Not Supported. Use `select * from partitions(...)` instead"
    }
    sql " show partitions from test_show_part where PartitionName = '123'; "
}