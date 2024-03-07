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

suite("check_time_type", "p0") {
    def tableName = "check_time_type"

    sql """
        drop table IF exists ${tableName}
    """

    sql """
        CREATE TABLE ${tableName}
        (
            k1 INT,
            k2 int,
            v1 INT,
            v2 INT,
            v3 varchar(32)
        )
        UNIQUE KEY (k1, k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 1
        PROPERTIES (
            'light_schema_change' = 'true',
            'replication_num' = '1'
        )
    """

    sql """
        insert into ${tableName} values (1, 2, 3, 4, 'a');
    """

    test {
        sql """ alter table ${tableName} add column new_col time """
        exception "errCode = 2, detailMessage = Time type is not supported for olap table"
    }

    sql """
        select * from ${tableName}
    """
}

