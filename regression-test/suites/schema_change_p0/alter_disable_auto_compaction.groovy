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

suite("test_alter_property_mow") {
    def tableName = "test_alter_property_mow"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            k1 INT,
            k2 int,
            v1 bigint,
            v2 INT,
            v3 varchar(32) null,
        )
        UNIQUE KEY (k1, k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 1
        PROPERTIES (
        'replication_num' = '1'
        )
    """

    sql """
        ALTER TABLE ${tableName} SET ('disable_auto_compaction'='true');
    """

    sql """
        ALTER TABLE ${tableName} SET ('disable_auto_compaction'='false');
    """

    sql """
        insert into ${tableName} (k1, k2, v1, v2, v3) values(2, 2, 3, 4, 'a')
    """

    sql """
        insert into ${tableName} (k1, k2, v1, v2, v3) values(1, 2, 3, 4, 'a')
    """

    sql """
        insert into ${tableName} (k1, k2, v1, v2, v3) values(1, 2, 3, 4, 'a')
    """

    sql """
        select * from ${tableName}
    """
}

