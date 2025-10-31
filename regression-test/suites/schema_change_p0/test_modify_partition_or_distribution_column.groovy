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

suite("test_modify_partition_or_distribution_column") {
    def tblName = "test_modify_partition_or_distribution_column"
    sql """ DROP TABLE IF EXISTS ${tblName} """
    sql """
       CREATE TABLE IF NOT EXISTS ${tblName}
        (
            k1 DATE,
            k2 INT DEFAULT '10',
            k3 bigint,
            k4 VARCHAR(32) DEFAULT '',
            v1 BIGINT DEFAULT '0'
        )
        UNIQUE  KEY(k1, k2, k3, k4)
        PARTITION BY RANGE(`k2`)
                (
                    partition `old_p1` values [("1"), ("2")),
                    partition `old_p2` values [("2"), ("3"))
                )
        DISTRIBUTED BY HASH(k3, k4) BUCKETS 5
        PROPERTIES("replication_num" = "1", "light_schema_change" = "true"); 
    """

    test {
        sql """ ALTER TABLE ${tblName} MODIFY COLUMN k2 INT NOT NULL """
        exception "Can not modify partition or distribution column"
    }

    test {
        sql """ ALTER TABLE ${tblName} MODIFY COLUMN k3 largeint """
        exception "Can not modify partition or distribution column"
    }
}
