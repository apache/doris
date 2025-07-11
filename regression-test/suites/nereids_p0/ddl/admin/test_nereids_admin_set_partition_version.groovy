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

suite("test_nereids_admin_set_partition_version") {
    sql "drop table if exists test_nereids_admin_set_partition_version"
    sql """
        CREATE TABLE IF NOT EXISTS test_nereids_admin_set_partition_version (
                    k1 INT,
                    v1 INT,
                    v2 varchar(20)
                )
        DUPLICATE KEY (k1)
        PARTITION BY LIST (`v2`)
        (
            PARTITION `p_huabei` VALUES IN ("beijing", "tianjin")
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 1 properties("replication_num" = "1");
    """

    def res = sql """show partitions from test_nereids_admin_set_partition_version"""

    sql """
        ADMIN SET TABLE __internal_schema.audit_log PARTITION VERSION PROPERTIES(\"partition_id\" = \"10075\", \"visible_version\" = \"100\"
    """
}
