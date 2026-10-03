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

suite("test_colocate_mapping_constraint_being_synced") {
    sql "DROP TABLE IF EXISTS test_colocate_mapping_constraint_being_synced"
    sql """
        CREATE TABLE test_colocate_mapping_constraint_being_synced (
            k1 INT,
            d1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 4
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        ALTER TABLE test_colocate_mapping_constraint_being_synced
        ADD CONSTRAINT mapping_before_sync
        COLOCATE MAPPING being_synced_mapping (d1)
        DETERMINES DISTRIBUTION KEY (k1) NOT ENFORCED
    """
    sql "INSERT INTO test_colocate_mapping_constraint_being_synced VALUES (1, 1)"
    sql "SYNC"

    def selfJoinSql = """
        SELECT /*+ SET_VAR(disable_join_reorder=true,
                           enable_colocate_mapping_constraint=true,
                           auto_broadcast_join_threshold=-1,
                           broadcast_row_count_limit=0) */ l.d1
        FROM test_colocate_mapping_constraint_being_synced l
        JOIN test_colocate_mapping_constraint_being_synced r ON l.d1 = r.d1
    """
    explain {
        sql selfJoinSql
        contains "COLOCATE"
    }

    sql """
        ALTER TABLE test_colocate_mapping_constraint_being_synced
        SET ("is_being_synced" = "true")
    """
    explain {
        sql selfJoinSql
        notContains "COLOCATE"
    }
    test {
        sql """
            ALTER TABLE test_colocate_mapping_constraint_being_synced
            ADD CONSTRAINT mapping_during_sync
            COLOCATE MAPPING being_synced_mapping_2 (d1)
            DETERMINES DISTRIBUTION KEY (k1) NOT ENFORCED
        """
        exception "being synchronized by CCR"
    }

    sql """
        ALTER TABLE test_colocate_mapping_constraint_being_synced
        DROP CONSTRAINT mapping_before_sync
    """
}
