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

suite('test_wal_mem_back_pressure_fault_injection', 'nonConcurrent') {
    def tableName = 'test_wal_mem_back_pressure_fault_injection'
    sql """ DROP TABLE IF EXISTS ${tableName}1 """
    sql """
            CREATE TABLE ${tableName}1 (
                k bigint,
                v string
                )
                UNIQUE KEY(k)
                DISTRIBUTED BY HASH (k) BUCKETS 32
                PROPERTIES(
                "replication_num" = "1"
            );
    """

    GetDebugPoint().enableDebugPointForAllBEs('LoadBlockQueue.add_block.block_queues_meme_size_too_large')
    GetDebugPoint().enableDebugPointForAllBEs('GroupCommitTable._create_group_commit_load.create_load_filed')
    for (int i = 0; i < 5; i++) {
        try {
            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'compress_type', 'GZ'
                set 'format', 'csv'
                set 'group_commit', 'async_mode'
                unset 'label'

                file 'test_wal_mem_back_pressure_fault_injection.csv.gz'
                time 100000
            }
        } catch (Exception e) {
            logger.info(e.getMessage())
            assertTrue(e.getMessage().contains('Communications link failure'))
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs('LoadBlockQueue.add_block.block_queues_meme_size_too_large')
            GetDebugPoint().disableDebugPointForAllBEs('GroupCommitTable._create_group_commit_load.create_load_filed')
        }
    }

    GetDebugPoint().enableDebugPointForAllBEs('LoadBlockQueue.add_block.block_queues_meme_size_too_large')
    GetDebugPoint().enableDebugPointForAllBEs('GroupCommitTable._exec_plan_fragment.exec_plan_fragment_fail')
    for (int i = 0; i < 5; i++) {
        try {
            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'compress_type', 'GZ'
                set 'format', 'csv'
                set 'group_commit', 'async_mode'
                unset 'label'

                file 'test_wal_mem_back_pressure_fault_injection.csv.gz'
                time 100000
            }
        } catch (Exception e) {
            logger.info(e.getMessage())
            assertTrue(e.getMessage().contains('Communications link failure'))
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs('LoadBlockQueue.add_block.block_queues_meme_size_too_large')
            GetDebugPoint().disableDebugPointForAllBEs('GroupCommitTable._exec_plan_fragment.exec_plan_fragment_fail')
        }
    }
}
