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

suite("partitioned_hash_join_fault_injection", "p2, nonConcurrent") {
    multi_sql """
    use regression_test_tpch_sf1_p2;
    set enable_force_spill=true;
    set min_revocable_mem=1024;
    """
    def test_sql = """
    SELECT
      L_ORDERKEY,
      L_COMMENT,
      O_ORDERKEY,
      L_QUANTITY,
      L_SHIPINSTRUCT
    FROM
        lineitem, 
        orders
    WHERE
      L_ORDERKEY = o_orderkey
    """
    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::spill_stream::spill_block")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject spill_stream spill_block failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::spill_stream::spill_block")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::spill_stream::prepare_spill")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject spill_stream prepare_spill failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::spill_stream::prepare_spill")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::spill_stream::spill_eof")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject spill_stream spill_eof failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::spill_stream::spill_eof")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::spill_stream::read_next_block")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject spill_stream read_next_block failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::spill_stream::read_next_block")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::partitioned_hash_join_sink::revoke_unpartitioned_block_submit_func")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject partitioned_hash_join_sink revoke_unpartitioned_block submit_func failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::partitioned_hash_join_sink::revoke_unpartitioned_block_submit_func")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::partitioned_hash_join_sink::revoke_memory_submit_func")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject partitioned_hash_join_sink revoke_memory submit_func failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::partitioned_hash_join_sink::revoke_memory_submit_func")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::partitioned_hash_join_sink::revoke_memory_cancel")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject partitioned_hash_join_sink revoke_memory canceled"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::partitioned_hash_join_sink::revoke_memory_cancel")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::partitioned_hash_join_sink::sink")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject partitioned_hash_join_sink sink failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::partitioned_hash_join_sink::sink")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::spill_probe_blocks")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject partitioned_hash_join_probe spill_probe_blocks failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::spill_probe_blocks")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::spill_probe_blocks_cancel")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject partitioned_hash_join_probe spill_probe_blocks canceled"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::spill_probe_blocks_cancel")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::spill_probe_blocks_submit_func")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject partitioned_hash_join_probe spill_probe_blocks submit_func failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::spill_probe_blocks_submit_func")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::recover_build_blocks")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject partitioned_hash_join_probe recover_build_blocks failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::recover_build_blocks")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::recover_build_blocks_cancel")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject partitioned_hash_join_probe recover_build_blocks canceled"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::recover_build_blocks_cancel")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::recovery_build_blocks_submit_func")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject partitioned_hash_join_probe recovery_build_blocks submit_func failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::recovery_build_blocks_submit_func")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::recover_probe_blocks")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject partitioned_hash_join_probe recover_probe_blocks failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::recover_probe_blocks")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::recover_probe_blocks_cancel")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject partitioned_hash_join_probe recover_probe_blocks canceled"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::recover_probe_blocks_cancel")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::recovery_probe_blocks_submit_func")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject partitioned_hash_join_probe recovery_probe_blocks submit_func failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::recovery_probe_blocks_submit_func")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::sink")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject partitioned_hash_join_probe sink failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::partitioned_hash_join_probe::sink")
    }
}