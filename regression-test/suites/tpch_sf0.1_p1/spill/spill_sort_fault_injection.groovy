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

suite("spill_sort_fault_injection", "nonConcurrent") {
    multi_sql """
    use regression_test_tpch_unique_sql_zstd_bucket1_p0;
    set enable_force_spill=true;
    set min_revocable_mem=1024;
    """
    def test_sql = """
    select
    l_orderkey,
    l_linenumber,
    l_partkey,
    l_suppkey,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_commitdate,
    l_receiptdate,
    l_shipinstruct,
    l_shipmode,
    l_shipdate
from
    lineitem
order by
    l_orderkey,
    l_linenumber,
    l_partkey,
    l_suppkey,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_commitdate,
    l_receiptdate,
    l_shipinstruct,
    l_shipmode;
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
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::spill_sort_sink::sink")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject spill_sort_sink sink failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::spill_sort_sink::sink")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::spill_sort_sink::revoke_memory_cancel")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject spill_sort_sink revoke_memory canceled"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::spill_sort_sink::revoke_memory_cancel")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::spill_sort_sink::revoke_memory_submit_func")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject spill_sort_sink revoke_memory submit_func failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::spill_sort_sink::revoke_memory_submit_func")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::spill_sort_source::recover_spill_data")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject spill_sort_source recover_spill_data failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::spill_sort_source::recover_spill_data")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::spill_sort_source::spill_merged_data")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject spill_sort_source spill_merged_data failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::spill_sort_source::spill_merged_data")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("fault_inject::spill_sort_source::spill_merged_data")
        sql test_sql
    } catch(Exception e) {
        log.error(e.getMessage())
        assertTrue(e.getMessage().contains("fault_inject spill_sort_source spill_merged_data failed"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("fault_inject::spill_sort_source::spill_merged_data")
    }
}