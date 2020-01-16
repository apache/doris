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

#include "exec/partitioned_hash_table.inline.h"

#include <boost/scoped_ptr.hpp>

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <vector>

#include <gtest/gtest.h>

#include "common/compiler_util.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/slot_ref.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/string_value.h"
#include "runtime/test_env.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/runtime_profile.h"

using std::vector;
using std::map;

using boost::scoped_ptr;

namespace doris {

class PartitionedHashTableTest : public testing::Test {
public:
    PartitionedHashTableTest() : _limit(-1), _mem_pool(&_limit) {}
    ~PartitionedHashTableTest() {}

protected:
    scoped_ptr<TestEnv> _test_env;
    RuntimeState* _runtime_state;
    ObjectPool _pool;
    MemTracker _tracker;
    MemTracker _limit;
    MemPool _mem_pool;
    vector<ExprContext*> _build_expr_ctxs;
    vector<ExprContext*> _probe_expr_ctxs;

    virtual void SetUp() {
        _test_env.reset(new TestEnv());

        RowDescriptor desc;
        Status status;

        // Not very easy to test complex tuple layouts so this test will use the
        // simplest.  The purpose of these tests is to exercise the hash map
        // internals so a simple build/probe expr is fine.
        Expr* expr = _pool.add(new SlotRef(TYPE_INT, 0));
        _build_expr_ctxs.push_back(_pool.add(new ExprContext(expr)));
        status = Expr::prepare(_build_expr_ctxs, NULL, desc, &_tracker);
        EXPECT_TRUE(status.ok());
        status = Expr::open(_build_expr_ctxs, NULL);
        EXPECT_TRUE(status.ok());

        expr = _pool.add(new SlotRef(TYPE_INT, 0));
        _probe_expr_ctxs.push_back(_pool.add(new ExprContext(expr)));
        status = Expr::prepare(_probe_expr_ctxs, NULL, desc, &_tracker);
        EXPECT_TRUE(status.ok());
        status = Expr::open(_probe_expr_ctxs, NULL);
        EXPECT_TRUE(status.ok());
    }

    virtual void TearDown() {
        Expr::close(_build_expr_ctxs, NULL);
        Expr::close(_probe_expr_ctxs, NULL);
        _runtime_state = NULL;
        _test_env.reset();
        _mem_pool.free_all();
    }

    TupleRow* CreateTupleRow(int32_t val) {
        uint8_t* tuple_row_mem = _mem_pool.allocate(sizeof(int32_t*));
        Tuple* tuple_mem = Tuple::create(sizeof(int32_t), &_mem_pool);
        *reinterpret_cast<int32_t*>(tuple_mem) = val;
        TupleRow* row = reinterpret_cast<TupleRow*>(tuple_row_mem);
        row->set_tuple(0, tuple_mem);
        return row;
    }

    // Wrapper to call private methods on PartitionedHashTable
    // TODO: understand google testing, there must be a more natural way to do this
    void ResizeTable(
            PartitionedHashTable* table, int64_t new_size, PartitionedHashTableCtx* ht_ctx) {
        table->resize_buckets(new_size, ht_ctx);
    }

    // Do a full table scan on table.  All values should be between [min,max).  If
    // all_unique, then each key(int value) should only appear once.  Results are
    // stored in results, indexed by the key.  Results must have been preallocated to
    // be at least max size.
    void FullScan(PartitionedHashTable* table, PartitionedHashTableCtx* ht_ctx, int min, int max,
            bool all_unique, TupleRow** results, TupleRow** expected) {
        PartitionedHashTable::Iterator iter = table->begin(ht_ctx);
        while (!iter.at_end()) {
            TupleRow* row = iter.get_row();
            int32_t val = *reinterpret_cast<int32_t*>(_build_expr_ctxs[0]->get_value(row));
            EXPECT_GE(val, min);
            EXPECT_LT(val, max);
            if (all_unique) {
                EXPECT_TRUE(results[val] == NULL);
            }
            EXPECT_EQ(row->get_tuple(0), expected[val]->get_tuple(0));
            results[val] = row;
            iter.next();
        }
    }

    // Validate that probe_row evaluates overs probe_exprs is equal to build_row
    // evaluated over build_exprs
    void ValidateMatch(TupleRow* probe_row, TupleRow* build_row) {
        EXPECT_TRUE(probe_row != build_row);
        int32_t build_val =
            *reinterpret_cast<int32_t*>(_build_expr_ctxs[0]->get_value(probe_row));
        int32_t probe_val =
            *reinterpret_cast<int32_t*>(_probe_expr_ctxs[0]->get_value(build_row));
        EXPECT_EQ(build_val, probe_val);
    }

    struct ProbeTestData {
        TupleRow* probe_row;
        vector<TupleRow*> expected_build_rows;
    };

    void ProbeTest(PartitionedHashTable* table, PartitionedHashTableCtx* ht_ctx,
            ProbeTestData* data, int num_data, bool scan) {
        uint32_t hash = 0;
        for (int i = 0; i < num_data; ++i) {
            TupleRow* row = data[i].probe_row;

            PartitionedHashTable::Iterator iter;
            if (ht_ctx->eval_and_hash_probe(row, &hash)) {
                continue;
            }
            iter = table->find(ht_ctx, hash);

            if (data[i].expected_build_rows.size() == 0) {
                EXPECT_TRUE(iter.at_end());
            } else {
                if (scan) {
                    map<TupleRow*, bool> matched;
                    while (!iter.at_end()) {
                        EXPECT_EQ(matched.find(iter.get_row()), matched.end());
                        matched[iter.get_row()] = true;
                        iter.next();
                    }
                    EXPECT_EQ(matched.size(), data[i].expected_build_rows.size());
                    for (int j = 0; i < data[j].expected_build_rows.size(); ++j) {
                        EXPECT_TRUE(matched[data[i].expected_build_rows[j]]);
                    }
                } else {
                    EXPECT_EQ(data[i].expected_build_rows.size(), 1);
                    EXPECT_EQ(data[i].expected_build_rows[0]->get_tuple(0),
                            iter.get_row()->get_tuple(0));
                    ValidateMatch(row, iter.get_row());
                }
            }
        }
    }

    // Construct hash table with custom block manager. Returns result of PartitionedHashTable::init()
    bool CreateHashTable(bool quadratic, int64_t initial_num_buckets,
            scoped_ptr<PartitionedHashTable>* table, int block_size = 8 * 1024 * 1024,
            int max_num_blocks = 100, int reserved_blocks = 10) {
        EXPECT_TRUE(_test_env->create_query_state(0, max_num_blocks, block_size,
                    &_runtime_state).ok());
        _runtime_state->init_mem_trackers(TUniqueId());

        BufferedBlockMgr2::Client* client;
        EXPECT_TRUE(_runtime_state->block_mgr2()->register_client(reserved_blocks, &_limit,
                    _runtime_state, &client).ok());

        // Initial_num_buckets must be a power of two.
        EXPECT_EQ(initial_num_buckets, BitUtil::next_power_of_two(initial_num_buckets));
        int64_t max_num_buckets = 1L << 31;
        table->reset(new PartitionedHashTable(quadratic, _runtime_state, client, 1, NULL,
                    max_num_buckets, initial_num_buckets));
        return (*table)->init();
    }

    // Constructs and closes a hash table.
    void SetupTest(bool quadratic, int64_t initial_num_buckets, bool too_big) {
        TupleRow* build_row1 = CreateTupleRow(1);
        TupleRow* build_row2 = CreateTupleRow(2);
        TupleRow* probe_row3 = CreateTupleRow(3);
        TupleRow* probe_row4 = CreateTupleRow(4);

        int32_t* val_row1 =
            reinterpret_cast<int32_t*>(_build_expr_ctxs[0]->get_value(build_row1));
        EXPECT_EQ(*val_row1, 1);
        int32_t* val_row2 =
            reinterpret_cast<int32_t*>(_build_expr_ctxs[0]->get_value(build_row2));
        EXPECT_EQ(*val_row2, 2);
        int32_t* val_row3 =
            reinterpret_cast<int32_t*>(_probe_expr_ctxs[0]->get_value(probe_row3));
        EXPECT_EQ(*val_row3, 3);
        int32_t* val_row4 =
            reinterpret_cast<int32_t*>(_probe_expr_ctxs[0]->get_value(probe_row4));
        EXPECT_EQ(*val_row4, 4);

        // Create and close the hash table.
        scoped_ptr<PartitionedHashTable> hash_table;
        bool initialized = CreateHashTable(quadratic, initial_num_buckets, &hash_table);
        EXPECT_EQ(too_big, !initialized);

        hash_table->close();
    }

    // This test inserts the build rows [0->5) to hash table. It validates that they
    // are all there using a full table scan. It also validates that find() is correct
    // testing for probe rows that are both there and not.
    // The hash table is resized a few times and the scans/finds are tested again.
    void BasicTest(bool quadratic, int initial_num_buckets) {
        TupleRow* build_rows[5];
        TupleRow* scan_rows[5] = {0};
        for (int i = 0; i < 5; ++i) {
            build_rows[i] = CreateTupleRow(i);
        }

        ProbeTestData probe_rows[10];
        for (int i = 0; i < 10; ++i) {
            probe_rows[i].probe_row = CreateTupleRow(i);
            if (i < 5) {
                probe_rows[i].expected_build_rows.push_back(build_rows[i]);
            }
        }

        // Create the hash table and Insert the build rows
        scoped_ptr<PartitionedHashTable> hash_table;
        ASSERT_TRUE(CreateHashTable(quadratic, initial_num_buckets, &hash_table));
        PartitionedHashTableCtx ht_ctx(_build_expr_ctxs, _probe_expr_ctxs, false, false, 1, 0, 1);

        uint32_t hash = 0;
        bool success = hash_table->check_and_resize(5, &ht_ctx);
        ASSERT_TRUE(success);
        for (int i = 0; i < 5; ++i) {
            if (!ht_ctx.eval_and_hash_build(build_rows[i], &hash)) {
                continue;
            }
            bool inserted = hash_table->insert(&ht_ctx, build_rows[i]->get_tuple(0), hash);
            EXPECT_TRUE(inserted);
        }
        EXPECT_EQ(hash_table->size(), 5);

        // Do a full table scan and validate returned pointers
        FullScan(hash_table.get(), &ht_ctx, 0, 5, true, scan_rows, build_rows);
        ProbeTest(hash_table.get(), &ht_ctx, probe_rows, 10, false);

        // Double the size of the hash table and scan again.
        ResizeTable(hash_table.get(), 2048, &ht_ctx);
        EXPECT_EQ(hash_table->num_buckets(), 2048);
        EXPECT_EQ(hash_table->size(), 5);
        memset(scan_rows, 0, sizeof(scan_rows));
        FullScan(hash_table.get(), &ht_ctx, 0, 5, true, scan_rows, build_rows);
        ProbeTest(hash_table.get(), &ht_ctx, probe_rows, 10, false);

        // Try to shrink and scan again.
        ResizeTable(hash_table.get(), 64, &ht_ctx);
        EXPECT_EQ(hash_table->num_buckets(), 64);
        EXPECT_EQ(hash_table->size(), 5);
        memset(scan_rows, 0, sizeof(scan_rows));
        FullScan(hash_table.get(), &ht_ctx, 0, 5, true, scan_rows, build_rows);
        ProbeTest(hash_table.get(), &ht_ctx, probe_rows, 10, false);

        // Resize to 8, which is the smallest value to fit the number of filled buckets.
        ResizeTable(hash_table.get(), 8, &ht_ctx);
        EXPECT_EQ(hash_table->num_buckets(), 8);
        EXPECT_EQ(hash_table->size(), 5);
        memset(scan_rows, 0, sizeof(scan_rows));
        FullScan(hash_table.get(), &ht_ctx, 0, 5, true, scan_rows, build_rows);
        ProbeTest(hash_table.get(), &ht_ctx, probe_rows, 10, false);

        hash_table->close();
        ht_ctx.close();
    }

    void ScanTest(bool quadratic, int initial_size, int rows_to_insert,
            int additional_rows) {
        scoped_ptr<PartitionedHashTable> hash_table;
        ASSERT_TRUE(CreateHashTable(quadratic, initial_size, &hash_table));

        int total_rows = rows_to_insert + additional_rows;
        PartitionedHashTableCtx ht_ctx(_build_expr_ctxs, _probe_expr_ctxs, false, false, 1, 0, 1);

        // Add 1 row with val 1, 2 with val 2, etc.
        vector<TupleRow*> build_rows;
        ProbeTestData* probe_rows = new ProbeTestData[total_rows];
        probe_rows[0].probe_row = CreateTupleRow(0);
        uint32_t hash = 0;
        for (int val = 1; val <= rows_to_insert; ++val) {
            bool success = hash_table->check_and_resize(val, &ht_ctx);
            EXPECT_TRUE(success) << " failed to resize: " << val;
            probe_rows[val].probe_row = CreateTupleRow(val);
            for (int i = 0; i < val; ++i) {
                TupleRow* row = CreateTupleRow(val);
                if (!ht_ctx.eval_and_hash_build(row, &hash)) {
                    continue;
                }
                hash_table->insert(&ht_ctx, row->get_tuple(0), hash);
                build_rows.push_back(row);
                probe_rows[val].expected_build_rows.push_back(row);
            }
        }

        // Add some more probe rows that aren't there.
        for (int val = rows_to_insert; val < rows_to_insert + additional_rows; ++val) {
            probe_rows[val].probe_row = CreateTupleRow(val);
        }

        // Test that all the builds were found.
        ProbeTest(hash_table.get(), &ht_ctx, probe_rows, total_rows, true);

        // Resize and try again.
        int target_size = BitUtil::next_power_of_two(2 * total_rows);
        ResizeTable(hash_table.get(), target_size, &ht_ctx);
        EXPECT_EQ(hash_table->num_buckets(), target_size);
        ProbeTest(hash_table.get(), &ht_ctx, probe_rows, total_rows, true);

        target_size = BitUtil::next_power_of_two(total_rows + 1);
        ResizeTable(hash_table.get(), target_size, &ht_ctx);
        EXPECT_EQ(hash_table->num_buckets(), target_size);
        ProbeTest(hash_table.get(), &ht_ctx, probe_rows, total_rows, true);

        delete [] probe_rows;
        hash_table->close();
        ht_ctx.close();
    }

    // This test continues adding tuples to the hash table and exercises the resize code
    // paths.
    void GrowTableTest(bool quadratic) {
        uint64_t num_to_add = 4;
        int expected_size = 0;

        // MemTracker tracker(100 * 1024 * 1024);
        MemTracker tracker(100 * 1024 * 1024);
        scoped_ptr<PartitionedHashTable> hash_table;
        ASSERT_TRUE(CreateHashTable(quadratic, num_to_add, &hash_table));
        PartitionedHashTableCtx ht_ctx(_build_expr_ctxs, _probe_expr_ctxs, false, false, 1, 0, 1);

        // Inserts num_to_add + (num_to_add^2) + (num_to_add^4) + ... + (num_to_add^20)
        // entries. When num_to_add == 4, then the total number of inserts is 4194300.
        int build_row_val = 0;
        uint32_t hash = 0;
        bool done_inserting = false;
        for (int i = 0; i < 20; ++i) {
            // Currently the mem used for the bucket is not being tracked by the mem tracker.
            // Thus the resize is expected to be successful.
            // TODO: Keep track of the mem used for the buckets and test cases where we actually
            // hit OOM.
            // TODO: Insert duplicates to also hit OOM.
            bool success = hash_table->check_and_resize(num_to_add, &ht_ctx);
            EXPECT_TRUE(success) << " failed to resize: " << num_to_add;
            for (int j = 0; j < num_to_add; ++build_row_val, ++j) {
                TupleRow* row = CreateTupleRow(build_row_val);
                if (!ht_ctx.eval_and_hash_build(row, &hash)) {
                    continue;
                }
                bool inserted = hash_table->insert(&ht_ctx, row->get_tuple(0), hash);
                if (!inserted) {
                    done_inserting = true;
                    break;
                }
            }
            if (done_inserting) {
                break;
            }
            expected_size += num_to_add;
            num_to_add *= 2;
        }
        EXPECT_FALSE(tracker.limit_exceeded());
        EXPECT_EQ(hash_table->size(), 4194300);
        // Validate that we can find the entries before we went over the limit
        for (int i = 0; i < expected_size * 5; i += 100000) {
            TupleRow* probe_row = CreateTupleRow(i);
            if (!ht_ctx.eval_and_hash_probe(probe_row, &hash)) {
                continue;
            }
            PartitionedHashTable::Iterator iter = hash_table->find(&ht_ctx, hash);
            if (i < hash_table->size()) {
                EXPECT_TRUE(!iter.at_end()) << " i: " << i;
                ValidateMatch(probe_row, iter.get_row());
            } else {
                EXPECT_TRUE(iter.at_end()) << " i: " << i;
            }
        }
        hash_table->close();
        ht_ctx.close();
    }

    // This test inserts and probes as many elements as the size of the hash table without
    // calling resize. All the inserts and probes are expected to succeed, because there is
    // enough space in the hash table (it is also expected to be slow). It also expects that
    // a probe for a N+1 element will return BUCKET_NOT_FOUND.
    void InsertFullTest(bool quadratic, int table_size) {
        scoped_ptr<PartitionedHashTable> hash_table;
        ASSERT_TRUE(CreateHashTable(quadratic, table_size, &hash_table));
        PartitionedHashTableCtx ht_ctx(_build_expr_ctxs, _probe_expr_ctxs, false, false, 1, 0, 1);
        EXPECT_EQ(hash_table->empty_buckets(), table_size);

        // Insert and probe table_size different tuples. All of them are expected to be
        // successfully inserted and probed.
        uint32_t hash = 0;
        PartitionedHashTable::Iterator iter;
        bool found = false;
        for (int build_row_val = 0; build_row_val < table_size; ++build_row_val) {
            TupleRow* row = CreateTupleRow(build_row_val);
            bool passes = ht_ctx.eval_and_hash_build(row, &hash);
            EXPECT_TRUE(passes);

            // Insert using both insert() and find_bucket() methods.
            if (build_row_val % 2 == 0) {
                bool inserted = hash_table->insert(&ht_ctx, row->get_tuple(0), hash);
                EXPECT_TRUE(inserted);
            } else {
                iter = hash_table->find_bucket(&ht_ctx, hash, &found);
                EXPECT_FALSE(iter.at_end());
                EXPECT_FALSE(found);
                iter.set_tuple(row->get_tuple(0), hash);
            }
            EXPECT_EQ(hash_table->empty_buckets(), table_size - build_row_val - 1);

            passes = ht_ctx.eval_and_hash_probe(row, &hash);
            EXPECT_TRUE(passes);
            iter = hash_table->find(&ht_ctx, hash);
            EXPECT_FALSE(iter.at_end());
            EXPECT_EQ(row->get_tuple(0), iter.get_tuple());

            iter = hash_table->find_bucket(&ht_ctx, hash, &found);
            EXPECT_FALSE(iter.at_end());
            EXPECT_TRUE(found);
            EXPECT_EQ(row->get_tuple(0), iter.get_tuple());
        }

        // Probe for a tuple that does not exist. This should exercise the probe of a full
        // hash table code path.
        EXPECT_EQ(hash_table->empty_buckets(), 0);
        TupleRow* probe_row = CreateTupleRow(table_size);
        bool passes = ht_ctx.eval_and_hash_probe(probe_row, &hash);
        EXPECT_TRUE(passes);
        iter = hash_table->find(&ht_ctx, hash);
        EXPECT_TRUE(iter.at_end());

        // Since hash_table is full, find_bucket cannot find an empty bucket, so returns End().
        iter = hash_table->find_bucket(&ht_ctx, hash, &found);
        EXPECT_TRUE(iter.at_end());
        EXPECT_FALSE(found);

        hash_table->close();
        ht_ctx.close();
    }

    // This test makes sure we can tolerate the low memory case where we do not have enough
    // memory to allocate the array of buckets for the hash table.
    void VeryLowMemTest(bool quadratic) {
        const int block_size = 2 * 1024;
        const int max_num_blocks = 1;
        const int reserved_blocks = 0;
        const int table_size = 1024;
        scoped_ptr<PartitionedHashTable> hash_table;
        ASSERT_FALSE(CreateHashTable(quadratic, table_size, &hash_table, block_size,
                    max_num_blocks, reserved_blocks));
        PartitionedHashTableCtx ht_ctx(_build_expr_ctxs, _probe_expr_ctxs, false, false, 1, 0, 1);
        PartitionedHashTable::Iterator iter = hash_table->begin(&ht_ctx);
        EXPECT_TRUE(iter.at_end());

        hash_table->close();
    }
};

TEST_F(PartitionedHashTableTest, LinearSetupTest) {
    SetupTest(false, 1, false);
    SetupTest(false, 1024, false);
    SetupTest(false, 65536, false);

    // Regression test for IMPALA-2065. Trying to init a hash table with large (>2^31)
    // number of buckets.
    SetupTest(false, 4294967296, true); // 2^32
}

TEST_F(PartitionedHashTableTest, QuadraticSetupTest) {
    SetupTest(true, 1, false);
    SetupTest(true, 1024, false);
    SetupTest(true, 65536, false);

    // Regression test for IMPALA-2065. Trying to init a hash table with large (>2^31)
    // number of buckets.
    SetupTest(true, 4294967296, true); // 2^32
}

TEST_F(PartitionedHashTableTest, LinearBasicTest) {
    BasicTest(false, 1);
    BasicTest(false, 1024);
    BasicTest(false, 65536);
}

TEST_F(PartitionedHashTableTest, QuadraticBasicTest) {
    BasicTest(true, 1);
    BasicTest(true, 1024);
    BasicTest(true, 65536);
}

// This test makes sure we can scan ranges of buckets.
TEST_F(PartitionedHashTableTest, LinearScanTest) {
    ScanTest(false, 1, 10, 5);
    ScanTest(false, 1024, 1000, 5);
    ScanTest(false, 1024, 1000, 500);
}

TEST_F(PartitionedHashTableTest, QuadraticScanTest) {
    ScanTest(true, 1, 10, 5);
    ScanTest(true, 1024, 1000, 5);
    ScanTest(true, 1024, 1000, 500);
}

TEST_F(PartitionedHashTableTest, LinearGrowTableTest) {
    GrowTableTest(false);
}

TEST_F(PartitionedHashTableTest, QuadraticGrowTableTest) {
    GrowTableTest(true);
}

TEST_F(PartitionedHashTableTest, LinearInsertFullTest) {
    InsertFullTest(false, 1);
    InsertFullTest(false, 4);
    InsertFullTest(false, 64);
    InsertFullTest(false, 1024);
    InsertFullTest(false, 65536);
}

TEST_F(PartitionedHashTableTest, QuadraticInsertFullTest) {
    InsertFullTest(true, 1);
    InsertFullTest(true, 4);
    InsertFullTest(true, 64);
    InsertFullTest(true, 1024);
    InsertFullTest(true, 65536);
}

// Test that hashing empty string updates hash value.
TEST_F(PartitionedHashTableTest, HashEmpty) {
    PartitionedHashTableCtx ht_ctx(_build_expr_ctxs, _probe_expr_ctxs, false, false, 1, 2, 1);
    uint32_t seed = 9999;
    ht_ctx.set_level(0);
    EXPECT_NE(seed, ht_ctx.hash_help(NULL, 0, seed));
    // TODO: level 0 uses CRC hash, which only swaps bytes around on empty input.
    // EXPECT_NE(seed, ht_ctx.hash_help(NULL, 0, ht_ctx.hash_help(NULL, 0, seed)));
    ht_ctx.set_level(1);
    EXPECT_NE(seed, ht_ctx.hash_help(NULL, 0, seed));
    EXPECT_NE(seed, ht_ctx.hash_help(NULL, 0, ht_ctx.hash_help(NULL, 0, seed)));
    ht_ctx.close();
}

TEST_F(PartitionedHashTableTest, VeryLowMemTest) {
    VeryLowMemTest(true);
    VeryLowMemTest(false);
}
#if 0
#endif

} // end namespace doris

int main(int argc, char** argv) {
    // std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    // if (!doris::config::init(conffile.c_str(), false)) {
    //     fprintf(stderr, "error read config file. \n");
    //     return -1;
    // }
    doris::config::query_scratch_dirs = "/tmp";
    // doris::config::max_free_io_buffers = 128;
    doris::config::read_size = 8388608;
    doris::config::min_buffer_size = 1024;

    doris::config::disable_mem_pools = false;

    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);

    doris::CpuInfo::init();
    doris::DiskInfo::init();

    return RUN_ALL_TESTS();
}
