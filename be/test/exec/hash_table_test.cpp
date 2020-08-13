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

#include <map>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <vector>

#include <gtest/gtest.h>

#include "common/compiler_util.h"
#include "exec/hash_table.hpp"
#include "exprs/expr.h"
#include "runtime/mem_pool.h"
#include "runtime/string_value.h"
#include "util/cpu_info.h"
#include "util/runtime_profile.h"

namespace doris {

using std::vector;
using std::map;


class HashTableTest : public testing::Test {
public:
    HashTableTest() : _mem_pool() {}

protected:
    ObjectPool _pool;
    MemPool _mem_pool;
    vector<Expr*> _build_expr;
    vector<Expr*> _probe_expr;

    virtual void SetUp() {
        RowDescriptor desc;
        Status status;

        // Not very easy to test complex tuple layouts so this test will use the
        // simplest.  The purpose of these tests is to exercise the hash map
        // internals so a simple build/probe expr is fine.
        _build_expr.push_back(_pool.add(new SlotRef(TYPE_INT, 0)));
        status = Expr::prepare(_build_expr, NULL, desc);
        EXPECT_TRUE(status.ok());

        _probe_expr.push_back(_pool.add(new SlotRef(TYPE_INT, 0)));
        status = Expr::prepare(_probe_expr, NULL, desc);
        EXPECT_TRUE(status.ok());
    }

    TupleRow* create_tuple_row(int32_t val);

    // Wrapper to call private methods on HashTable
    // TODO: understand google testing, there must be a more natural way to do this
    void resize_table(HashTable* table, int64_t new_size) {
        table->resize_buckets(new_size);
    }

    // Do a full table scan on table.  All values should be between [min,max).  If
    // all_unique, then each key(int value) should only appear once.  Results are
    // stored in results, indexed by the key.  Results must have been preallocated to
    // be at least max size.
    void full_scan(HashTable* table, int min, int max, bool all_unique,
                  TupleRow** results, TupleRow** expected) {
        HashTable::Iterator iter = table->begin();

        while (iter != table->end()) {
            TupleRow* row = iter.get_row();
            int32_t val = *reinterpret_cast<int32_t*>(_build_expr[0]->get_value(row));
            EXPECT_GE(val, min);
            EXPECT_LT(val, max);

            if (all_unique) {
                EXPECT_TRUE(results[val] == NULL);
            }

            EXPECT_EQ(row->get_tuple(0), expected[val]->get_tuple(0));
            results[val] = row;
            iter.next<false>();
        }
    }

    // Validate that probe_row evaluates overs probe_exprs is equal to build_row
    // evaluated over build_exprs
    void validate_match(TupleRow* probe_row, TupleRow* build_row) {
        EXPECT_TRUE(probe_row != build_row);
        int32_t build_val = *reinterpret_cast<int32_t*>(_build_expr[0]->get_value(probe_row));
        int32_t probe_val = *reinterpret_cast<int32_t*>(_probe_expr[0]->get_value(build_row));
        EXPECT_EQ(build_val, probe_val);
    }

    struct ProbeTestData {
        TupleRow* probe_row;
        vector<TupleRow*> expected_build_rows;
    };

    void probe_test(HashTable* table, ProbeTestData* data, int num_data, bool scan) {
        for (int i = 0; i < num_data; ++i) {
            TupleRow* row = data[i].probe_row;

            HashTable::Iterator iter;
            iter = table->find(row);

            if (data[i].expected_build_rows.size() == 0) {
                EXPECT_TRUE(iter == table->end());
            } else {
                if (scan) {
                    map<TupleRow*, bool> matched;

                    while (iter != table->end()) {
                        EXPECT_TRUE(matched.find(iter.get_row()) == matched.end());
                        matched[iter.get_row()] = true;
                        iter.next<true>();
                    }

                    EXPECT_EQ(matched.size(), data[i].expected_build_rows.size());

                    for (int j = 0; i < data[j].expected_build_rows.size(); ++j) {
                        EXPECT_TRUE(matched[data[i].expected_build_rows[j]]);
                    }
                } else {
                    EXPECT_EQ(data[i].expected_build_rows.size(), 1);
                    EXPECT_EQ(data[i].expected_build_rows[0]->get_tuple(0), 
                              iter.get_row()->get_tuple(0));
                    validate_match(row, iter.get_row());
                }
            }
        }
    }
};

TupleRow* HashTableTest::create_tuple_row(int32_t val) {
    uint8_t* tuple_row_mem = _mem_pool.allocate(sizeof(int32_t*));
    uint8_t* tuple_mem = _mem_pool.allocate(sizeof(int32_t));
    *reinterpret_cast<int32_t*>(tuple_mem) = val;
    TupleRow* row = reinterpret_cast<TupleRow*>(tuple_row_mem);
    row->set_tuple(0, reinterpret_cast<Tuple*>(tuple_mem));
    return row;
}

TEST_F(HashTableTest, SetupTest) {
    TupleRow* build_row1 = create_tuple_row(1);
    TupleRow* build_row2 = create_tuple_row(2);
    TupleRow* probe_row3 = create_tuple_row(3);
    TupleRow* probe_row4 = create_tuple_row(4);

    int32_t* val_row1 = reinterpret_cast<int32_t*>(_build_expr[0]->get_value(build_row1));
    int32_t* val_row2 = reinterpret_cast<int32_t*>(_build_expr[0]->get_value(build_row2));
    int32_t* val_row3 = reinterpret_cast<int32_t*>(_probe_expr[0]->get_value(probe_row3));
    int32_t* val_row4 = reinterpret_cast<int32_t*>(_probe_expr[0]->get_value(probe_row4));

    EXPECT_EQ(*val_row1, 1);
    EXPECT_EQ(*val_row2, 2);
    EXPECT_EQ(*val_row3, 3);
    EXPECT_EQ(*val_row4, 4);
}

// This tests inserts the build rows [0->5) to hash table.  It validates that they
// are all there using a full table scan.  It also validates that find() is correct
// testing for probe rows that are both there and not.
// The hash table is rehashed a few times and the scans/finds are tested again.
TEST_F(HashTableTest, BasicTest) {
    TupleRow* build_rows[5];
    TupleRow* scan_rows[5] = {0};

    for (int i = 0; i < 5; ++i) {
        build_rows[i] = create_tuple_row(i);
    }

    ProbeTestData probe_rows[10];

    for (int i = 0; i < 10; ++i) {
        probe_rows[i].probe_row = create_tuple_row(i);

        if (i < 5) {
            probe_rows[i].expected_build_rows.push_back(build_rows[i]);
        }
    }

    // Create the hash table and insert the build rows
    HashTable hash_table(_build_expr, _probe_expr, 1, false, 0);

    for (int i = 0; i < 5; ++i) {
        hash_table.insert(build_rows[i]);
    }

    EXPECT_EQ(hash_table.size(), 5);

    // Do a full table scan and validate returned pointers
    full_scan(&hash_table, 0, 5, true, scan_rows, build_rows);
    probe_test(&hash_table, probe_rows, 10, false);

    // Resize and scan again
    resize_table(&hash_table, 64);
    EXPECT_EQ(hash_table.num_buckets(), 64);
    EXPECT_EQ(hash_table.size(), 5);
    memset(scan_rows, 0, sizeof(scan_rows));
    full_scan(&hash_table, 0, 5, true, scan_rows, build_rows);
    probe_test(&hash_table, probe_rows, 10, false);

    // Resize to two and cause some collisions
    resize_table(&hash_table, 2);
    EXPECT_EQ(hash_table.num_buckets(), 2);
    EXPECT_EQ(hash_table.size(), 5);
    memset(scan_rows, 0, sizeof(scan_rows));
    full_scan(&hash_table, 0, 5, true, scan_rows, build_rows);
    probe_test(&hash_table, probe_rows, 10, false);

    // Resize to one and turn it into a linked list
    resize_table(&hash_table, 1);
    EXPECT_EQ(hash_table.num_buckets(), 1);
    EXPECT_EQ(hash_table.size(), 5);
    memset(scan_rows, 0, sizeof(scan_rows));
    full_scan(&hash_table, 0, 5, true, scan_rows, build_rows);
    probe_test(&hash_table, probe_rows, 10, false);
}

// This tests makes sure we can scan ranges of buckets
TEST_F(HashTableTest, ScanTest) {
    HashTable hash_table(_build_expr, _probe_expr, 1, false, 0);
    // Add 1 row with val 1, 2 with val 2, etc
    vector<TupleRow*> build_rows;
    ProbeTestData probe_rows[15];
    probe_rows[0].probe_row = create_tuple_row(0);

    for (int val = 1; val <= 10; ++val) {
        probe_rows[val].probe_row = create_tuple_row(val);

        for (int i = 0; i < val; ++i) {
            TupleRow* row = create_tuple_row(val);
            hash_table.insert(row);
            build_rows.push_back(row);
            probe_rows[val].expected_build_rows.push_back(row);
        }
    }

    // Add some more probe rows that aren't there
    for (int val = 11; val < 15; ++val) {
        probe_rows[val].probe_row = create_tuple_row(val);
    }

    // Test that all the builds were found
    probe_test(&hash_table, probe_rows, 15, true);

    // Resize and try again
    resize_table(&hash_table, 128);
    EXPECT_EQ(hash_table.num_buckets(), 128);
    probe_test(&hash_table, probe_rows, 15, true);

    resize_table(&hash_table, 16);
    EXPECT_EQ(hash_table.num_buckets(), 16);
    probe_test(&hash_table, probe_rows, 15, true);

    resize_table(&hash_table, 2);
    EXPECT_EQ(hash_table.num_buckets(), 2);
    probe_test(&hash_table, probe_rows, 15, true);
}

// This test continues adding to the hash table to trigger the resize code paths
TEST_F(HashTableTest, GrowTableTest) {
    int build_row_val = 0;
    int num_to_add = 4;
    int expected_size = 0;

    auto mem_tracker = std::make_shared<MemTracker>(1024 * 1024);
    HashTable hash_table(
        _build_expr, _probe_expr, 1, false, 0, mem_tracker, num_to_add);
    EXPECT_FALSE(mem_tracker->limit_exceeded());

    // This inserts about 5M entries
    for (int i = 0; i < 20; ++i) {
        for (int j = 0; j < num_to_add; ++build_row_val, ++j) {
            hash_table.insert(create_tuple_row(build_row_val));
        }

        expected_size += num_to_add;
        num_to_add *= 2;
        EXPECT_EQ(hash_table.size(), expected_size);
    }

    EXPECT_TRUE(mem_tracker->limit_exceeded());

    // Validate that we can find the entries
    for (int i = 0; i < expected_size * 5; i += 100000) {
        TupleRow* probe_row = create_tuple_row(i);
        HashTable::Iterator iter = hash_table.find(probe_row);

        if (i < expected_size) {
            EXPECT_TRUE(iter != hash_table.end());
            validate_match(probe_row, iter.get_row());
        } else {
            EXPECT_TRUE(iter == hash_table.end());
        }
    }
}

// This test continues adding to the hash table to trigger the resize code paths
TEST_F(HashTableTest, GrowTableTest2) {
    int build_row_val = 0;
    int num_to_add = 1024;
    int expected_size = 0;

    auto mem_tracker = std::make_shared<MemTracker>(1024 * 1024);
    HashTable hash_table(
        _build_expr, _probe_expr, 1, false, 0, mem_tracker, num_to_add);

    LOG(INFO) << time(NULL);

    // This inserts about 5M entries
    for (int i = 0; i < 5 * 1024 * 1024; ++i) {
        hash_table.insert(create_tuple_row(build_row_val));
        expected_size += num_to_add;
    }

    LOG(INFO) << time(NULL);

    // Validate that we can find the entries
    for (int i = 0; i < 5 * 1024 * 1024; ++i) {
        TupleRow* probe_row = create_tuple_row(i);
        hash_table.find(probe_row);
    }

    LOG(INFO) << time(NULL);
}

}

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
