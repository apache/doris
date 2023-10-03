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
//
// This file is copied from
// https://github.com/apache/kudu/blob/master/src/kudu/tablet/rowset_tree-test.cc
// and modified by Doris

#include "olap/rowset/rowset_tree.h"

#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "gutil/map-util.h"
#include "gutil/stringprintf.h"
#include "gutil/strings/substitute.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/unique_rowset_id_generator.h"
#include "olap/tablet_schema.h"
#include "testutil/mock_rowset.h"
#include "testutil/test_util.h"
#include "util/slice.h"
#include "util/stopwatch.hpp"

using std::make_shared;
using std::shared_ptr;
using std::string;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace doris {

class TestRowsetTree : public testing::Test {
public:
    TestRowsetTree() : rowset_id_generator_({0, 0}) {}

    void SetUp() {
        schema_ = std::make_shared<TabletSchema>();
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(UNIQUE_KEYS);
        schema_->init_from_pb(schema_pb);
    }

    // Generates random rowsets with keys between 0 and 10000
    RowsetVector GenerateRandomRowsets(int num_sets) {
        RowsetVector vec;
        for (int i = 0; i < num_sets; i++) {
            int min = rand() % 9000;
            int max = min + 1000;
            vec.push_back(create_rowset(StringPrintf("%04d", min), StringPrintf("%04d", max)));
        }
        return vec;
    }

    RowsetSharedPtr create_rowset(const string& min_key, const string& max_key,
                                  bool is_mem_rowset = false) {
        RowsetMetaPB rs_meta_pb;
        rs_meta_pb.set_rowset_id_v2(rowset_id_generator_.next_id().to_string());
        rs_meta_pb.set_num_segments(1);
        KeyBoundsPB key_bounds;
        key_bounds.set_min_key(min_key);
        key_bounds.set_max_key(max_key);
        KeyBoundsPB* new_key_bounds = rs_meta_pb.add_segments_key_bounds();
        *new_key_bounds = key_bounds;
        RowsetMetaSharedPtr meta_ptr = make_shared<RowsetMeta>();
        meta_ptr->init_from_pb(rs_meta_pb);
        RowsetSharedPtr res_ptr;
        static_cast<void>(MockRowset::create_rowset(schema_, meta_ptr, &res_ptr, is_mem_rowset));
        return res_ptr;
    }

private:
    TabletSchemaSPtr schema_;
    std::string rowset_path_;
    UniqueRowsetIdGenerator rowset_id_generator_;
};

TEST_F(TestRowsetTree, TestTree) {
    RowsetIdUnorderedSet rowset_ids;
    RowsetVector vec;
    auto rowset1 = create_rowset("0", "5");
    vec.push_back(rowset1);
    rowset_ids.insert(rowset1->rowset_id());

    auto rowset2 = create_rowset("3", "5");
    vec.push_back(rowset2);
    rowset_ids.insert(rowset2->rowset_id());

    auto rowset3 = create_rowset("5", "9");
    vec.push_back(rowset3);
    rowset_ids.insert(rowset3->rowset_id());

    auto rowset4 = create_rowset("0", "0", true);
    vec.push_back(rowset4);
    rowset_ids.insert(rowset4->rowset_id());

    RowsetTree tree;
    ASSERT_FALSE(tree.Init(vec).ok());

    vec.erase(vec.begin() + 3);
    ASSERT_TRUE(tree.Init(vec).ok());

    // "2" overlaps 0-5
    vector<std::pair<RowsetSharedPtr, int32_t>> out;
    tree.FindRowsetsWithKeyInRange("2", &rowset_ids, &out);
    ASSERT_EQ(1, out.size());
    ASSERT_EQ(vec[0].get(), out[0].first.get());

    // "4" overlaps 0-5, 3-5
    out.clear();
    tree.FindRowsetsWithKeyInRange("4", &rowset_ids, &out);
    ASSERT_EQ(2, out.size());
    ASSERT_EQ(vec[0].get(), out[0].first.get());
    ASSERT_EQ(vec[1].get(), out[1].first.get());

    // interval [3,4) overlaps 0-5, 3-5
    out.clear();
    tree.FindRowsetsIntersectingInterval(Slice("3"), Slice("4"), &out);
    ASSERT_EQ(2, out.size());
    ASSERT_EQ(vec[0].get(), out[0].first.get());
    ASSERT_EQ(vec[1].get(), out[1].first.get());

    // interval [0,2) overlaps 0-5
    out.clear();
    tree.FindRowsetsIntersectingInterval(Slice("0"), Slice("2"), &out);
    ASSERT_EQ(1, out.size());
    ASSERT_EQ(vec[0].get(), out[0].first.get());

    // interval [5,7) overlaps 0-5, 3-5, 5-9
    out.clear();
    tree.FindRowsetsIntersectingInterval(Slice("5"), Slice("7"), &out);
    ASSERT_EQ(3, out.size());
    ASSERT_EQ(vec[0].get(), out[0].first.get());
    ASSERT_EQ(vec[1].get(), out[1].first.get());
    ASSERT_EQ(vec[2].get(), out[2].first.get());

    // "3" overlaps 0-5, 3-5
    out.clear();
    tree.FindRowsetsWithKeyInRange("3", &rowset_ids, &out);
    ASSERT_EQ(2, out.size());
    ASSERT_EQ(vec[0].get(), out[0].first.get());
    ASSERT_EQ(vec[1].get(), out[1].first.get());

    // "5" overlaps 0-5, 3-5, 5-9
    out.clear();
    tree.FindRowsetsWithKeyInRange("5", &rowset_ids, &out);
    ASSERT_EQ(3, out.size());
    ASSERT_EQ(vec[0].get(), out[0].first.get());
    ASSERT_EQ(vec[1].get(), out[1].first.get());
    ASSERT_EQ(vec[2].get(), out[2].first.get());

    // interval [0,5) overlaps 0-5, 3-5
    out.clear();
    tree.FindRowsetsIntersectingInterval(Slice("0"), Slice("5"), &out);
    ASSERT_EQ(2, out.size());
    ASSERT_EQ(vec[0].get(), out[0].first.get());
    ASSERT_EQ(vec[1].get(), out[1].first.get());

    // interval [3,5) overlaps 0-5, 3-5
    out.clear();
    tree.FindRowsetsIntersectingInterval(Slice("3"), Slice("5"), &out);
    ASSERT_EQ(2, out.size());
    ASSERT_EQ(vec[0].get(), out[0].first.get());
    ASSERT_EQ(vec[1].get(), out[1].first.get());

    // interval [-OO,3) overlaps 0-5
    out.clear();
    tree.FindRowsetsIntersectingInterval(std::nullopt, Slice("3"), &out);
    ASSERT_EQ(1, out.size());
    ASSERT_EQ(vec[0].get(), out[0].first.get());

    // interval [-OO,5) overlaps 0-5, 3-5
    out.clear();
    tree.FindRowsetsIntersectingInterval(std::nullopt, Slice("5"), &out);
    ASSERT_EQ(2, out.size());
    ASSERT_EQ(vec[0].get(), out[0].first.get());
    ASSERT_EQ(vec[1].get(), out[1].first.get());

    // interval [-OO,99) overlaps 0-5, 3-5, 5-9
    out.clear();
    tree.FindRowsetsIntersectingInterval(std::nullopt, Slice("99"), &out);
    ASSERT_EQ(3, out.size());
    ASSERT_EQ(vec[0].get(), out[0].first.get());
    ASSERT_EQ(vec[1].get(), out[1].first.get());
    ASSERT_EQ(vec[2].get(), out[2].first.get());

    // interval [6,+OO) overlaps 5-9
    out.clear();
    tree.FindRowsetsIntersectingInterval(Slice("6"), std::nullopt, &out);
    ASSERT_EQ(1, out.size());
    ASSERT_EQ(vec[2].get(), out[0].first.get());

    // interval [5,+OO) overlaps 0-5, 3-5, 5-9
    out.clear();
    tree.FindRowsetsIntersectingInterval(Slice("5"), std::nullopt, &out);
    ASSERT_EQ(3, out.size());
    ASSERT_EQ(vec[0].get(), out[0].first.get());
    ASSERT_EQ(vec[1].get(), out[1].first.get());
    ASSERT_EQ(vec[2].get(), out[2].first.get());

    // interval [4,+OO) overlaps 0-5, 3-5, 5-9
    out.clear();
    tree.FindRowsetsIntersectingInterval(Slice("4"), std::nullopt, &out);
    ASSERT_EQ(3, out.size());
    ASSERT_EQ(vec[0].get(), out[0].first.get());
    ASSERT_EQ(vec[1].get(), out[1].first.get());
    ASSERT_EQ(vec[2].get(), out[2].first.get());

    // interval [-OO,+OO) overlaps 0-5, 3-5, 5-9
    out.clear();
    tree.FindRowsetsIntersectingInterval(std::nullopt, std::nullopt, &out);
    ASSERT_EQ(3, out.size());
    ASSERT_EQ(vec[0].get(), out[0].first.get());
    ASSERT_EQ(vec[1].get(), out[1].first.get());
    ASSERT_EQ(vec[2].get(), out[2].first.get());
}

TEST_F(TestRowsetTree, TestTreeRandomized) {
    enum BoundOperator {
        BOUND_LESS_THAN,
        BOUND_LESS_EQUAL,
        BOUND_GREATER_THAN,
        BOUND_GREATER_EQUAL,
        BOUND_EQUAL
    };
    const auto& GetStringPair = [](const BoundOperator op, int start, int range_length) {
        while (true) {
            string s1 = Substitute("$0", rand_rng_int(start, start + range_length));
            string s2 = Substitute("$0", rand_rng_int(start, start + range_length));
            int r = strcmp(s1.c_str(), s2.c_str());
            switch (op) {
            case BOUND_LESS_THAN:
                if (r == 0) continue;
                [[fallthrough]];
            case BOUND_LESS_EQUAL:
                return std::pair<string, string>(std::min(s1, s2), std::max(s1, s2));
            case BOUND_GREATER_THAN:
                if (r == 0) continue;
                [[fallthrough]];
            case BOUND_GREATER_EQUAL:
                return std::pair<string, string>(std::max(s1, s2), std::min(s1, s2));
            case BOUND_EQUAL:
                return std::pair<string, string>(s1, s1);
            }
        }
    };

    RowsetVector vec;
    for (int i = 0; i < 100; i++) {
        std::pair<string, string> bound = GetStringPair(BOUND_LESS_EQUAL, 1000, 900);
        ASSERT_LE(bound.first, bound.second);
        vec.push_back(shared_ptr<Rowset>(create_rowset(bound.first, bound.second)));
    }
    RowsetTree tree;
    ASSERT_TRUE(tree.Init(vec).ok());

    // When lower < upper.
    vector<std::pair<RowsetSharedPtr, int32_t>> out;
    for (int i = 0; i < 100; i++) {
        out.clear();
        std::pair<string, string> bound = GetStringPair(BOUND_LESS_THAN, 1000, 900);
        ASSERT_LT(bound.first, bound.second);
        tree.FindRowsetsIntersectingInterval(Slice(bound.first), Slice(bound.second), &out);
        for (const auto& e : out) {
            std::vector<KeyBoundsPB> segments_key_bounds;
            static_cast<void>(e.first->get_segments_key_bounds(&segments_key_bounds));
            ASSERT_EQ(1, segments_key_bounds.size());
            string min = segments_key_bounds[0].min_key();
            string max = segments_key_bounds[0].max_key();
            if (min < bound.first) {
                ASSERT_GE(max, bound.first);
            } else {
                ASSERT_LT(min, bound.second);
            }
            if (max >= bound.second) {
                ASSERT_LT(min, bound.second);
            } else {
                ASSERT_GE(max, bound.first);
            }
        }
    }

    // Remove 50 rowsets, add 10 new rowsets, with non overlapping key range.
    RowsetVector vec_to_del(vec.begin(), vec.begin() + 50);
    RowsetVector vec_to_add;
    for (int i = 0; i < 10; i++) {
        std::pair<string, string> bound = GetStringPair(BOUND_LESS_EQUAL, 2000, 900);
        ASSERT_LE(bound.first, bound.second);
        vec_to_add.push_back(shared_ptr<Rowset>(create_rowset(bound.first, bound.second)));
    }

    RowsetTree new_tree;
    ModifyRowSetTree(tree, vec_to_del, vec_to_add, &new_tree);

    // only 50 rowsets left in old key range "1000"-"1900"
    out.clear();
    new_tree.FindRowsetsIntersectingInterval(Slice("1000"), Slice("1999"), &out);
    ASSERT_EQ(50, out.size());
    // should get 10 new added rowsets with key range "2000"-"2900"
    out.clear();
    new_tree.FindRowsetsIntersectingInterval(Slice("2000"), Slice("2999"), &out);
    ASSERT_EQ(10, out.size());
    out.clear();
    new_tree.FindRowsetsIntersectingInterval(Slice("1000"), Slice("2999"), &out);
    ASSERT_EQ(60, out.size());
}

class TestRowsetTreePerformance : public TestRowsetTree,
                                  public testing::WithParamInterface<std::tuple<int, int>> {};
INSTANTIATE_TEST_SUITE_P(Parameters, TestRowsetTreePerformance,
                         testing::Combine(
                                 // Number of rowsets.
                                 // Up to 500 rowsets (500*32MB = 16GB tablet)
                                 testing::Values(10, 100, 250, 500),
                                 // Number of query points in a batch.
                                 testing::Values(10, 100, 500, 1000, 5000)));

TEST_P(TestRowsetTreePerformance, TestPerformance) {
    const int kNumRowsets = std::get<0>(GetParam());
    const int kNumQueries = std::get<1>(GetParam());
    const int kNumIterations = AllowSlowTests() ? 1000 : 10;

    MonotonicStopWatch one_at_time_timer;
    MonotonicStopWatch batch_timer;
    RowsetIdUnorderedSet rowset_ids;
    for (int i = 0; i < kNumIterations; i++) {
        rowset_ids.clear();
        // Create a bunch of rowsets, each of which spans about 10% of the "row space".
        // The row space here is 4-digit 0-padded numbers.
        RowsetVector vec = GenerateRandomRowsets(kNumRowsets);
        for (auto rowset : vec) {
            rowset_ids.insert(rowset->rowset_id());
        }

        RowsetTree tree;
        ASSERT_TRUE(tree.Init(vec).ok());

        vector<string> queries;
        for (int j = 0; j < kNumQueries; j++) {
            int query = rand_rng_int(0, 10000);
            queries.emplace_back(StringPrintf("%04d", query));
        }

        int individual_matches = 0;
        one_at_time_timer.start();
        {
            vector<std::pair<RowsetSharedPtr, int32_t>> out;
            for (const auto& q : queries) {
                out.clear();
                tree.FindRowsetsWithKeyInRange(Slice(q), &rowset_ids, &out);
                individual_matches += out.size();
            }
        }
        one_at_time_timer.stop();

        vector<Slice> query_slices;
        for (const auto& q : queries) {
            query_slices.emplace_back(q);
        }

        batch_timer.start();
        std::sort(query_slices.begin(), query_slices.end(), Slice::Comparator());
        int bulk_matches = 0;
        {
            tree.ForEachRowsetContainingKeys(
                    query_slices, [&](RowsetSharedPtr rs, int slice_idx) { bulk_matches++; });
        }
        batch_timer.stop();

        ASSERT_EQ(bulk_matches, individual_matches);
    }

    double batch_total = batch_timer.elapsed_time();
    double oat_total = one_at_time_timer.elapsed_time();
    const string& case_desc = StringPrintf("Q=% 5d R=% 5d", kNumQueries, kNumRowsets);
    LOG(INFO) << StringPrintf("%s %10s %d ms", case_desc.c_str(), "1-by-1",
                              static_cast<int>(oat_total / 1e6));
    LOG(INFO) << StringPrintf("%s %10s %d ms (%.2fx)", case_desc.c_str(), "batched",
                              static_cast<int>(batch_total / 1e6),
                              batch_total ? (oat_total / batch_total) : 0);
}

TEST_F(TestRowsetTree, TestEndpointsConsistency) {
    const int kNumRowsets = 1000;
    RowsetVector vec = GenerateRandomRowsets(kNumRowsets);
    // Add pathological one-key rows
    for (int i = 0; i < 10; ++i) {
        vec.push_back(create_rowset(StringPrintf("%04d", 11000), StringPrintf("%04d", 11000)));
    }
    vec.push_back(create_rowset(StringPrintf("%04d", 12000), StringPrintf("%04d", 12000)));
    // Make tree
    RowsetTree tree;
    ASSERT_TRUE(tree.Init(vec).ok());
    // Keep track of "currently open" intervals defined by the endpoints
    unordered_set<RowsetSharedPtr> open;
    // Keep track of all rowsets that have been visited
    unordered_set<RowsetSharedPtr> visited;

    Slice prev;
    for (const RowsetTree::RSEndpoint& rse : tree.key_endpoints()) {
        RowsetSharedPtr rs = rse.rowset_;
        enum RowsetTree::EndpointType ept = rse.endpoint_;
        const Slice& slice = rse.slice_;

        ASSERT_TRUE(rs != nullptr) << "RowsetTree has an endpoint with no rowset";
        ASSERT_TRUE(!slice.empty()) << "RowsetTree has an endpoint with no key";

        if (!prev.empty()) {
            ASSERT_LE(prev.compare(slice), 0);
        }

        std::vector<KeyBoundsPB> segments_key_bounds;
        ASSERT_TRUE(rs->get_segments_key_bounds(&segments_key_bounds).ok());
        ASSERT_EQ(1, segments_key_bounds.size());
        string min = segments_key_bounds[0].min_key();
        string max = segments_key_bounds[0].max_key();
        if (ept == RowsetTree::START) {
            ASSERT_EQ(min, slice.to_string());
            ASSERT_TRUE(InsertIfNotPresent(&open, rs));
            ASSERT_TRUE(InsertIfNotPresent(&visited, rs));
        } else if (ept == RowsetTree::STOP) {
            ASSERT_EQ(max, slice.to_string());
            ASSERT_TRUE(open.erase(rs) == 1);
        } else {
            FAIL() << "No such endpoint type exists";
        }
    }
}

} // namespace doris
