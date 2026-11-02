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

#include "exec/operator/file_scan_operator.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "exec/pipeline/dependency.h"
#include "exec/scan/file_scanner_v2.h"
#include "exec/scan/scanner.h"
#include "exec/scan/scanner_scheduler.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris::pipeline {
namespace {

// One tuple with no slots: all a file scan node needs to be initialized and prepared.
Status create_one_tuple_descriptors(ObjectPool* pool, DescriptorTbl** descriptors) {
    TDescriptorTable thrift_descriptors;
    TTupleDescriptor tuple_descriptor;
    tuple_descriptor.id = 0;
    tuple_descriptor.byteSize = 0;
    tuple_descriptor.numNullBytes = 0;
    thrift_descriptors.tupleDescriptors.push_back(tuple_descriptor);
    return DescriptorTbl::create(pool, thrift_descriptors, descriptors);
}

// A file scan node over that tuple.
TPlanNode file_scan_plan_node() {
    TPlanNode plan_node;
    plan_node.node_id = 0;
    plan_node.node_type = TPlanNodeType::FILE_SCAN_NODE;
    plan_node.num_children = 0;
    plan_node.limit = -1;
    plan_node.row_tuples.push_back(0);
    plan_node.file_scan_node.tuple_id = 0;
    plan_node.__isset.file_scan_node = true;
    return plan_node;
}

TFileScanRangeParams scan_level_format(const std::string& table_format) {
    TFileScanRangeParams params;
    if (!table_format.empty()) {
        TTableFormatFileDesc fmt;
        fmt.__set_table_format_type(table_format);
        params.__set_table_format_params(fmt);
    }
    return params;
}

TFileRangeDesc jni_range(const std::string& table_format) {
    TTableFormatFileDesc fmt;
    fmt.__set_table_format_type(table_format);
    TFileRangeDesc range;
    range.__set_format_type(TFileFormatType::FORMAT_JNI);
    range.__set_table_format_params(fmt);
    return range;
}

void expect_scanners_share_node_cache(FileScanLocalState* local_state, MockRuntimeState* state,
                                      FileScanOperatorX* node) {
    std::list<ScannerSPtr> scanners;
    ASSERT_TRUE(local_state->_init_scanners(&scanners).ok());
    ASSERT_FALSE(scanners.empty());
    for (const auto& scanner : scanners) {
        const auto* file_scanner = dynamic_cast<FileScannerV2*>(scanner.get());
        ASSERT_NE(file_scanner, nullptr);
        EXPECT_EQ(file_scanner->_kv_cache, node->_kv_cache.get());
    }
}

} // namespace

// All instances of a node share cached log-tail and delete-file data. A per-instance cache
// would reload the same entry when splits referencing it are assigned to different instances.
TEST(FileScanOperatorCacheTest, EveryInstanceOfTheNodeReadsThroughTheNodesCache) {
    ObjectPool pool;
    DescriptorTbl* descriptors = nullptr;
    ASSERT_TRUE(create_one_tuple_descriptors(&pool, &descriptors).ok());
    MockRuntimeState state;
    state.set_desc_tbl(descriptors);

    const TPlanNode plan_node = file_scan_plan_node();
    FileScanOperatorX node(&pool, plan_node, 0, *descriptors, /*parallel_tasks=*/1);
    ASSERT_TRUE(node.init(plan_node, &state).ok());
    ASSERT_TRUE(node.prepare(&state).ok());
    ASSERT_NE(node._kv_cache, nullptr);

    TFileRangeDesc log_range = jni_range("fluss");
    log_range.table_format_params.__set_fluss_params({{"fluss.range_type", "LOG"}});
    TFileScanRange file_scan_range;
    file_scan_range.__set_params(scan_level_format("fluss"));
    // A query reads no source tuple; tuple 0 there would make this scan a load.
    file_scan_range.params.__set_src_tuple_id(1);
    file_scan_range.ranges.push_back(log_range);
    TScanRangeParams scan_range;
    scan_range.scan_range.ext_scan_range.__set_file_scan_range(file_scan_range);
    scan_range.scan_range.__isset.ext_scan_range = true;
    const std::vector<TScanRangeParams> scan_ranges {scan_range};

    RuntimeProfile profile("EveryInstanceOfTheNodeReadsThroughTheNodesCache");
    const std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                  std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    for (int instance = 0; instance < 2; ++instance) {
        SCOPED_TRACE(instance);
        auto local_state = FileScanLocalState::create_unique(&state, &node);
        LocalStateInfo info {.parent_profile = &profile,
                             .scan_ranges = scan_ranges,
                             .shared_state = nullptr,
                             .shared_state_map = shared_state_map,
                             .task_idx = instance};
        ASSERT_TRUE(local_state->init(&state, info).ok());
        ASSERT_NO_FATAL_FAILURE(expect_scanners_share_node_cache(local_state.get(), &state, &node));
    }
}

// An unlimited scanner setting can overflow an int when multiplied by the instance count.
// Shard count must instead be capped by remote scan threads, preserving concurrency across instances.
TEST(FileScanOperatorCacheTest, CacheShardsOfAnUnlimitedScannerSettingAreCappedNotOverflowed) {
    ObjectPool pool;
    DescriptorTbl* descriptors = nullptr;
    ASSERT_TRUE(create_one_tuple_descriptors(&pool, &descriptors).ok());
    MockRuntimeState state;
    state.set_desc_tbl(descriptors);
    state._query_options.__set_parallel_instance(2);
    state._query_options.__set_max_file_scanners_concurrency(std::numeric_limits<int32_t>::max());

    const TPlanNode plan_node = file_scan_plan_node();
    FileScanOperatorX node(&pool, plan_node, 0, *descriptors, /*parallel_tasks=*/2);
    ASSERT_TRUE(node.init(plan_node, &state).ok());
    ASSERT_TRUE(node.prepare(&state).ok());
    EXPECT_EQ(node._kv_cache->_num_shards,
              static_cast<uint32_t>(ScannerScheduler::default_remote_scan_thread_num()));
}

} // namespace doris::pipeline
