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

#pragma once

#include <butil/iobuf.h>
#include <gen_cpp/internal_service.pb.h>

#include "runtime/define_primitive_type.h"

namespace doris {

enum class RuntimeFilterType {
    UNKNOWN_FILTER,
    IN_FILTER,
    MINMAX_FILTER,
    BLOOM_FILTER,
    IN_OR_BLOOM_FILTER,
    BITMAP_FILTER,
    MIN_FILTER, // only min
    MAX_FILTER  // only max
};

struct RuntimeFilterParams {
    // Filter ID
    int32_t filter_id {};
    // Filter type
    RuntimeFilterType filter_type {};
    // Data type of build column
    PrimitiveType column_return_type {};
    bool null_aware {};
    bool enable_fixed_len_to_uint32_v2 {};

    // The max limitation of in-set
    int32_t max_in_num {};
    // The min size limitation of bloom filter
    int64_t runtime_bloom_filter_min_size {};
    // The max size limitation of bloom filter
    int64_t runtime_bloom_filter_max_size {};
    // Size of bloom filter which is estimated by FE using NDV
    int64_t bloom_filter_size {};
    // Whether a runtime size is used to build bloom filter
    bool build_bf_by_runtime_size {};
    // Whether an estimated size by NDV is used to build bloom filter
    bool bloom_filter_size_calculated_by_ndv {};


    bool bitmap_filter_not_in {};
};

class MinMaxFuncBase;
class HybridSetBase;
class BloomFilterFuncBase;
class BitmapFilterFuncBase;

class RuntimeState;
class QueryContext;
class RuntimeFilterMgr;
// There are two types of runtime filters:
// 1. Global runtime filter. Managed by QueryContext's RuntimeFilterMgr which is produced by multiple producers and shared by multiple consumers.
// 2. Local runtime filter. Managed by RuntimeState's RuntimeFilterMgr which is 1-producer-1-consumer mode.
struct RuntimeFilterParamsContext {
    static RuntimeFilterParamsContext* create(RuntimeState* state);
    static RuntimeFilterParamsContext* create(QueryContext* query_ctx);

    QueryContext* get_query_ctx() const { return _query_ctx; }
    RuntimeState* get_runtime_state() const { return _state; }
    void set_state(RuntimeState* state) { _state = state; }
    RuntimeFilterMgr* global_runtime_filter_mgr();
    RuntimeFilterMgr* local_runtime_filter_mgr();

private:
    RuntimeFilterParamsContext() = default;

    QueryContext* _query_ctx;
    RuntimeState* _state;
};

} // namespace doris
