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

#include "runtime/define_primitive_type.h"

namespace doris {
#include "common/compile_check_begin.h"
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
    // Whether this runtime filter is null-aware
    bool null_aware {};

    // In filter
    // The max limitation of in-set
    int32_t max_in_num {};

    // Bloom filter
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

    // Bitmap filter
    // Whether a join expression is `not in`
    bool bitmap_filter_not_in {};
};

class MinMaxFuncBase;
class HybridSetBase;
class BloomFilterFuncBase;
class BitmapFilterFuncBase;
#include "common/compile_check_end.h"
} // namespace doris
