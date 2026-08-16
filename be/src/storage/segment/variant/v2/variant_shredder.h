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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

#include "common/consts.h"
#include "common/status.h"
#include "core/column/column.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type.h"
#include "storage/segment/variant/v2/variant_path_builder.h"
#include "storage/segment/variant/variant_statistics.h"

namespace doris {
class TabletSchema;
}

namespace doris::segment_v2 {

enum class VariantShredderPhysicalLayout : uint8_t {
    ORDINARY,
    DOC,
};

struct VariantShredderOptions {
    const TabletSchema* tablet_schema = nullptr;
    int32_t parent_column_unique_id = -1;
    VariantShredderPhysicalLayout physical_layout = VariantShredderPhysicalLayout::ORDINARY;
    // Zero retains all dynamic paths, matching variant_max_subcolumns_count semantics.
    size_t max_subcolumns_count = 0;
    bool typed_paths_to_sparse = false;
    uint32_t sparse_bucket_count = 1;
    size_t max_sparse_column_statistics_size =
            BeConsts::DEFAULT_VARIANT_MAX_SPARSE_COLUMN_STATS_SIZE;
    uint32_t doc_bucket_count = 1;
    size_t doc_materialization_min_rows = 0;
    bool check_duplicate_json_path = false;
};

struct VariantShreddedColumns {
    size_t num_rows = 0;
    ColumnPtr root_jsonb;
    DorisVector<VariantPathColumn> materialized;
    struct BinaryBucket {
        ColumnPtr column;
        VariantStatistics statistics;
    };
    DorisVector<BinaryBucket> binary_buckets;
    VariantStatistics statistics;
};

struct VariantShredderAppendStats {
    size_t native_shredded_rows = 0;
    size_t encoded_fallback_rows = 0;
};

// Incremental native Variant V2 shredder. Encoded E state is consumed directly. Shredded S state
// keeps its residual and active scalar fields separate on the ordinary path; only a row whose
// logical paths are ambiguous in the legacy dotted storage namespace is re-encoded. A failed
// append or finish is terminal: the first error is retained, all later calls return it, and finish
// never publishes a partial result.
class VariantShredder final {
public:
    explicit VariantShredder(VariantShredderOptions options);
    ~VariantShredder();

    VariantShredder(VariantShredder&&) noexcept;
    VariantShredder& operator=(VariantShredder&&) noexcept;
    VariantShredder(const VariantShredder&) = delete;
    VariantShredder& operator=(const VariantShredder&) = delete;

    Status append(const ColumnVariantV2::ReadView& view, size_t begin, size_t length,
                  std::span<const uint8_t> outer_nulls = {});
    Status append_shredded(const ColumnVariantV2& source, size_t begin, size_t length,
                           std::span<const uint8_t> outer_nulls = {},
                           VariantShredderAppendStats* append_stats = nullptr);
    Status finish(VariantShreddedColumns* output);
    size_t byte_size() const;

#ifdef BE_TEST
    struct TestAccess {
        static size_t typed_direct_scalar_appends(const VariantShredder& shredder);
        static size_t typed_encoded_slow_appends(const VariantShredder& shredder);
        static size_t native_residual_value_walks(const VariantShredder& shredder);
    };
#endif

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace doris::segment_v2
