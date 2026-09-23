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
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_profile.h"

struct LanceBatch;
struct LanceDataset;
struct LanceScanner;

namespace arrow {
class Array;
class DataType;
class Field;
class MemoryPool;
class Schema;
} // namespace arrow

namespace doris::format::lance {

inline constexpr std::string_view LANCE_DISTANCE_COLUMN = "_distance";
inline constexpr std::string_view LANCE_SCORE_COLUMN = "_score";
inline constexpr std::string_view LANCE_ROW_ID_COLUMN = "_rowid";
inline constexpr const char* LANCE_READER_PROFILE = "LanceReader";
inline constexpr const char* LANCE_TIMING_PROFILE = "LanceTiming";
inline constexpr const char* LANCE_IO_PROFILE = "LanceIO";
inline constexpr const char* LANCE_INDEX_PROFILE = "LanceIndex";
inline constexpr const char* LANCE_VECTOR_INDEX_PROFILE = "LanceVectorIndex";
inline constexpr const char* LANCE_SCALAR_INDEX_PROFILE = "LanceScalarIndex";
inline constexpr const char* LANCE_SCAN_PROFILE = "LanceScan";
inline constexpr const char* LANCE_SEARCH_PLAN_PROFILE = "LanceSearchPlan";
inline constexpr const char* LANCE_DATA_CACHE_PROFILE = "LanceDataCache";

struct LanceProfileMetric {
    std::string_view native_name;
    const char* profile_name;
    const char* group;
    TUnit::type unit;
};

// Keep this mapping immutable: callbacks for native execution streams may run concurrently.
// Register optional metrics only when Lance reports them, so absent paths do not appear as
// successful zero-cost operations. A reported zero is still meaningful and is retained.
inline constexpr LanceProfileMetric LANCE_SCAN_METRICS[] = {
        {.native_name = "index_cache_hits",
         .profile_name = "LanceIndexCacheHits",
         .group = LANCE_INDEX_PROFILE,
         .unit = TUnit::UNIT},
        {.native_name = "index_cache_misses",
         .profile_name = "LanceIndexCacheMisses",
         .group = LANCE_INDEX_PROFILE,
         .unit = TUnit::UNIT},
        // FilteredRead input, which can describe a vector search's row-id prefilter.
        {.native_name = "fragments_scanned",
         .profile_name = "LanceFragmentsScanned",
         .group = LANCE_SCAN_PROFILE,
         .unit = TUnit::UNIT},
        {.native_name = "ranges_scanned",
         .profile_name = "LanceRowOffsetRangesScanned",
         .group = LANCE_SCAN_PROFILE,
         .unit = TUnit::UNIT},
        {.native_name = "rows_scanned",
         .profile_name = "LanceRowsScanned",
         .group = LANCE_SCAN_PROFILE,
         .unit = TUnit::UNIT},
        {.native_name = "task_wait_time",
         .profile_name = "LanceScanTaskWaitTime",
         .group = LANCE_SCAN_PROFILE,
         .unit = TUnit::TIME_NS},
        {.native_name = "partitions_ranked",
         .profile_name = "LanceIVFPartitionsRanked",
         .group = LANCE_VECTOR_INDEX_PROFILE,
         .unit = TUnit::UNIT},
        // Lance also emits partitions_searched for FTS; it is not exclusively an IVF metric.
        {.native_name = "partitions_searched",
         .profile_name = "LanceIndexPartitionsSearched",
         .group = LANCE_INDEX_PROFILE,
         .unit = TUnit::UNIT},
        {.native_name = "deltas_searched",
         .profile_name = "LanceVectorIndexSegmentsAccessed",
         .group = LANCE_VECTOR_INDEX_PROFILE,
         .unit = TUnit::UNIT},
        {.native_name = "find_partitions_elapsed",
         .profile_name = "LanceIVFPartitionRankingTime",
         .group = LANCE_VECTOR_INDEX_PROFILE,
         .unit = TUnit::TIME_NS},
        {.native_name = "scalar_segments_requested",
         .profile_name = "LanceScalarIndexScanRequests",
         .group = LANCE_SCALAR_INDEX_PROFILE,
         .unit = TUnit::UNIT},
        {.native_name = "scalar_segments_searched",
         .profile_name = "LanceScalarIndexSegmentsSearched",
         .group = LANCE_SCALAR_INDEX_PROFILE,
         .unit = TUnit::UNIT},
        {.native_name = "scalar_segment_fallbacks",
         .profile_name = "LanceScalarIndexSegmentFallbacks",
         .group = LANCE_SCALAR_INDEX_PROFILE,
         .unit = TUnit::UNIT},
        {.native_name = "scalar_segment_candidate_rows",
         .profile_name = "LanceScalarIndexCandidateRows",
         .group = LANCE_SCALAR_INDEX_PROFILE,
         .unit = TUnit::UNIT},
        {.native_name = "scalar_segment_prepare_time",
         .profile_name = "LanceScalarIndexCandidateBuildTime",
         .group = LANCE_SCALAR_INDEX_PROFILE,
         .unit = TUnit::TIME_NS},
        {.native_name = "scalar_segment_search_time",
         .profile_name = "LanceScalarIndexSegmentSearchTime",
         .group = LANCE_SCALAR_INDEX_PROFILE,
         .unit = TUnit::TIME_NS},
        {.native_name = "search_time",
         .profile_name = "LanceScalarIndexQueryTime",
         .group = LANCE_SCALAR_INDEX_PROFILE,
         .unit = TUnit::TIME_NS},
        {.native_name = "serialization_time",
         .profile_name = "LanceScalarIndexResultSerializationTime",
         .group = LANCE_SCALAR_INDEX_PROFILE,
         .unit = TUnit::TIME_NS},
};

RuntimeProfile::Counter* add_lance_counter(RuntimeProfile* profile, const char* name,
                                           TUnit::type unit, const char* group);

struct LanceDatasetDeleter {
    void operator()(LanceDataset* dataset) const;
};

struct LanceScannerDeleter {
    void operator()(LanceScanner* scanner) const;
};

struct LanceBatchDeleter {
    void operator()(LanceBatch* batch) const;
};

size_t lance_vector_element_width(TVectorElementType::type type);

// Import the physical Arrow schema owned by a Lance dataset. The caller owns the resulting
// shared schema and may cache it for operations that need physical Arrow types.
Status import_lance_dataset_schema(LanceDataset* dataset, std::shared_ptr<arrow::Schema>* schema);

// Validate and convert the fragment and index-segment identifiers carried by the FE into the
// unsigned and packed representations expected by lance-c.
Status parse_fragment_ids(const TLanceFileDesc& lance_params, std::vector<uint64_t>* fragment_ids);
Status parse_index_segment_uuids(const TLanceFileDesc& lance_params,
                                 std::vector<uint8_t>* segment_uuids, size_t* segment_count);

// Resolves one Arrow field once when a stream schema is bound. Runtime conversion then reuses this
// plan instead of rescanning extension metadata and nested fields for every array.
class LanceArrowArrayNormalizer {
public:
    static Status create(const std::shared_ptr<arrow::Field>& field,
                         LanceArrowArrayNormalizer* normalizer);

    // Return an Arrow array whose physical layout and type can be consumed by Doris SerDes. The
    // input is returned unchanged when no compaction or type adaptation is required.
    Status normalize_for_doris(const std::shared_ptr<arrow::Array>& array,
                               arrow::MemoryPool* memory_pool,
                               std::shared_ptr<arrow::Array>* normalized) const;

private:
    std::string _field_name;
    std::shared_ptr<arrow::DataType> _storage_type;
    std::vector<LanceArrowArrayNormalizer> _child_normalizers;
    bool _unwrap_registered_extension = false;
    bool _convert_bfloat16 = false;
    bool _requires_special_handling = false;
};

#ifdef BE_TEST
// Expose Lance Arrow normalization for allocation-sensitive unit tests.
Status normalize_lance_arrow_array_for_test(const std::shared_ptr<arrow::Field>& field,
                                            const std::shared_ptr<arrow::Array>& array,
                                            std::shared_ptr<arrow::Array>* normalized,
                                            arrow::MemoryPool* memory_pool = nullptr);
#endif

// Convert every top-level field without discarding unsupported columns. Malformed schemas still
// return an error and leave both output vectors unchanged. DataTypeNothing is the local sentinel
// for a valid Arrow field whose logical type Doris does not support.
Status convert_arrow_schema_to_doris(const std::shared_ptr<arrow::Schema>& arrow_schema,
                                     std::vector<std::string>* column_names,
                                     std::vector<DataTypePtr>* column_types);

// The FE sends storage options in Lance's own vocabulary. Preserve the key-value sequence exactly
// while validating that every value can cross the C-string boundary into lance-c.
Status build_lance_storage_options(const TFileScanRangeParams* scan_params,
                                   std::vector<std::string>* options);

// Copy and release lance-c's thread-local error message before returning a Doris status.
Status lance_error(std::string_view operation);

} // namespace doris::format::lance
