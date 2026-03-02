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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "vec/data_types/data_type.h"
#include "vec/json/path_in_data.h"

namespace roaring {
class Roaring;
}

namespace doris {
class TabletColumn;
class TabletSchema;
class StorageReadOptions;
namespace io {
class FileReader;
} // namespace io
namespace vectorized {
class ColumnVariant;
class OlapBlockDataConvertor;
} // namespace vectorized
} // namespace doris

namespace doris::segment_v2 {

struct ColumnWriterOptions;
struct ColumnReaderOptions;
struct VariantStatistics;
struct NestedGroupReader;
class SegmentFooterPB;

// Path filter for selecting specific children in NestedGroup reads.
// Used to prune unnecessary child column I/O during query execution.
struct NestedGroupPathFilter {
    bool allow_all = false;
    std::unordered_set<std::string> allowed_paths;

    bool empty() const { return !allow_all && allowed_paths.empty(); }

    void set_allow_all() {
        allow_all = true;
        allowed_paths.clear();
    }

    void add_path(std::string path) {
        if (!path.empty()) {
            allowed_paths.emplace(std::move(path));
        }
    }

    bool matches_child(const std::string& name) const {
        if (allow_all) {
            return true;
        }
        if (allowed_paths.contains(name)) {
            return true;
        }
        std::string prefix = name + ".";
        return std::ranges::any_of(allowed_paths, [&](const auto& path) {
            return path.starts_with(prefix) || name.starts_with(path + ".");
        });
    }

    NestedGroupPathFilter sub_filter(const std::string& prefix) const {
        NestedGroupPathFilter sub;
        if (allow_all) {
            sub.allow_all = true;
            return sub;
        }
        std::string prefix_dot = prefix + ".";
        for (const auto& path : allowed_paths) {
            if (path == prefix) {
                sub.set_allow_all();
                return sub;
            }
            if (path.starts_with(prefix_dot)) {
                sub.add_path(path.substr(prefix_dot.size()));
            }
        }
        return sub;
    }
};

// Map from array path to NestedGroupReader.
// Defined here so both provider and reader can use it.
using NestedGroupReaders = std::unordered_map<std::string, std::unique_ptr<NestedGroupReader>>;

struct NestedGroupPathMatch {
    const NestedGroupReader* reader = nullptr;
    std::vector<const NestedGroupReader*> chain;
    std::string child_path;
    bool found = false;
};

// Shared resolver for nested-group path matching used by both reader and provider.
NestedGroupPathMatch find_in_nested_groups(const NestedGroupReaders& readers,
                                           const std::string& path, bool collect_chain);

// --------------------------------------------------------------------------
// Write provider
// --------------------------------------------------------------------------

// Extension point for NestedGroup write path.
// Concrete behavior is selected by create_nested_group_write_provider().
// The default provider is a no-op placeholder.
// Downstream integrations may provide a full implementation that expands JSONB
// into NestedGroup columns with auxiliary indexes.
class NestedGroupWriteProvider {
public:
    virtual ~NestedGroupWriteProvider() = default;

    virtual Status prepare(const vectorized::ColumnVariant& variant, bool include_jsonb_subcolumns,
                           const TabletColumn* tablet_column, const ColumnWriterOptions& opts,
                           vectorized::OlapBlockDataConvertor* converter, size_t num_rows,
                           int* column_id, VariantStatistics* statistics) = 0;

    virtual uint64_t estimate_buffer_size() const = 0;

    virtual Status finish() = 0;
    virtual Status write_data() = 0;
    virtual Status write_ordinal_index() = 0;
    virtual Status write_zone_map() = 0;
    virtual Status write_inverted_index() = 0;
    virtual Status write_bloom_filter_index() = 0;
};

// --------------------------------------------------------------------------
// Read provider
// --------------------------------------------------------------------------

// Extension point for NestedGroup read path.
// Concrete behavior is selected by create_nested_group_read_provider().
// The default provider is disabled and falls back to generic variant reads.
// Downstream integrations may provide a full implementation for nested
// array<object> access.
class NestedGroupReadProvider {
public:
    virtual ~NestedGroupReadProvider() = default;

    // Whether the NestedGroup read path is available in this build.
    virtual bool should_enable_nested_group_read_path() const = 0;

    // --- Reader initialization ---
    // Scan segment footer for NestedGroup columns and populate |out_readers|.
    // Called once per VariantColumnReader::init().
    virtual Status init_readers(const ColumnReaderOptions& opts,
                                const std::shared_ptr<SegmentFooterPB>& footer,
                                const std::shared_ptr<io::FileReader>& file_reader,
                                uint64_t num_rows, NestedGroupReaders& out_readers) = 0;

    // --- Read planning ---
    // Determines if |relative_path| should be read via the NestedGroup path and if so
    // fills output parameters. Returns true if a NestedGroup plan was built.
    //
    // Output parameters (only valid when returning true):
    //   out_is_whole       — true for WHOLE access, false for CHILD access
    //   out_type           — resulting data type
    //   out_relative_path  — the relative path in the plan
    //   out_child_path     — child path within NestedGroup (for CHILD)
    //   out_pruned_path    — pruned prefix for object reconstruction (for WHOLE)
    //   out_chain          — nested group reader chain from outermost to innermost
    //   out_path_filter    — optional path filter for pruning child columns
    virtual bool try_build_read_plan(
            const TabletSchema* tablet_schema, const NestedGroupReaders& readers,
            const TabletColumn& target_col, const StorageReadOptions* opt, int32_t col_uid,
            const vectorized::PathInData& relative_path,
            // outputs:
            bool* out_is_whole, vectorized::DataTypePtr* out_type,
            vectorized::PathInData* out_relative_path, std::string* out_child_path,
            std::string* out_pruned_path, std::vector<const NestedGroupReader*>* out_chain,
            std::optional<NestedGroupPathFilter>* out_path_filter) const = 0;

    // --- Iterator creation ---
    // Create a ColumnIterator for a NestedGroup read plan.
    // For WHOLE access: creates an iterator that reconstructs array<variant> from element columns.
    // For CHILD access: creates an iterator that reads a single child column with array offsets.
    virtual Status create_nested_group_iterator(
            bool is_whole, const std::vector<const NestedGroupReader*>& chain,
            const std::string& child_path, const std::string& pruned_path,
            const std::optional<NestedGroupPathFilter>& path_filter, ColumnIteratorUPtr* out_iter,
            vectorized::DataTypePtr* out_type) = 0;

    // --- Search support ---
    // Get total number of elements in the innermost group of the chain.
    virtual Status get_total_elements(const ColumnIteratorOptions& opts,
                                      const NestedGroupReader* leaf_group,
                                      uint64_t* total_elements) const = 0;

    // Map element-level bitmap to row-level bitmap through the nested group chain.
    virtual Status map_elements_to_parent_ords(
            const std::vector<const NestedGroupReader*>& group_chain,
            const ColumnIteratorOptions& opts, const roaring::Roaring& element_bitmap,
            roaring::Roaring* parent_bitmap) const = 0;
};

// Factory functions. Implementations are selected at link time.
// The default providers are disabled/no-op placeholders.
std::unique_ptr<NestedGroupWriteProvider> create_nested_group_write_provider();
std::unique_ptr<NestedGroupReadProvider> create_nested_group_read_provider();

} // namespace doris::segment_v2
