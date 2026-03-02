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

#include "olap/rowset/segment_v2/variant/nested_group_provider.h"

#include <string>

namespace doris::segment_v2 {

namespace {

// --------------------------------------------------------------------------
// Default write provider — no-op placeholder.
// TODO: provide a full implementation that expands JSONB into NestedGroup
//       columns with auxiliary indexes.
// --------------------------------------------------------------------------
class DefaultNestedGroupWriteProvider final : public NestedGroupWriteProvider {
public:
    Status prepare(const vectorized::ColumnVariant& /*variant*/, bool /*include_jsonb_subcolumns*/,
                   const TabletColumn* /*tablet_column*/, const ColumnWriterOptions& /*opts*/,
                   vectorized::OlapBlockDataConvertor* /*converter*/, size_t /*num_rows*/,
                   int* /*column_id*/, VariantStatistics* /*statistics*/) override {
        // No-op: NestedGroup write is not available in this build.
        return Status::OK();
    }

    uint64_t estimate_buffer_size() const override { return 0; }

    Status finish() override { return Status::OK(); }
    Status write_data() override { return Status::OK(); }
    Status write_ordinal_index() override { return Status::OK(); }
    Status write_zone_map() override { return Status::OK(); }
    Status write_inverted_index() override { return Status::OK(); }
    Status write_bloom_filter_index() override { return Status::OK(); }
};

// --------------------------------------------------------------------------
// Default read provider — disabled placeholder.
// TODO: provide a full implementation for nested array<object> access.
// --------------------------------------------------------------------------
class DefaultNestedGroupReadProvider final : public NestedGroupReadProvider {
public:
    bool should_enable_nested_group_read_path() const override {
        // Disabled: NestedGroup read path is not available in this build.
        return false;
    }

    Status init_readers(const ColumnReaderOptions& /*opts*/,
                        const std::shared_ptr<SegmentFooterPB>& /*footer*/,
                        const std::shared_ptr<io::FileReader>& /*file_reader*/,
                        uint64_t /*num_rows*/, NestedGroupReaders& /*out_readers*/) override {
        return Status::OK();
    }

    bool try_build_read_plan(
            const TabletSchema* /*tablet_schema*/, const NestedGroupReaders& /*readers*/,
            const TabletColumn& /*target_col*/, const StorageReadOptions* /*opt*/,
            int32_t /*col_uid*/, const vectorized::PathInData& /*relative_path*/,
            // outputs:
            bool* /*out_is_whole*/, vectorized::DataTypePtr* /*out_type*/,
            vectorized::PathInData* /*out_relative_path*/, std::string* /*out_child_path*/,
            std::string* /*out_pruned_path*/, std::vector<const NestedGroupReader*>* /*out_chain*/,
            std::optional<NestedGroupPathFilter>* /*out_path_filter*/) const override {
        // Always returns false: NestedGroup read planning is not available.
        return false;
    }

    Status create_nested_group_iterator(bool /*is_whole*/,
                                        const std::vector<const NestedGroupReader*>& /*chain*/,
                                        const std::string& /*child_path*/,
                                        const std::string& /*pruned_path*/,
                                        const std::optional<NestedGroupPathFilter>& /*path_filter*/,
                                        ColumnIteratorUPtr* /*out_iter*/,
                                        vectorized::DataTypePtr* /*out_type*/) override {
        return Status::NotSupported("NestedGroup iterator is not available in this build");
    }

    Status get_total_elements(const ColumnIteratorOptions& /*opts*/,
                              const NestedGroupReader* /*leaf_group*/,
                              uint64_t* /*total_elements*/) const override {
        return Status::NotSupported("NestedGroup element access is not available in this build");
    }

    Status map_elements_to_parent_ords(const std::vector<const NestedGroupReader*>& /*group_chain*/,
                                       const ColumnIteratorOptions& /*opts*/,
                                       const roaring::Roaring& /*element_bitmap*/,
                                       roaring::Roaring* /*parent_bitmap*/) const override {
        return Status::NotSupported(
                "NestedGroup element-to-parent mapping is not available in this build");
    }
};

} // namespace

NestedGroupPathMatch find_in_nested_groups(const NestedGroupReaders& readers,
                                           const std::string& path, bool collect_chain) {
    // Default implementation: no nested groups are populated, so nothing matches.
    return {};
}

std::unique_ptr<NestedGroupWriteProvider> create_nested_group_write_provider() {
    return std::make_unique<DefaultNestedGroupWriteProvider>();
}

std::unique_ptr<NestedGroupReadProvider> create_nested_group_read_provider() {
    return std::make_unique<DefaultNestedGroupReadProvider>();
}

} // namespace doris::segment_v2
