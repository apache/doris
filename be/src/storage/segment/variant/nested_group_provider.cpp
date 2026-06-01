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

#include "storage/segment/variant/nested_group_provider.h"

namespace doris::segment_v2 {

namespace {

NestedGroupPathMatch build_path_match(const NestedGroupReader* reader, std::string child_path,
                                      bool collect_chain) {
    NestedGroupPathMatch result;
    result.reader = reader;
    result.child_path = std::move(child_path);
    result.found = true;
    if (collect_chain) {
        result.chain.push_back(reader);
    }
    return result;
}

void maybe_prepend_chain(NestedGroupPathMatch* result, const NestedGroupReader* reader,
                         bool collect_chain) {
    if (result == nullptr || !result->found || !collect_chain) {
        return;
    }
    result->chain.insert(result->chain.begin(), reader);
}

bool has_child_or_nested_prefix(const NestedGroupReader& reader, const std::string& remaining) {
    const std::string remaining_dot = remaining + ".";
    const bool has_child_prefix =
            std::any_of(reader.child_readers.begin(), reader.child_readers.end(),
                        [&](const auto& e) { return e.first.starts_with(remaining_dot); });
    if (has_child_prefix) {
        return true;
    }
    return std::any_of(reader.nested_group_readers.begin(), reader.nested_group_readers.end(),
                       [&](const auto& e) { return e.first.starts_with(remaining_dot); });
}

NestedGroupPathMatch find_in_nested_groups_impl(const NestedGroupReaders& readers,
                                                const std::string& path, bool collect_chain) {
    if (path.empty()) {
        return {};
    }

    const std::string root_path(kRootNestedGroupPath);
    if (auto it = readers.find(root_path); it != readers.end()) {
        const auto& root_reader = it->second;
        if (root_reader && root_reader->is_valid()) {
            if (path == root_path) {
                return build_path_match(root_reader.get(), {}, collect_chain);
            }
            if (root_reader->child_readers.contains(path)) {
                return build_path_match(root_reader.get(), path, collect_chain);
            }
            auto nested = find_in_nested_groups_impl(root_reader->nested_group_readers, path,
                                                     collect_chain);
            if (nested.found) {
                maybe_prepend_chain(&nested, root_reader.get(), collect_chain);
                return nested;
            }
        }
    }

    for (const auto& [ng_path, reader] : readers) {
        if (ng_path == root_path) {
            continue;
        }
        if (!reader || !reader->is_valid()) {
            continue;
        }

        if (path == ng_path) {
            return build_path_match(reader.get(), {}, collect_chain);
        }

        const std::string prefix = ng_path + ".";
        if (path.size() <= prefix.size() || !path.starts_with(prefix)) {
            continue;
        }

        std::string remaining = path.substr(prefix.size());
        if (reader->child_readers.contains(remaining)) {
            return build_path_match(reader.get(), std::move(remaining), collect_chain);
        }

        auto nested =
                find_in_nested_groups_impl(reader->nested_group_readers, remaining, collect_chain);
        if (nested.found) {
            maybe_prepend_chain(&nested, reader.get(), collect_chain);
            return nested;
        }

        if (has_child_or_nested_prefix(*reader, remaining)) {
            return build_path_match(reader.get(), std::move(remaining), collect_chain);
        }
    }
    return {};
}

class DefaultNestedGroupWriteProvider final : public NestedGroupWriteProvider {
public:
    Status prepare(const ColumnVariant& /*variant*/, const TabletColumn* tablet_column,
                   const ColumnWriterOptions& /*opts*/, OlapBlockDataConvertor* converter,
                   int* column_id, VariantStatistics* statistics) override {
        if (tablet_column == nullptr || converter == nullptr || column_id == nullptr ||
            statistics == nullptr) {
            return Status::InvalidArgument("NestedGroup provider input is null");
        }
        return Status::OK();
    }

    Status prepare_with_built_groups(const NestedGroupsMap& /*nested_groups*/,
                                     const TabletColumn* tablet_column,
                                     const ColumnWriterOptions& /*opts*/,
                                     OlapBlockDataConvertor* converter, int* column_id,
                                     VariantStatistics* statistics) override {
        if (tablet_column == nullptr || converter == nullptr || column_id == nullptr ||
            statistics == nullptr) {
            return Status::InvalidArgument("NestedGroup provider input is null");
        }
        return Status::OK();
    }

    Status init_with_plan(const NestedGroupStreamingWritePlan& /*plan*/,
                          const TabletColumn* tablet_column, const ColumnWriterOptions& /*opts*/,
                          int* column_id, VariantStatistics* statistics) override {
        if (tablet_column == nullptr || column_id == nullptr || statistics == nullptr) {
            return Status::InvalidArgument("NestedGroup streaming init input is null");
        }
        return Status::OK();
    }

    Status append_chunk(const NestedGroupStreamingWritePlan& /*plan*/,
                        const ColumnVariant& /*variant*/) override {
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

class DefaultNestedGroupReadProvider final : public NestedGroupReadProvider {
public:
    bool should_enable_nested_group_read_path() const override { return false; }

    Status init_readers(const ColumnReaderOptions& /*opts*/,
                        const std::shared_ptr<SegmentFooterPB>& /*footer*/,
                        const std::shared_ptr<io::FileReader>& /*file_reader*/,
                        ColumnMetaAccessor* /*accessor*/, int32_t /*root_unique_id*/,
                        uint64_t /*num_rows*/, NestedGroupReaders& /*out_readers*/) override {
        return Status::OK();
    }

    bool try_build_read_plan(
            const TabletSchema* /*tablet_schema*/, const NestedGroupReaders& /*readers*/,
            const TabletColumn& /*target_col*/, const StorageReadOptions* /*opt*/,
            int32_t /*col_uid*/, const PathInData& /*relative_path*/, bool* /*out_is_whole*/,
            DataTypePtr* /*out_type*/, PathInData* /*out_relative_path*/,
            std::string* /*out_child_path*/, std::string* /*out_pruned_path*/,
            std::vector<const NestedGroupReader*>* /*out_chain*/,
            std::optional<NestedGroupPathFilter>* /*out_path_filter*/) const override {
        return false;
    }

    Status create_nested_group_iterator(bool /*is_whole*/,
                                        const std::vector<const NestedGroupReader*>& /*chain*/,
                                        const std::string& /*child_path*/,
                                        const std::string& /*pruned_path*/,
                                        const std::optional<NestedGroupPathFilter>& /*path_filter*/,
                                        ColumnIteratorUPtr* /*out_iter*/,
                                        DataTypePtr* /*out_type*/) override {
        return Status::NotSupported("NestedGroup iterator is not available in this build");
    }

    Status get_total_elements(const ColumnIteratorOptions& /*opts*/,
                              const NestedGroupReader* /*leaf_group*/,
                              uint64_t* /*total_elements*/) const override {
        return Status::NotSupported("NestedGroup element access is not available in this build");
    }

    Status create_root_merge_iterator(ColumnIteratorUPtr base_iterator,
                                      const NestedGroupReaders& /*readers*/,
                                      const StorageReadOptions* /*opt*/,
                                      ColumnIteratorUPtr* out) override {
        if (out == nullptr) {
            return Status::InvalidArgument("out is null");
        }
        *out = std::move(base_iterator);
        return Status::OK();
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
    return find_in_nested_groups_impl(readers, path, collect_chain);
}

Status build_nested_groups_from_variant_jsonb(const ColumnVariant& /*variant*/,
                                              NestedGroupsMap* nested_groups,
                                              std::vector<std::string>* out_ng_paths,
                                              std::vector<std::string>* out_conflict_paths) {
    if (nested_groups == nullptr) {
        return Status::InvalidArgument("nested_groups is null");
    }
    nested_groups->clear();
    if (out_ng_paths != nullptr) {
        out_ng_paths->clear();
    }
    if (out_conflict_paths != nullptr) {
        out_conflict_paths->clear();
    }
    return Status::OK();
}

Status collect_nested_group_routing_paths_from_variant_jsonb(
        const ColumnVariant& variant, std::vector<std::string>* out_ng_paths,
        std::vector<std::string>* out_conflict_paths) {
    if (out_ng_paths == nullptr || out_conflict_paths == nullptr) {
        return Status::InvalidArgument("out_ng_paths or out_conflict_paths is null");
    }
    NestedGroupsMap nested_groups;
    return build_nested_groups_from_variant_jsonb(variant, &nested_groups, out_ng_paths,
                                                  out_conflict_paths);
}

std::unique_ptr<NestedGroupWriteProvider> create_nested_group_write_provider() {
    return std::make_unique<DefaultNestedGroupWriteProvider>();
}

std::unique_ptr<NestedGroupReadProvider> create_nested_group_read_provider() {
    return std::make_unique<DefaultNestedGroupReadProvider>();
}

} // namespace doris::segment_v2
