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

#include "storage/segment/variant/v2/variant_shredder.h"

#include <algorithm>
#include <limits>
#include <numeric>
#include <optional>
#include <unordered_map>
#include <utility>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_map.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_factory.hpp"
#include "exec/common/hash_table/phmap_fwd_decl.h"
#include "exec/common/variant_util.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "storage/tablet/tablet_schema.h"
#include "util/jsonb_writer.h"

namespace doris::segment_v2 {
namespace {

PathInData normalize_doc_publication_path(const PathInData& path) {
    if (path.empty()) {
        return path;
    }
    for (const PathInData::Part& part : path.get_parts()) {
        if (part.is_nested || part.anonymous_array_level != 0) {
            return path;
        }
    }
    return PathInData(path.get_path(), path.get_is_typed());
}

size_t path_allocated_bytes(const PathInData& path) {
    return path.get_path().capacity() + path.get_parts().capacity() * sizeof(PathInData::Part);
}

} // namespace

struct VariantShredder::Impl {
    enum class State : uint8_t { COLLECTING, FINISHED, FAILED };

    using PathIndex = uint32_t;
    using ParentFieldKey = uint64_t;
    using ChildPathCache = doris::flat_hash_map<ParentFieldKey, PathIndex>;
    static constexpr PathIndex UNRESOLVED_PATH = std::numeric_limits<PathIndex>::max();
    static constexpr size_t MAX_BINARY_CELLS_PER_CHUNK = 1U << 20;

    // Metadata bytes belong to the input ReadView, and this cache never escapes
    // one append call. The first observed root field lazily allocates a
    // dictionary-sized direct lookup table; nested parent+field transitions are
    // retained only when observed in the batch. Canonical paths themselves remain
    // owned by PathState across appends.
    struct MetadataPathCache {
        explicit MetadataPathCache(VariantMetadataRef metadata_) : metadata(metadata_) {}

        VariantMetadataRef metadata;
        DorisVector<PathIndex> root_child_paths;
        ChildPathCache nested_child_paths;
    };

    // Keep all state for one canonical dotted path together. This replaces three parallel
    // containers (path plan, builders, and last-row markers), so a path has one index and one
    // lifetime throughout shredding.
    struct PathState {
        explicit PathState(const PathInData& path_) : path(path_) {}

        PathInData path;
        std::optional<VariantPathBuilder> builder;
        size_t last_row_marker = 0;
    };

    struct SparsePlan {
        VariantPathBuilder* builder = nullptr;
        uint32_t bucket = 0;
        const std::string* path = nullptr;
        bool track_statistics = false;
    };

    struct DocPlan {
        VariantPathBuilder* builder = nullptr;
        uint32_t bucket = 0;
        const std::string* path = nullptr;
        size_t candidate_index = 0;
    };

    explicit Impl(VariantShredderOptions options_) : options(std::move(options_)) {
        paths.emplace_back(PathInData());
        if (options.physical_layout == VariantShredderPhysicalLayout::ORDINARY &&
            options.sparse_bucket_count == 0) {
            failure = Status::InvalidArgument(
                    "Variant shredder sparse bucket count must be positive");
            state = State::FAILED;
        } else if (options.physical_layout == VariantShredderPhysicalLayout::DOC &&
                   options.doc_bucket_count == 0) {
            failure = Status::InvalidArgument("Variant shredder doc bucket count must be positive");
            state = State::FAILED;
        } else if (options.tablet_schema != nullptr && options.parent_column_unique_id < 0) {
            failure = Status::InvalidArgument(
                    "Variant shredder tablet schema requires a parent column unique id");
            state = State::FAILED;
        }
    }

    Status require_collecting() const {
        if (state == State::FAILED) {
            return failure;
        }
        if (state == State::FINISHED) {
            return Status::InvalidArgument("Variant shredder is already finished");
        }
        return Status::OK();
    }

    Status fail(Status status) {
        if (state != State::FAILED) {
            failure = std::move(status);
            state = State::FAILED;
        }
        return failure;
    }

    VariantPathBuilder* get_or_create_builder(PathIndex path_index) {
        PathState& path_state = paths[path_index];
        if (!path_state.builder.has_value()) {
            path_state.builder.emplace(path_state.path, rows);
        }
        return &*path_state.builder;
    }

    Status validate_doc_path(PathIndex path_index) const {
        if (options.physical_layout != VariantShredderPhysicalLayout::DOC) {
            return Status::OK();
        }
        const auto& parts = paths[path_index].path.get_parts();
        if (parts.empty()) {
            return Status::Corruption("Variant doc path must not be empty");
        }
        return Status::OK();
    }

    Status append_leaf(VariantRef value, PathIndex path_index, size_t row) {
        PathState& path_state = paths[path_index];
        const size_t row_marker = row + 1;
        if (path_state.last_row_marker == row_marker) {
            if (!options.check_duplicate_json_path) {
                return Status::InvalidArgument("may contains duplicated entry : {}",
                                               path_state.path.get_path());
            }
            return Status::OK();
        }
        path_state.last_row_marker = row_marker;
        if (value.is_null()) {
            return Status::OK();
        }
        return get_or_create_builder(path_index)->append(value, row);
    }

    // Pack two uint32 values into one key without allocating a pair object for every transition.
    static ParentFieldKey parent_field_key(PathIndex parent, uint32_t field) {
        return (static_cast<uint64_t>(parent) << 32) | field;
    }

    PathIndex resolve_child_path(MetadataPathCache& metadata_cache, PathIndex parent,
                                 uint32_t field) {
        ParentFieldKey cache_key = 0;
        if (parent == 0) {
            if (metadata_cache.root_child_paths.empty()) {
                const uint32_t dictionary_size = metadata_cache.metadata.dict_size();
                if (field >= dictionary_size) {
                    throw Exception(ErrorCode::CORRUPTION,
                                    "Variant object field id {} is outside metadata "
                                    "dictionary of size {}",
                                    field, dictionary_size);
                }
                metadata_cache.root_child_paths.assign(dictionary_size, UNRESOLVED_PATH);
            } else if (field >= metadata_cache.root_child_paths.size()) {
                throw Exception(ErrorCode::CORRUPTION,
                                "Variant object field id {} is outside metadata "
                                "dictionary of size {}",
                                field, metadata_cache.root_child_paths.size());
            }
            if (metadata_cache.root_child_paths[field] != UNRESOLVED_PATH) {
                return metadata_cache.root_child_paths[field];
            }
        } else {
            cache_key = parent_field_key(parent, field);
            if (const auto found = metadata_cache.nested_child_paths.find(cache_key);
                found != metadata_cache.nested_child_paths.end()) {
                return found->second;
            }
        }

        PathInDataBuilder builder;
        builder.append(paths[parent].path.get_parts(), false)
                .append(metadata_cache.metadata.key_at(field).to_string_view(), false);
        PathInData child = builder.build();
        // V2 object traversal keeps arrays as leaves. Canonicalizing into the dotted on-disk
        // namespace also makes {"a.b": 1} and {"a": {"b": 1}} share one path.
        child = PathInData(child.get_path());

        PathIndex child_index = 0;
        if (const auto found = path_indices.find(child); found != path_indices.end()) {
            child_index = found->second;
        } else {
            if (paths.size() >= UNRESOLVED_PATH) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Variant path count exceeds uint32 limit");
            }
            child_index = static_cast<PathIndex>(paths.size());
            path_indices.emplace(child, child_index);
            paths.emplace_back(child);
        }
        if (parent == 0) {
            metadata_cache.root_child_paths[field] = child_index;
        } else {
            metadata_cache.nested_child_paths.emplace(cache_key, child_index);
        }
        return child_index;
    }

    Status visit(VariantRef value, MetadataPathCache& metadata_cache, PathIndex path_index,
                 size_t row) {
        if (value.is_null()) {
            return options.check_duplicate_json_path ? append_leaf(value, path_index, row)
                                                     : Status::OK();
        }
        if (value.basic_type() != VariantBasicType::OBJECT) {
            return append_leaf(value, path_index, row);
        }
        const VariantRef::ObjectView object = value.object_view();
        for (uint32_t index = 0; index < object.size(); ++index) {
            uint32_t field = 0;
            VariantRef child = object.value_at(index, &field);
            const PathIndex child_path = resolve_child_path(metadata_cache, path_index, field);
            RETURN_IF_ERROR(validate_doc_path(child_path));
            RETURN_IF_ERROR(visit(child, metadata_cache, child_path, row));
        }
        return Status::OK();
    }

    Status complete_builder_rows(size_t completed_rows) {
        for (PathState& path : paths) {
            if (path.builder.has_value()) {
                RETURN_IF_ERROR(path.builder->complete_rows(completed_rows));
            }
        }
        return Status::OK();
    }

    void append_default_root() { root_values->insert_default(); }

    Status append_root(VariantRef value) {
        // V1 reconstructs objects exclusively from shredded paths, so JSON-null and unresolved
        // leaves remain absent. Keep the root only for scalar and array values. The physical
        // writer applies SQL NULL through the column's ordinary nullable map, so the shredder
        // does not need a separate root-null state.
        if (value.is_null() || value.basic_type() == VariantBasicType::OBJECT) {
            append_default_root();
            return Status::OK();
        }
        variant_to_jsonb(value, root_writer);
        root_values->insert_data(root_writer.getOutput()->getBuffer(),
                                 root_writer.getOutput()->getSize());
        return Status::OK();
    }

    Status prepare_logical_candidates(DorisVector<VariantPathSelectionCandidate>* candidates,
                                      DorisVector<VariantPathBuilder*>* candidate_builders,
                                      DorisVector<DataTypePtr>* storage_types) {
        candidates->reserve(paths.size());
        candidate_builders->reserve(paths.size());
        storage_types->reserve(paths.size());
        for (PathState& path : paths) {
            if (!path.builder.has_value()) {
                continue;
            }
            VariantPathBuilder* builder = &*path.builder;
            bool is_typed_path = false;
            DataTypePtr storage_type;
            if (options.tablet_schema != nullptr) {
                TabletSchema::SubColumnInfo info;
                is_typed_path = variant_util::generate_sub_column_info(
                        *options.tablet_schema, options.parent_column_unique_id,
                        builder->path().get_path(), &info);
                if (is_typed_path) {
                    storage_type = DataTypeFactory::instance().create_data_type(info.column);
                }
            }
            if (builder->non_null_rows() == 0) {
                if (!is_typed_path || options.typed_paths_to_sparse) {
                    continue;
                }
                RETURN_IF_ERROR(builder->convert_to(storage_type));
            } else {
                RETURN_IF_ERROR(builder->convert_to(
                        normalize_variant_path_integer_widths(builder->type())));
            }
            if (builder->non_null_rows() != 0 &&
                variant_path_type_contains_nothing(builder->type()) &&
                (storage_type == nullptr || variant_path_type_contains_nothing(storage_type))) {
                continue;
            }
            candidate_builders->push_back(builder);
            candidates->push_back(VariantPathSelectionCandidate {.builder = builder,
                                                                 .is_typed_path = is_typed_path});
            storage_types->push_back(std::move(storage_type));
        }
        return Status::OK();
    }

    Status convert_typed_candidates(std::span<const size_t> selected,
                                    const DorisVector<VariantPathBuilder*>& candidate_builders,
                                    const DorisVector<DataTypePtr>& storage_types) const {
        for (size_t index : selected) {
            if (storage_types[index] != nullptr) {
                RETURN_IF_ERROR(candidate_builders[index]->convert_to(storage_types[index]));
            }
        }
        return Status::OK();
    }

    Status publish_materialized(const VariantPathSelection& selection,
                                const DorisVector<VariantPathBuilder*>& candidate_builders,
                                VariantShreddedColumns* result) const {
        result->materialized.reserve(selection.materialized.size());
        for (size_t selected : selection.materialized) {
            VariantPathBuilder& builder = *candidate_builders[selected];
            const PathInData& raw_path = builder.path();
            PathInData publication_path =
                    options.physical_layout == VariantShredderPhysicalLayout::DOC
                            ? normalize_doc_publication_path(raw_path)
                            : raw_path;
            const std::span<const uint32_t> compact_rowids = builder.rowids();
            DorisVector<uint32_t> rowids(compact_rowids.begin(), compact_rowids.end());
            result->materialized.push_back({.path = publication_path,
                                            .type = builder.type(),
                                            .column = builder.column(),
                                            .rowids = std::move(rowids)});
        }
        return Status::OK();
    }

    void publish_root(VariantShreddedColumns* result) {
        result->num_rows = rows;
        result->root_jsonb = std::move(root_values);
    }

    DorisVector<SparsePlan> build_sparse_plan(
            const VariantPathSelection& selection,
            const DorisVector<VariantPathBuilder*>& candidate_builders) const {
        DorisVector<SparsePlan> sparse_plan;
        sparse_plan.reserve(selection.sparse.size());
        for (size_t selected : selection.sparse) {
            VariantPathBuilder* builder = candidate_builders[selected];
            const std::string& path = builder->path().get_path();
            sparse_plan.push_back({.builder = builder,
                                   .bucket = variant_util::variant_binary_shard_of(
                                           {path.data(), path.size()}, options.sparse_bucket_count),
                                   .path = &path,
                                   .track_statistics = false});
        }
        return sparse_plan;
    }

    void select_sparse_statistics(DorisVector<SparsePlan>* sparse_plan) const {
        if (options.max_sparse_column_statistics_size == 0) {
            return;
        }
        DorisVector<size_t> encounter_order(sparse_plan->size());
        std::iota(encounter_order.begin(), encounter_order.end(), 0);
        std::ranges::sort(encounter_order, [&](size_t left, size_t right) {
            const auto left_rowids = (*sparse_plan)[left].builder->rowids();
            const auto right_rowids = (*sparse_plan)[right].builder->rowids();
            DORIS_CHECK(!left_rowids.empty());
            DORIS_CHECK(!right_rowids.empty());
            if (left_rowids.front() != right_rowids.front()) {
                return left_rowids.front() < right_rowids.front();
            }
            // The plan is path-sorted, matching the old inner loop's tie-break at a row.
            return left < right;
        });

        DorisVector<size_t> tracked_paths_per_bucket(options.sparse_bucket_count, 0);
        for (size_t index : encounter_order) {
            SparsePlan& plan = (*sparse_plan)[index];
            if (tracked_paths_per_bucket[plan.bucket] < options.max_sparse_column_statistics_size) {
                plan.track_statistics = true;
                ++tracked_paths_per_bucket[plan.bucket];
            }
        }
    }

    template <typename BinaryPlan>
    Status append_binary_rows(const DorisVector<BinaryPlan>& binary_plan,
                              const DorisVector<ColumnMap*>& maps) const {
        DorisVector<size_t> bucket_cells(maps.size(), 0);
        DorisVector<size_t> bucket_key_bytes(maps.size(), 0);
        DorisVector<size_t> bucket_value_bytes(maps.size(), 0);
        // Build a compact row index in two passes. Each path contributes only its
        // present values, and paths are visited in publication order so cells
        // within one row preserve path order.
        DorisVector<size_t> row_offsets(rows + 1, 0);
        for (const BinaryPlan& plan : binary_plan) {
            DORIS_CHECK_LT(plan.bucket, maps.size());
            const std::span<const uint32_t> rowids = plan.builder->rowids();
            bucket_cells[plan.bucket] += rowids.size();
            bucket_key_bytes[plan.bucket] += rowids.size() * plan.path->size();
            const ColumnPtr column = plan.builder->column();
            DORIS_CHECK(column);
            bucket_value_bytes[plan.bucket] += column->byte_size();
            for (uint32_t row : rowids) {
                if (row >= rows) {
                    return Status::InternalError("Variant path {} row {} exceeds {} rows",
                                                 *plan.path, row, rows);
                }
                ++row_offsets[row + 1];
            }
        }
        for (size_t bucket = 0; bucket < maps.size(); ++bucket) {
            auto& keys = assert_cast<ColumnString&>(maps[bucket]->get_keys());
            auto& values = assert_cast<ColumnString&>(maps[bucket]->get_values());
            maps[bucket]->get_offsets().reserve(rows);
            keys.reserve(bucket_cells[bucket]);
            keys.get_chars().reserve(bucket_key_bytes[bucket]);
            values.reserve(bucket_cells[bucket]);
            values.get_chars().reserve(bucket_value_bytes[bucket]);
        }
        if (binary_plan.size() > std::numeric_limits<uint32_t>::max()) {
            return Status::InternalError("Variant binary path count {} exceeds uint32 limit",
                                         binary_plan.size());
        }
        std::partial_sum(row_offsets.begin(), row_offsets.end(), row_offsets.begin());

        // Transpose path-major builders into row-major maps in bounded chunks. A
        // chunk always ends at a row boundary, so the path-sorted plan order
        // remains the canonical key order within each row and bucket. The per-plan
        // cursor also recovers value_index without storing it in every cell.
#if defined(BE_TEST) && !defined(BE_BENCHMARK)
        const size_t max_binary_cells_per_chunk = binary_cells_per_chunk;
#else
        constexpr size_t max_binary_cells_per_chunk = MAX_BINARY_CELLS_PER_CHUNK;
#endif
        DorisVector<size_t> value_indices(binary_plan.size(), 0);
        DorisVector<uint32_t> cells;
        DorisVector<size_t> next_cell;
        size_t row_begin = 0;
        while (row_begin < rows) {
#if defined(BE_TEST) && !defined(BE_BENCHMARK)
            ++binary_chunk_count;
#endif
            const size_t first_cell = row_offsets[row_begin];
            size_t row_end = rows;
            if (row_offsets.back() - first_cell > max_binary_cells_per_chunk) {
                const auto first_too_large =
                        std::upper_bound(row_offsets.begin() + row_begin + 1, row_offsets.end(),
                                         first_cell + max_binary_cells_per_chunk);
                row_end = static_cast<size_t>(first_too_large - row_offsets.begin() - 1);
                // One exceptionally wide row may exceed the bound, but must stay
                // intact.
                row_end = std::max(row_end, row_begin + 1);
            }

            cells.resize(row_offsets[row_end] - first_cell);
            next_cell.resize(row_end - row_begin);
            for (size_t row = row_begin; row < row_end; ++row) {
                next_cell[row - row_begin] = row_offsets[row] - first_cell;
            }
            for (size_t plan_index = 0; plan_index < binary_plan.size(); ++plan_index) {
                const std::span<const uint32_t> rowids = binary_plan[plan_index].builder->rowids();
                size_t value_index = value_indices[plan_index];
                DORIS_CHECK(value_index == rowids.size() || rowids[value_index] >= row_begin);
                while (value_index < rowids.size() && rowids[value_index] < row_end) {
                    const uint32_t row = rowids[value_index++];
                    cells[next_cell[row - row_begin]++] = static_cast<uint32_t>(plan_index);
                }
            }

            for (size_t row = row_begin; row < row_end; ++row) {
                DORIS_CHECK_EQ(next_cell[row - row_begin], row_offsets[row + 1] - first_cell);
                for (size_t cell_index = row_offsets[row] - first_cell;
                     cell_index < row_offsets[row + 1] - first_cell; ++cell_index) {
                    const uint32_t plan_index = cells[cell_index];
                    const BinaryPlan& plan = binary_plan[plan_index];
                    const size_t value_index = value_indices[plan_index]++;
                    auto& keys = assert_cast<ColumnString&>(maps[plan.bucket]->get_keys());
                    auto& values = assert_cast<ColumnString&>(maps[plan.bucket]->get_values());
                    keys.insert_data(plan.path->data(), plan.path->size());
                    RETURN_IF_ERROR(
                            plan.builder->write_sparse_cell(value_index, &values.get_chars()));
                    values.get_offsets().push_back(values.get_chars().size());
                }
                for (ColumnMap* map : maps) {
                    map->get_offsets().push_back(map->get_keys().size());
                }
            }
            row_begin = row_end;
        }
        for (size_t plan_index = 0; plan_index < binary_plan.size(); ++plan_index) {
            DORIS_CHECK_EQ(value_indices[plan_index],
                           binary_plan[plan_index].builder->rowids().size());
        }
        return Status::OK();
    }

    void publish_sparse_statistics(const DorisVector<SparsePlan>& sparse_plan,
                                   DorisVector<VariantStatistics>* bucket_statistics,
                                   VariantShreddedColumns* result) const {
        for (const SparsePlan& plan : sparse_plan) {
            if (!plan.track_statistics) {
                continue;
            }
            const uint32_t count = plan.builder->non_null_rows();
            (*bucket_statistics)[plan.bucket].sparse_column_non_null_size[*plan.path] = count;
            result->statistics.sparse_column_non_null_size[*plan.path] += count;
        }
    }

    Status publish_sparse(const VariantPathSelection& selection,
                          const DorisVector<VariantPathBuilder*>& candidate_builders,
                          VariantShreddedColumns* result) const {
        DorisVector<MutableColumnPtr> sparse_owners;
        DorisVector<ColumnMap*> sparse_maps;
        sparse_owners.reserve(options.sparse_bucket_count);
        sparse_maps.reserve(options.sparse_bucket_count);
        result->binary_buckets.reserve(options.sparse_bucket_count);
        for (uint32_t bucket = 0; bucket < options.sparse_bucket_count; ++bucket) {
            MutableColumnPtr map = ColumnVariant::create_binary_column_fn();
            sparse_maps.push_back(assert_cast<ColumnMap*>(map.get()));
            sparse_owners.emplace_back(std::move(map));
        }

        DorisVector<SparsePlan> sparse_plan = build_sparse_plan(selection, candidate_builders);
        select_sparse_statistics(&sparse_plan);
        RETURN_IF_ERROR(append_binary_rows(sparse_plan, sparse_maps));
        DorisVector<VariantStatistics> bucket_statistics(options.sparse_bucket_count);
        publish_sparse_statistics(sparse_plan, &bucket_statistics, result);
        for (uint32_t bucket = 0; bucket < options.sparse_bucket_count; ++bucket) {
            result->binary_buckets.push_back({.column = std::move(sparse_owners[bucket]),
                                              .statistics = std::move(bucket_statistics[bucket])});
        }
        return Status::OK();
    }

    Status build_doc_plan(const DorisVector<VariantPathBuilder*>& candidate_builders,
                          DorisVector<DocPlan>& plan) const {
        plan.reserve(candidate_builders.size());
        for (size_t index = 0; index < candidate_builders.size(); ++index) {
            VariantPathBuilder* builder = candidate_builders[index];
            const PathInData publication_path = normalize_doc_publication_path(builder->path());
            if (publication_path.get_parts().empty()) {
                return Status::Corruption("Variant doc path must not be empty");
            }
            for (const PathInData::Part& part : publication_path.get_parts()) {
                if (part.key.find('.') != std::string_view::npos) {
                    return Status::Corruption("Variant doc path has an ambiguous dotted part");
                }
            }
            const std::string& path = builder->path().get_path();
            plan.push_back({.builder = builder,
                            .bucket = variant_util::variant_binary_shard_of(
                                    {path.data(), path.size()}, options.doc_bucket_count),
                            .path = &path,
                            .candidate_index = index});
        }
        std::ranges::sort(plan, [](const DocPlan& left, const DocPlan& right) {
            return *left.path < *right.path;
        });
        for (size_t index = 1; index < plan.size(); ++index) {
            if (*plan[index - 1].path == *plan[index].path) {
                return Status::Corruption("Variant structured doc paths collide at {}",
                                          *plan[index].path);
            }
        }
        return Status::OK();
    }

    Status publish_doc(const DorisVector<DocPlan>& plan, VariantShreddedColumns* result) const {
        DorisVector<MutableColumnPtr> owners;
        DorisVector<ColumnMap*> maps;
        owners.reserve(options.doc_bucket_count);
        maps.reserve(options.doc_bucket_count);
        for (uint32_t bucket = 0; bucket < options.doc_bucket_count; ++bucket) {
            MutableColumnPtr map = ColumnVariant::create_binary_column_fn();
            maps.push_back(assert_cast<ColumnMap*>(map.get()));
            owners.emplace_back(std::move(map));
        }
        RETURN_IF_ERROR(append_binary_rows(plan, maps));

        DorisVector<VariantStatistics> statistics(options.doc_bucket_count);
        for (const DocPlan& entry : plan) {
            const uint32_t count = entry.builder->non_null_rows();
            statistics[entry.bucket].doc_value_column_non_null_size[*entry.path] = count;
            result->statistics.doc_value_column_non_null_size[*entry.path] += count;
        }
        result->binary_buckets.reserve(options.doc_bucket_count);
        for (uint32_t bucket = 0; bucket < options.doc_bucket_count; ++bucket) {
            result->binary_buckets.push_back({.column = std::move(owners[bucket]),
                                              .statistics = std::move(statistics[bucket])});
        }
        return Status::OK();
    }

    Status finish_ordinary(const DorisVector<VariantPathSelectionCandidate>& candidates,
                           const DorisVector<VariantPathBuilder*>& candidate_builders,
                           const DorisVector<DataTypePtr>& storage_types,
                           VariantShreddedColumns* result) {
        DorisVector<size_t> all(candidate_builders.size());
        std::iota(all.begin(), all.end(), 0);
        RETURN_IF_ERROR(convert_typed_candidates(all, candidate_builders, storage_types));
        const VariantPathSelection selection = select_variant_paths(
                candidates, options.max_subcolumns_count, options.typed_paths_to_sparse);
        publish_root(result);
        RETURN_IF_ERROR(publish_materialized(selection, candidate_builders, result));
        RETURN_IF_ERROR(publish_sparse(selection, candidate_builders, result));
        return Status::OK();
    }

    Status finish_doc(const DorisVector<VariantPathBuilder*>& candidate_builders,
                      const DorisVector<DataTypePtr>& storage_types,
                      VariantShreddedColumns* result) {
        DorisVector<DocPlan> doc_plan;
        RETURN_IF_ERROR(build_doc_plan(candidate_builders, doc_plan));
        RETURN_IF_ERROR(publish_doc(doc_plan, result));

        VariantPathSelection selection;
        if (rows >= options.doc_materialization_min_rows) {
            selection.materialized.reserve(doc_plan.size());
            for (const DocPlan& entry : doc_plan) {
                selection.materialized.push_back(entry.candidate_index);
            }
            RETURN_IF_ERROR(convert_typed_candidates(selection.materialized, candidate_builders,
                                                     storage_types));
        }
        publish_root(result);
        return publish_materialized(selection, candidate_builders, result);
    }

    Status finish_impl(VariantShreddedColumns* result) {
        RETURN_IF_ERROR(complete_builder_rows(rows));
        DorisVector<VariantPathSelectionCandidate> candidates;
        DorisVector<VariantPathBuilder*> candidate_builders;
        DorisVector<DataTypePtr> storage_types;
        RETURN_IF_ERROR(
                prepare_logical_candidates(&candidates, &candidate_builders, &storage_types));
        if (options.physical_layout == VariantShredderPhysicalLayout::DOC) {
            RETURN_IF_ERROR(finish_doc(candidate_builders, storage_types, result));
        } else {
            RETURN_IF_ERROR(finish_ordinary(candidates, candidate_builders, storage_types, result));
        }
        return Status::OK();
    }

    VariantShredderOptions options;
    State state = State::COLLECTING;
    Status failure;
    size_t rows = 0;
    std::unordered_map<PathInData, PathIndex, PathInData::Hash> path_indices = {{PathInData(), 0}};
    DorisVector<PathState> paths;
    ColumnString::MutablePtr root_values = ColumnString::create();
    JsonbWriter root_writer;
#if defined(BE_TEST) && !defined(BE_BENCHMARK)
    size_t binary_cells_per_chunk = MAX_BINARY_CELLS_PER_CHUNK;
    mutable size_t binary_chunk_count = 0;
#endif
};

VariantShredder::VariantShredder(VariantShredderOptions options)
        : _impl(std::make_unique<Impl>(std::move(options))) {}

VariantShredder::~VariantShredder() = default;
VariantShredder::VariantShredder(VariantShredder&&) noexcept = default;
VariantShredder& VariantShredder::operator=(VariantShredder&&) noexcept = default;

Status VariantShredder::append(const ColumnVariantV2::ReadView& view, size_t begin, size_t length,
                               std::span<const uint8_t> outer_nulls) {
    RETURN_IF_ERROR(_impl->require_collecting());
    if (view.is_typed()) {
        return _impl->fail(Status::InvalidArgument(
                "Variant shredder requires encoded E-state input; caller must ensure_encoded"));
    }
    if (begin > view.size() || length > view.size() - begin) {
        return _impl->fail(
                Status::InvalidArgument("Variant shredder range [{}, {}) exceeds input size {}",
                                        begin, begin + length, view.size()));
    }
    if (!outer_nulls.empty() && outer_nulls.size() != length) {
        return _impl->fail(
                Status::InvalidArgument("Variant shredder outer-null span has {} rows, expected {}",
                                        outer_nulls.size(), length));
    }
    if (length > std::numeric_limits<size_t>::max() - _impl->rows) {
        return _impl->fail(Status::InvalidArgument("Variant shredder row count overflows size_t"));
    }
    try {
        // Dictionaries are usually few per batch. Borrow each dictionary for this append and
        // cache only the parent+field transitions that are actually traversed; no metadata bytes
        // or metadata-specific ids survive the call.
        DorisVector<Impl::MetadataPathCache> metadata_caches;
        metadata_caches.reserve(view.metadata_count());
        for (size_t metadata_index = 0; metadata_index < view.metadata_count(); ++metadata_index) {
            metadata_caches.emplace_back(view.metadata_at(static_cast<uint32_t>(metadata_index)));
        }

        for (size_t offset = 0; offset < length; ++offset) {
            const bool outer_null = !outer_nulls.empty() && outer_nulls[offset] != 0;
            if (outer_null) {
                _impl->append_default_root();
                ++_impl->rows;
                continue;
            }
            const size_t input_row = begin + offset;
            const uint32_t metadata_index = view.metadata_id_at(input_row);
            if (metadata_index >= metadata_caches.size()) {
                return _impl->fail(
                        Status::Corruption("Variant row {} metadata index {} exceeds {} entries",
                                           input_row, metadata_index, metadata_caches.size()));
            }
            VariantRef value = view.value_at(input_row);
            Status status = _impl->append_root(value);
            if (!status.ok()) {
                return _impl->fail(std::move(status));
            }
            if (value.basic_type() == VariantBasicType::OBJECT) {
                status = _impl->visit(value, metadata_caches[metadata_index], 0, _impl->rows);
                if (!status.ok()) {
                    return _impl->fail(std::move(status));
                }
            }
            ++_impl->rows;
        }
        return Status::OK();
    } catch (const Exception& exception) {
        return _impl->fail(exception.to_status());
    }
}

Status VariantShredder::finish(VariantShreddedColumns* output) {
    RETURN_IF_ERROR(_impl->require_collecting());
    if (output == nullptr) {
        return _impl->fail(Status::InvalidArgument("Variant shredder output must not be null"));
    }

    try {
        VariantShreddedColumns result;
        Status status = _impl->finish_impl(&result);
        if (!status.ok()) {
            return _impl->fail(std::move(status));
        }
        *output = std::move(result);
        _impl->state = Impl::State::FINISHED;
        return Status::OK();
    } catch (const Exception& exception) {
        return _impl->fail(exception.to_status());
    }
}

size_t VariantShredder::byte_size() const {
    size_t size = sizeof(Impl);
    size += _impl->path_indices.bucket_count() * sizeof(void*);
    for (const auto& [path, index] : _impl->path_indices) {
        static_cast<void>(index);
        size += sizeof(std::pair<const PathInData, Impl::PathIndex>) + path_allocated_bytes(path);
    }
    size += _impl->paths.capacity() * sizeof(Impl::PathState);
    for (const Impl::PathState& path : _impl->paths) {
        size += path_allocated_bytes(path.path);
        if (path.builder.has_value()) {
            size += path.builder->byte_size();
        }
    }
    if (_impl->root_values) {
        size += _impl->root_values->allocated_bytes();
    }
    size += sizeof(JsonbOutStream) + _impl->root_writer.getOutput()->allocated_bytes();
    return size;
}

#if defined(BE_TEST) && !defined(BE_BENCHMARK)
size_t VariantShredder::TestAccess::binary_chunk_count(const VariantShredder& shredder) {
    return shredder._impl->binary_chunk_count;
}

void VariantShredder::TestAccess::set_binary_cells_per_chunk(VariantShredder& shredder,
                                                             size_t cells) {
    DORIS_CHECK(cells > 0);
    shredder._impl->binary_cells_per_chunk = cells;
}
#endif
} // namespace doris::segment_v2
