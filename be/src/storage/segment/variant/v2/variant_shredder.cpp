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
#include <string_view>
#include <unordered_map>
#include <utility>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
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

    // Metadata bytes belong to the input ReadView. This cache never escapes one append call, so
    // it can borrow the dictionary and retain only parent+field transitions observed in that
    // batch. Canonical paths themselves remain owned by PathState across appends.
    struct MetadataPathCache {
        explicit MetadataPathCache(VariantMetadataRef metadata_) : metadata(metadata_) {}

        VariantMetadataRef metadata;
        ChildPathCache child_paths;
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

    Status prepare_leaf(PathIndex path_index, size_t row, bool* should_append) {
        DORIS_CHECK(should_append != nullptr);
        PathState& path_state = paths[path_index];
        const size_t row_marker = row + 1;
        if (path_state.last_row_marker == row_marker) {
            if (!options.check_duplicate_json_path) {
                return Status::InvalidArgument("may contains duplicated entry : {}",
                                               path_state.path.get_path());
            }
            *should_append = false;
            return Status::OK();
        }
        path_state.last_row_marker = row_marker;
        *should_append = true;
        return Status::OK();
    }

    Status append_leaf(VariantRef value, PathIndex path_index, size_t row) {
        bool should_append = false;
        RETURN_IF_ERROR(prepare_leaf(path_index, row, &should_append));
        if (!should_append || value.is_null()) {
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
        const ParentFieldKey cache_key = parent_field_key(parent, field);
        if (const auto found = metadata_cache.child_paths.find(cache_key);
            found != metadata_cache.child_paths.end()) {
            return found->second;
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
            if (paths.size() > std::numeric_limits<PathIndex>::max()) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Variant path count exceeds uint32 limit");
            }
            child_index = static_cast<PathIndex>(paths.size());
            path_indices.emplace(child, child_index);
            paths.emplace_back(child);
        }
        metadata_cache.child_paths.emplace(cache_key, child_index);
        return child_index;
    }

    PathIndex resolve_shredded_path(const PathInData& path) {
        // Match encoded object traversal exactly: storage path identity is the canonical dotted
        // namespace, not the execution layout's part identity or typed marker.
        PathInData canonical(path.get_path());
        if (const auto found = path_indices.find(canonical); found != path_indices.end()) {
            return found->second;
        }
        if (paths.size() > std::numeric_limits<PathIndex>::max()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant path count exceeds uint32 limit");
        }
        const PathIndex index = static_cast<PathIndex>(paths.size());
        path_indices.emplace(canonical, index);
        paths.emplace_back(canonical);
        return index;
    }

    bool residual_contains_canonical_leaf(VariantRef value, std::string_view canonical_path) const {
        if (value.basic_type() != VariantBasicType::OBJECT) {
            return false;
        }
        const uint32_t children = value.num_elements();
        for (uint32_t index = 0; index < children; ++index) {
            uint32_t field = 0;
            const VariantRef child = value.object_value_at(index, &field);
            const std::string_view key = value.metadata.key_at(field).to_string_view();
            if (!canonical_path.starts_with(key)) {
                continue;
            }
            if (key.size() == canonical_path.size()) {
                if (child.is_null()) {
                    if (options.check_duplicate_json_path) {
                        return true;
                    }
                } else if (child.basic_type() != VariantBasicType::OBJECT) {
                    // Arrays are leaves in the storage traversal, just like scalars.
                    return true;
                }
                continue;
            }
            if (canonical_path[key.size()] != '.' ||
                child.basic_type() != VariantBasicType::OBJECT) {
                continue;
            }
            if (residual_contains_canonical_leaf(child, canonical_path.substr(key.size() + 1))) {
                return true;
            }
        }
        return false;
    }

    Status append_shredded_field(const ColumnVariantV2::ReadView& field, size_t input_row,
                                 PathIndex path_index, size_t output_row,
                                 DorisVector<char>* encoded_slow_scratch) {
        if (field.is_encoded()) {
            const VariantRef value = field.value_at(input_row);
            return value.is_null() && !options.check_duplicate_json_path
                           ? Status::OK()
                           : append_leaf(value, path_index, output_row);
        }
        DORIS_CHECK(field.is_typed());
        DORIS_CHECK(encoded_slow_scratch != nullptr);
        const auto& nullable = assert_cast<const ColumnNullable&>(field.typed_column());
        const uint32_t scale = field.typed_type()->get_scale();
        DORIS_CHECK_LE(scale, static_cast<uint32_t>(std::numeric_limits<uint8_t>::max()));

        if (nullable.is_null_at(input_row)) {
            if (options.check_duplicate_json_path) {
                bool should_append = false;
                RETURN_IF_ERROR(prepare_leaf(path_index, output_row, &should_append));
            }
            return Status::OK();
        }

        bool should_append = false;
        RETURN_IF_ERROR(prepare_leaf(path_index, output_row, &should_append));
        if (!should_append) {
            return Status::OK();
        }
        Status status;
        dispatch_variant_typed_column(
                nullable.get_nested_column(), field.typed_type()->get_primitive_type(),
                [&]<PrimitiveType Type>(const auto& nested) {
                    with_variant_typed_scalar<Type>(
                            nested, input_row, static_cast<uint8_t>(scale),
                            [&](const VariantScalarRef& scalar) {
                                bool used_encoded_scratch = false;
                                status = get_or_create_builder(path_index)
                                                 ->append_scalar(scalar, output_row,
                                                                 *encoded_slow_scratch,
                                                                 &used_encoded_scratch);
#ifdef BE_TEST
                                if (status.ok()) {
                                    if (used_encoded_scratch) {
                                        ++typed_encoded_slow_appends;
                                    } else {
                                        ++typed_direct_scalar_appends;
                                    }
                                }
#endif
                            });
                });
        return status;
    }

    bool shredded_field_participates(const ColumnVariantV2::ReadView& field,
                                     size_t input_row) const {
        if (options.check_duplicate_json_path) {
            return true;
        }
        if (field.is_encoded()) {
            return !field.value_at(input_row).is_null();
        }
        DORIS_CHECK(field.is_typed());
        return !assert_cast<const ColumnNullable&>(field.typed_column()).is_null_at(input_row);
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
        const uint32_t children = value.num_elements();
        for (uint32_t index = 0; index < children; ++index) {
            uint32_t field = 0;
            VariantRef child = value.object_value_at(index, &field);
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
        struct BinaryCell {
            size_t plan_index = 0;
            size_t value_index = 0;
        };

        // Build a compact row index in two passes. Each path contributes only its present values,
        // and paths are visited in publication order so cells within one row preserve path order.
        DorisVector<size_t> row_offsets(rows + 1, 0);
        for (const BinaryPlan& plan : binary_plan) {
            for (uint32_t row : plan.builder->rowids()) {
                if (row >= rows) {
                    return Status::InternalError("Variant path {} row {} exceeds {} rows",
                                                 *plan.path, row, rows);
                }
                ++row_offsets[row + 1];
            }
        }
        std::partial_sum(row_offsets.begin(), row_offsets.end(), row_offsets.begin());
        DorisVector<BinaryCell> cells(row_offsets.back());
        DorisVector<size_t> next_cell = row_offsets;
        for (size_t plan_index = 0; plan_index < binary_plan.size(); ++plan_index) {
            const auto rowids = binary_plan[plan_index].builder->rowids();
            for (size_t value_index = 0; value_index < rowids.size(); ++value_index) {
                cells[next_cell[rowids[value_index]]++] = {.plan_index = plan_index,
                                                           .value_index = value_index};
            }
        }

        for (size_t row = 0; row < rows; ++row) {
            for (size_t cell_index = row_offsets[row]; cell_index < row_offsets[row + 1];
                 ++cell_index) {
                const BinaryCell& cell = cells[cell_index];
                const BinaryPlan& plan = binary_plan[cell.plan_index];
                auto& keys = assert_cast<ColumnString&>(maps[plan.bucket]->get_keys());
                auto& values = assert_cast<ColumnString&>(maps[plan.bucket]->get_values());
                keys.insert_data(plan.path->data(), plan.path->size());
                RETURN_IF_ERROR(
                        plan.builder->write_sparse_cell(cell.value_index, &values.get_chars()));
                values.get_offsets().push_back(values.get_chars().size());
            }
            for (ColumnMap* map : maps) {
                map->get_offsets().push_back(map->get_keys().size());
            }
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
#ifdef BE_TEST
    size_t typed_direct_scalar_appends = 0;
    size_t typed_encoded_slow_appends = 0;
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
    if (!view.is_encoded()) {
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

Status VariantShredder::append_shredded(const ColumnVariantV2& source, size_t begin, size_t length,
                                        std::span<const uint8_t> outer_nulls,
                                        VariantShredderAppendStats* append_stats) {
    RETURN_IF_ERROR(_impl->require_collecting());
    const ColumnVariantV2::ReadView view = source.read_view();
    if (!view.is_shredded()) {
        return _impl->fail(
                Status::InvalidArgument("Variant shredder shredded append requires S-state input"));
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

    VariantShredderAppendStats result_stats;
    try {
        DorisVector<Impl::MetadataPathCache> residual_metadata_caches;
        residual_metadata_caches.reserve(view.residual_metadata_count());
        for (size_t metadata_index = 0; metadata_index < view.residual_metadata_count();
             ++metadata_index) {
            residual_metadata_caches.emplace_back(
                    view.residual_metadata_at(static_cast<uint32_t>(metadata_index)));
        }

        const size_t field_count = view.shredded_field_count();
        DorisVector<ColumnVariantV2::ReadView> field_views;
        field_views.reserve(field_count);
        DorisVector<std::optional<Impl::PathIndex>> field_path_indices(field_count);
        DorisVector<size_t> field_canonical_groups(field_count, 0);
        std::unordered_map<std::string_view, size_t> canonical_field_groups;
        canonical_field_groups.reserve(field_count);
        for (size_t field_index = 0; field_index < field_count; ++field_index) {
            field_views.emplace_back(view.shredded_field_values(field_index).read_view());
            const PathInData& field_path = view.shredded_field_path(field_index);
            const auto [group, inserted] = canonical_field_groups.emplace(
                    field_path.get_path(), canonical_field_groups.size());
            static_cast<void>(inserted);
            field_canonical_groups[field_index] = group->second;
        }
        DorisVector<std::string_view> canonical_group_paths(canonical_field_groups.size());
        for (const auto& [path, group] : canonical_field_groups) {
            canonical_group_paths[group] = path;
        }
        DorisVector<size_t> active_group_markers(canonical_field_groups.size(), 0);

        DorisVector<size_t> active_fields;
        active_fields.reserve(field_count);
        DorisVector<size_t> active_groups;
        active_groups.reserve(canonical_field_groups.size());
        DorisVector<char> encoded_slow_scratch;
        for (size_t offset = 0; offset < length; ++offset) {
            if (!outer_nulls.empty() && outer_nulls[offset] != 0) {
                _impl->append_default_root();
                ++_impl->rows;
                ++result_stats.native_shredded_rows;
                continue;
            }

            const size_t input_row = begin + offset;
            active_fields.clear();
            active_groups.clear();
            bool duplicate_active_path = false;
            const size_t row_marker = offset + 1;
            for (size_t field_index = 0; field_index < field_count; ++field_index) {
                if (view.shredded_field_presence(field_index).get_data()[input_row] != 0) {
                    active_fields.push_back(field_index);
                    if (!_impl->shredded_field_participates(field_views[field_index], input_row)) {
                        continue;
                    }
                    const size_t group = field_canonical_groups[field_index];
                    duplicate_active_path |= active_group_markers[group] == row_marker;
                    if (active_group_markers[group] != row_marker) {
                        active_group_markers[group] = row_marker;
                        active_groups.push_back(group);
                    }
                }
            }

            const uint32_t metadata_index = view.residual_metadata_id_at(input_row);
            if (metadata_index >= residual_metadata_caches.size()) {
                return _impl->fail(Status::Corruption(
                        "Variant residual row {} metadata index {} exceeds {} entries", input_row,
                        metadata_index, residual_metadata_caches.size()));
            }
            const VariantRef residual = view.residual_value_at(input_row);

            // A literal dotted key and a nested path intentionally collide in the legacy storage
            // namespace. Probe only residual branches that can spell a currently active canonical
            // path; unrelated dotted keys do not trigger a full residual pre-scan.
            Impl::MetadataPathCache& residual_metadata_cache =
                    residual_metadata_caches[metadata_index];
            bool residual_collision = false;
            if (!duplicate_active_path) {
                for (size_t group : active_groups) {
                    const std::string_view canonical_path = canonical_group_paths[group];
                    if (canonical_path.find('.') != std::string_view::npos &&
                        _impl->residual_contains_canonical_leaf(residual, canonical_path)) {
                        residual_collision = true;
                        break;
                    }
                }
            }
            if (duplicate_active_path || residual_collision) {
                ColumnVariantV2::MutablePtr encoded_row =
                        source.materialize_encoded_range(input_row, 1);
                DORIS_CHECK(encoded_row->is_encoded());
                const Status status = append(encoded_row->read_view(), 0, 1);
                if (!status.ok()) {
                    return status;
                }
                ++result_stats.encoded_fallback_rows;
                continue;
            }

            Status status;
            if (active_fields.empty()) {
                status = _impl->append_root(residual);
            } else {
                _impl->append_default_root();
            }
            if (!status.ok()) {
                return _impl->fail(std::move(status));
            }
            if (residual.basic_type() == VariantBasicType::OBJECT) {
                status = _impl->visit(residual, residual_metadata_cache, 0, _impl->rows);
                if (!status.ok()) {
                    return _impl->fail(std::move(status));
                }
            }
            for (size_t field_index : active_fields) {
                if (!field_path_indices[field_index].has_value()) {
                    const Impl::PathIndex path_index =
                            _impl->resolve_shredded_path(view.shredded_field_path(field_index));
                    status = _impl->validate_doc_path(path_index);
                    if (!status.ok()) {
                        return _impl->fail(std::move(status));
                    }
                    field_path_indices[field_index] = path_index;
                }
                status = _impl->append_shredded_field(field_views[field_index], input_row,
                                                      *field_path_indices[field_index], _impl->rows,
                                                      &encoded_slow_scratch);
                if (!status.ok()) {
                    return _impl->fail(std::move(status));
                }
            }
            ++_impl->rows;
            ++result_stats.native_shredded_rows;
        }
        if (append_stats != nullptr) {
            *append_stats = result_stats;
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

#ifdef BE_TEST
size_t VariantShredder::TestAccess::typed_direct_scalar_appends(const VariantShredder& shredder) {
    return shredder._impl->typed_direct_scalar_appends;
}

size_t VariantShredder::TestAccess::typed_encoded_slow_appends(const VariantShredder& shredder) {
    return shredder._impl->typed_encoded_slow_appends;
}
#endif

} // namespace doris::segment_v2
