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

#include "olap/rowset/segment_v2/variant/variant_doc_snapshot_iterator.h"

#include <algorithm>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/json/path_in_data.h"

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

VariantDocSnapshotIteratorBase::VariantDocSnapshotIteratorBase(
        std::vector<BinaryColumnCacheSPtr>&& doc_snapshot_column_caches)
        : _doc_snapshot_column_caches(std::move(doc_snapshot_column_caches)) {}

Status VariantDocSnapshotIteratorBase::init(const ColumnIteratorOptions& opts) {
    for (const auto& cache : _doc_snapshot_column_caches) {
        RETURN_IF_ERROR(cache->init(opts));
    }
    return Status::OK();
}

Status VariantDocSnapshotIteratorBase::seek_to_ordinal(ordinal_t ord) {
    for (const auto& cache : _doc_snapshot_column_caches) {
        RETURN_IF_ERROR(cache->seek_to_ordinal(ord));
    }
    return Status::OK();
}

ordinal_t VariantDocSnapshotIteratorBase::get_current_ordinal() const {
    DCHECK(!_doc_snapshot_column_caches.empty());
    return _doc_snapshot_column_caches[0]->get_current_ordinal();
}

VariantDocSnapshotRootIterator::VariantDocSnapshotRootIterator(
        std::vector<BinaryColumnCacheSPtr>&& doc_snapshot_column_caches,
        std::unique_ptr<SubstreamIterator>&& root_reader)
        : VariantDocSnapshotIteratorBase(std::move(doc_snapshot_column_caches)),
          _root_reader(std::move(root_reader)) {}

Status VariantDocSnapshotRootIterator::init(const ColumnIteratorOptions& opts) {
    RETURN_IF_ERROR(VariantDocSnapshotIteratorBase::init(opts));
    DCHECK(_root_reader);
    RETURN_IF_ERROR(_root_reader->iterator->init(opts));
    return Status::OK();
}

Status VariantDocSnapshotRootIterator::seek_to_ordinal(ordinal_t ord) {
    RETURN_IF_ERROR(VariantDocSnapshotIteratorBase::seek_to_ordinal(ord));
    DCHECK(_root_reader);
    RETURN_IF_ERROR(_root_reader->iterator->seek_to_ordinal(ord));
    return Status::OK();
}

Status VariantDocSnapshotRootIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                                  bool* has_null) {
    DCHECK(_root_reader);
    RETURN_IF_ERROR(_root_reader->iterator->next_batch(n, _root_reader->column, has_null));
    std::vector<vectorized::ColumnPtr> doc_snapshot_data_buckets;
    RETURN_IF_ERROR(_collect_doc_snapshot_data(
            [&](BinaryColumnCache* cache) { return cache->next_batch(n, has_null); },
            &doc_snapshot_data_buckets));

    size_t num_rows = doc_snapshot_data_buckets.empty() ? 0 : doc_snapshot_data_buckets[0]->size();
    if (n != nullptr) {
        *n = num_rows;
    }
    RETURN_IF_ERROR(_merge_doc_snapshot_into_variant(dst, doc_snapshot_data_buckets, num_rows));
    _root_reader->column->clear();
    return Status::OK();
}

Status VariantDocSnapshotRootIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                                      vectorized::MutableColumnPtr& dst) {
    DCHECK(_root_reader);
    RETURN_IF_ERROR(_root_reader->iterator->read_by_rowids(rowids, count, _root_reader->column));
    std::vector<vectorized::ColumnPtr> doc_snapshot_data_buckets;
    RETURN_IF_ERROR(_collect_doc_snapshot_data(
            [&](BinaryColumnCache* cache) { return cache->read_by_rowids(rowids, count); },
            &doc_snapshot_data_buckets));

    size_t num_rows = doc_snapshot_data_buckets.empty() ? 0 : doc_snapshot_data_buckets[0]->size();
    RETURN_IF_ERROR(_merge_doc_snapshot_into_variant(dst, doc_snapshot_data_buckets, num_rows));
    _root_reader->column->clear();
    return Status::OK();
}

Status VariantDocSnapshotRootIterator::_merge_doc_snapshot_into_variant(
        vectorized::MutableColumnPtr& dst,
        const std::vector<vectorized::ColumnPtr>& doc_snapshot_data_buckets,
        size_t num_rows) const {
    using namespace vectorized;
    ColumnNullable* nullable_column = nullptr;
    if (dst->is_nullable()) {
        nullable_column = assert_cast<ColumnNullable*>(dst.get());
    }
    auto& variant = nullable_column == nullptr
                            ? assert_cast<ColumnVariant&>(*dst)
                            : assert_cast<ColumnVariant&>(nullable_column->get_nested_column());
    NullMap* null_map = nullable_column ? &nullable_column->get_null_map_data() : nullptr;

    std::vector<const ColumnString*> src_doc_snapshot_data_paths_buckets(
            doc_snapshot_data_buckets.size());
    std::vector<const ColumnString*> src_doc_snapshot_data_values_buckets(
            doc_snapshot_data_buckets.size());
    std::vector<const ColumnArray::Offsets64*> src_doc_snapshot_data_offsets_buckets(
            doc_snapshot_data_buckets.size());
    for (size_t i = 0; i != doc_snapshot_data_buckets.size(); ++i) {
        const auto& src_map =
                assert_cast<const vectorized::ColumnMap&>(*doc_snapshot_data_buckets[i]);
        src_doc_snapshot_data_paths_buckets[i] =
                assert_cast<const vectorized::ColumnString*>(&src_map.get_keys());
        src_doc_snapshot_data_values_buckets[i] =
                assert_cast<const vectorized::ColumnString*>(&src_map.get_values());
        src_doc_snapshot_data_offsets_buckets[i] =
                assert_cast<const vectorized::ColumnArray::Offsets64*>(&src_map.get_offsets());
    }

    CHECK(_root_reader);
    MutableColumnPtr root_column = _root_reader->column->get_ptr();
    DCHECK(root_column->size() == num_rows);
    auto root_nullable_column = make_nullable(root_column->get_ptr());
    auto root_type = make_nullable(_root_reader->type);
    MutableColumnPtr container = ColumnVariant::create(variant.max_subcolumns_count(), root_type,
                                                       root_nullable_column->assume_mutable());
    auto& container_variant = assert_cast<ColumnVariant&>(*container);
    vectorized::MutableColumnPtr doc_snapshot_column =
            vectorized::ColumnVariant::create_binary_column_fn();

    auto& column_map = assert_cast<ColumnMap&>(*doc_snapshot_column);
    auto& dst_doc_snapshot_data_paths =
            assert_cast<vectorized::ColumnString&>(column_map.get_keys());
    auto& dst_doc_snapshot_data_values =
            assert_cast<vectorized::ColumnString&>(column_map.get_values());
    auto& dst_doc_snapshot_data_offsets =
            assert_cast<vectorized::ColumnArray::Offsets64&>(column_map.get_offsets());
    for (size_t i = 0; i != num_rows; ++i) {
        std::vector<std::tuple<std::string_view, size_t, size_t>> all_paths;
        for (size_t bucket = 0; bucket != doc_snapshot_data_buckets.size(); ++bucket) {
            size_t offset_start = (*src_doc_snapshot_data_offsets_buckets[bucket])[ssize_t(i) - 1];
            size_t offset_end = (*src_doc_snapshot_data_offsets_buckets[bucket])[ssize_t(i)];
            for (size_t j = offset_start; j != offset_end; ++j) {
                auto path = src_doc_snapshot_data_paths_buckets[bucket]
                                    ->get_data_at(j)
                                    .to_string_view();
                all_paths.emplace_back(path, bucket, j);
            }
        }

        std::sort(all_paths.begin(), all_paths.end());
        for (const auto& [path, bucket, offset] : all_paths) {
            dst_doc_snapshot_data_paths.insert_data(path.data(), path.size());
            dst_doc_snapshot_data_values.insert_from(*src_doc_snapshot_data_values_buckets[bucket],
                                                     offset);
        }

        dst_doc_snapshot_data_offsets.push_back(dst_doc_snapshot_data_paths.size());
    }

    container_variant.set_doc_snapshot_column(std::move(doc_snapshot_column));

    if (null_map) {
        if (_root_reader->column->is_nullable()) {
            ColumnUInt8& src_null_map =
                    assert_cast<ColumnNullable&>(*_root_reader->column).get_null_map_column();
            nullable_column->get_null_map_column().insert_range_from(src_null_map, 0,
                                                                     src_null_map.size());
        } else {
            auto fake_nullable_column = ColumnUInt8::create(num_rows, 0);
            nullable_column->get_null_map_column().insert_range_from(*fake_nullable_column, 0,
                                                                     num_rows);
        }
    }
    variant.insert_range_from(container_variant, 0, num_rows);
    variant.finalize();
    return Status::OK();
}

VariantDocSnapshotPathIterator::VariantDocSnapshotPathIterator(
        std::vector<BinaryColumnCacheSPtr>&& doc_snapshot_column_caches, std::string path)
        : VariantDocSnapshotIteratorBase(std::move(doc_snapshot_column_caches)),
          _path(std::move(path)) {}

Status VariantDocSnapshotPathIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                                  bool* has_null) {
    std::vector<vectorized::ColumnPtr> doc_snapshot_data_buckets;
    RETURN_IF_ERROR(_collect_doc_snapshot_data(
            [&](BinaryColumnCache* cache) { return cache->next_batch(n, has_null); },
            &doc_snapshot_data_buckets));

    size_t num_rows = doc_snapshot_data_buckets.empty() ? 0 : doc_snapshot_data_buckets[0]->size();
    if (n != nullptr) {
        *n = num_rows;
    }
    return _merge_doc_snapshot_into_variant(dst, doc_snapshot_data_buckets, num_rows);
}

Status VariantDocSnapshotPathIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                                      vectorized::MutableColumnPtr& dst) {
    std::vector<vectorized::ColumnPtr> doc_snapshot_data_buckets;
    RETURN_IF_ERROR(_collect_doc_snapshot_data(
            [&](BinaryColumnCache* cache) { return cache->read_by_rowids(rowids, count); },
            &doc_snapshot_data_buckets));

    size_t num_rows = doc_snapshot_data_buckets.empty() ? 0 : doc_snapshot_data_buckets[0]->size();
    RETURN_IF_ERROR(_merge_doc_snapshot_into_variant(dst, doc_snapshot_data_buckets, num_rows));
    return Status::OK();
}

Status VariantDocSnapshotPathIterator::_merge_doc_snapshot_into_variant(
        vectorized::MutableColumnPtr& dst,
        const std::vector<vectorized::ColumnPtr>& doc_snapshot_data_buckets,
        size_t num_rows) const {
    using namespace vectorized;
    ColumnNullable* nullable_column = nullptr;
    if (dst->is_nullable()) {
        nullable_column = assert_cast<ColumnNullable*>(dst.get());
    }
    auto& variant = nullable_column == nullptr
                            ? assert_cast<ColumnVariant&>(*dst)
                            : assert_cast<ColumnVariant&>(nullable_column->get_nested_column());
    NullMap* null_map = nullable_column ? &nullable_column->get_null_map_data() : nullptr;

    std::vector<const ColumnString*> src_doc_snapshot_data_paths_buckets(
            doc_snapshot_data_buckets.size());
    std::vector<const ColumnString*> src_doc_snapshot_data_values_buckets(
            doc_snapshot_data_buckets.size());
    std::vector<const ColumnArray::Offsets64*> src_doc_snapshot_data_offsets_buckets(
            doc_snapshot_data_buckets.size());
    for (size_t i = 0; i != doc_snapshot_data_buckets.size(); ++i) {
        const auto& src_map =
                assert_cast<const vectorized::ColumnMap&>(*doc_snapshot_data_buckets[i]);
        src_doc_snapshot_data_paths_buckets[i] =
                assert_cast<const vectorized::ColumnString*>(&src_map.get_keys());
        src_doc_snapshot_data_values_buckets[i] =
                assert_cast<const vectorized::ColumnString*>(&src_map.get_values());
        src_doc_snapshot_data_offsets_buckets[i] =
                assert_cast<const vectorized::ColumnArray::Offsets64*>(&src_map.get_offsets());
    }

    MutableColumnPtr container = variant.clone_resized(0);
    auto& container_variant = assert_cast<ColumnVariant&>(*container);
    auto [variant_sparse_keys, variant_sparse_values] =
            container_variant.get_sparse_data_paths_and_values();
    auto& variant_sparse_offsets = container_variant.serialized_sparse_column_offsets();
    std::unordered_map<std::string_view, ColumnVariant::Subcolumn> subcolumn_map;
    StringRef prefix_ref(_path.data(), _path.size());
    for (size_t i = 0; i != num_rows; ++i) {
        bool has_data = false;
        std::vector<std::tuple<std::string_view, size_t, size_t>> all_paths;
        for (size_t bucket = 0; bucket != doc_snapshot_data_buckets.size(); ++bucket) {
            size_t offset_start = (*src_doc_snapshot_data_offsets_buckets[bucket])[ssize_t(i) - 1];
            size_t offset_end = (*src_doc_snapshot_data_offsets_buckets[bucket])[ssize_t(i)];
            size_t lower_bound_index =
                    vectorized::ColumnVariant::find_path_lower_bound_in_sparse_data(
                            prefix_ref, *src_doc_snapshot_data_paths_buckets[bucket], offset_start,
                            offset_end);
            for (; lower_bound_index != offset_end; ++lower_bound_index) {
                auto path_ref = (*src_doc_snapshot_data_paths_buckets[bucket])
                                        .get_data_at(lower_bound_index);
                std::string_view path(path_ref.data, path_ref.size);
                if (!path.starts_with(prefix_ref)) {
                    break;
                }
                if (path.size() == prefix_ref.size) {
                    has_data = true;
                    container_variant.get_subcolumn({})->deserialize_from_binary_column(
                            src_doc_snapshot_data_values_buckets[bucket], lower_bound_index);
                    continue;
                }
                if (path.size() > prefix_ref.size && path.at(prefix_ref.size) != '.') {
                    continue;
                }
                has_data = true;

                if (auto it = subcolumn_map.find(path); it != subcolumn_map.end()) {
                    it->second.deserialize_from_binary_column(
                            src_doc_snapshot_data_values_buckets[bucket], lower_bound_index);
                } else {
                    ColumnVariant::Subcolumn subcolumn(/*size*/ i, /*is_nullable*/ true, false);
                    subcolumn.deserialize_from_binary_column(
                            src_doc_snapshot_data_values_buckets[bucket], lower_bound_index);
                    subcolumn_map[path] = std::move(subcolumn);
                }
            }
        }
        variant_sparse_offsets.push_back(variant_sparse_keys->size());
        if (container_variant.get_subcolumn({})->size() == i) {
            container_variant.get_subcolumn({})->insert_default();
        }
        for (auto& [path, subcolumn] : subcolumn_map) {
            if (subcolumn.size() == i) {
                subcolumn.insert_default();
            }
        }
        container_variant.serialized_doc_snapshot_column_offsets().push_back(0);
        if (null_map) {
            if (has_data) {
                null_map->push_back(false);
            } else {
                null_map->push_back(true);
            }
        }
    }
    container_variant.set_num_rows(num_rows);
    for (auto& [path, subcolumn] : subcolumn_map) {
        subcolumn.finalize();
        if (!container_variant.add_sub_column(PathInData(path.substr(prefix_ref.size + 1)),
                                              IColumn::mutate(subcolumn.get_finalized_column_ptr()),
                                              subcolumn.get_least_common_type())) {
            return Status::InternalError("Failed to add subcolumn {}, which is from sparse column",
                                         path);
        }
    }
    variant.insert_range_from(container_variant, 0, num_rows);
    variant.finalize();
    return Status::OK();
}

#include "common/compile_check_end.h"

} // namespace doris::segment_v2
