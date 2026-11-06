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

#include "storage/segment/variant/binary_column_extract_iterator.h"

#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/variant_column_utils.h"
#include "core/custom_allocator.h"
#include "storage/segment/variant/v2/variant_column_reader.h"
#include "storage/segment/variant/v2/variant_storage_cell.h"

namespace doris::segment_v2 {

BinaryColumnExtractIterator::BinaryColumnExtractIterator(std::string_view path,
                                                         BinaryColumnCacheSPtr sparse_column_cache,
                                                         const StorageReadOptions* opts)
        : BaseBinaryColumnProcessor(std::move(sparse_column_cache), opts), _path(path) {}

Status BinaryColumnExtractIterator::next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) {
    RETURN_IF_ERROR(_validate_destination(*dst));
    {
        SCOPED_RAW_TIMER(&_read_opts->stats->variant_scan_sparse_column_timer_ns);
        const int64_t before_size = _read_opts->stats->uncompressed_bytes_read;
        bool ignored_physical_has_null = false;
        RETURN_IF_ERROR(_sparse_column_cache->next_batch(n, &ignored_physical_has_null));
        _read_opts->stats->variant_scan_sparse_column_bytes +=
                _read_opts->stats->uncompressed_bytes_read - before_size;
    }
    return _finish_variant_v2_batch(*n, dst, has_null);
}

Status BinaryColumnExtractIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                                   MutableColumnPtr& dst) {
    RETURN_IF_ERROR(_validate_destination(*dst));
    {
        SCOPED_RAW_TIMER(&_read_opts->stats->variant_scan_sparse_column_timer_ns);
        const int64_t before_size = _read_opts->stats->uncompressed_bytes_read;
        RETURN_IF_ERROR(_sparse_column_cache->read_by_rowids(rowids, count));
        _read_opts->stats->variant_scan_sparse_column_bytes +=
                _read_opts->stats->uncompressed_bytes_read - before_size;
    }
    return _finish_variant_v2_batch(count, dst, nullptr);
}

Status BinaryColumnExtractIterator::_validate_destination(IColumn& dst) const {
    if (variant_v2::try_get_variant_v2_destination(dst) != nullptr) {
        return Status::OK();
    }
    return Status::InvalidArgument("Variant V2 reader requires a ColumnVariantV2 destination");
}

Status BinaryColumnExtractIterator::_finish_variant_v2_batch(size_t num_rows, MutableColumnPtr& dst,
                                                             bool* has_null) {
    SCOPED_RAW_TIMER(&_read_opts->stats->variant_fill_path_from_sparse_column_timer_ns);
    if (_sparse_column_cache->binary_column->size() != num_rows) {
        return Status::Corruption("Variant sparse reader returned {} rows, expected {}",
                                  _sparse_column_cache->binary_column->size(), num_rows);
    }
    return _fill_variant_v2_path(dst, num_rows, has_null);
}

Status BinaryColumnExtractIterator::_fill_variant_v2_path(MutableColumnPtr& dst, size_t num_rows,
                                                          bool* has_null) {
    const auto* map = check_and_get_column<ColumnMap>(_sparse_column_cache->binary_column.get());
    if (map == nullptr || map->size() != num_rows) {
        return Status::Corruption("Variant sparse input must be a {}-row Map<String,String>",
                                  num_rows);
    }
    const auto* paths = check_and_get_column<ColumnString>(&map->get_keys());
    const auto* values = check_and_get_column<ColumnString>(&map->get_values());
    if (paths == nullptr || values == nullptr || paths->size() != values->size()) {
        return Status::Corruption("Variant sparse input is not Map<String,String>");
    }

    if (num_rows != 0 && map->get_offsets().back() != paths->size()) {
        return Status::Corruption("Variant sparse offsets consume {} of {} cells",
                                  map->get_offsets().back(), paths->size());
    }
    // Compaction promotes one physical path. For a sparse row containing only a.b,
    // assembling the a subtree would invent a non-null a and break path statistics.
    DorisVector<StringRef> cells(num_rows);
    DorisVector<uint8_t> missing(num_rows, 1);
    const StringRef requested {_path.data(), _path.size()};
    const auto& offsets = map->get_offsets();
    for (size_t row = 0; row < num_rows; ++row) {
        const size_t start = offsets[row - 1];
        const size_t end = offsets[row];
        // Corrupt offsets must not reach the binary search or cell decoder.
        if (start > end || end > paths->size()) {
            return Status::Corruption("Invalid Variant sparse offsets [{}, {}) for {} cells", start,
                                      end, paths->size());
        }
        const size_t pos = find_variant_sparse_path_lower_bound(requested, *paths, start, end);
        // Prefix matches such as a.b belong to another physical path.
        if (pos < end && paths->get_data_at(pos) == requested) {
            cells[row] = values->get_data_at(pos);
            missing[row] = 0;
        }
    }
    ColumnNullable::MutablePtr assembled;
    RETURN_IF_ERROR(variant_v2::decode_v1_storage_cells(cells, {}, missing, &assembled));
    if (has_null != nullptr) {
        *has_null = assembled->has_null();
    }
    return variant_v2::append_assembled_variant(dst, std::move(assembled));
}

void BinaryColumnExtractIterator::_process_data_with_existing_sparse_column(MutableColumnPtr& dst,
                                                                            size_t num_rows) {
    dst->insert_many_defaults(num_rows);
}

void BinaryColumnExtractIterator::_process_data_without_sparse_column(MutableColumnPtr& dst,
                                                                      size_t num_rows) {
    dst->insert_many_defaults(num_rows);
}

} // namespace doris::segment_v2
