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

#include "storage/segment/segment_key_seeker.h"

#include "common/check.h"
#include "common/status.h"
#include "core/column/column.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/field.h"
#include "storage/index/short_key_index.h"
#include "storage/iterators.h"
#include "storage/key/row_key_encoder.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment.h"

namespace doris {
namespace {
class StoredSegmentKeySeeker final : public SegmentKeySeeker {
public:
    StoredSegmentKeySeeker(std::shared_ptr<segment_v2::Segment> segment,
                           const StorageReadOptions& options)
            : _segment(std::move(segment)),
              _options(options),
              _encoder(*_segment->tablet_schema(), false) {}

    const ShortKeyIndexDecoder& index() const override { return *_segment->get_short_key_index(); }

    Status read_prefix(uint32_t row, std::string* prefix) override {
        const size_t count = _segment->tablet_schema()->num_short_key_columns();
        RETURN_IF_ERROR(read(row, count));
        std::vector<const IColumn*> columns;
        for (size_t i = 0; i < count; ++i) {
            columns.push_back(_columns[i].get());
        }
        *prefix = _encoder.encode_short_keys(columns, 0);
        return Status::OK();
    }

    Status read_tuple(uint32_t row, std::vector<Field>* tuple, size_t* bytes) override {
        RETURN_IF_ERROR(read(row, _segment->tablet_schema()->num_key_columns()));
        tuple->clear();
        *bytes = 0;
        for (const auto& column : _columns) {
            tuple->push_back((*column)[0]);
            *bytes += sizeof(Field) + column->byte_size();
        }
        return Status::OK();
    }

    Status read_key(uint32_t row, std::string* prefix, std::vector<Field>* tuple,
                    size_t* bytes) override {
        const auto& schema = *_segment->tablet_schema();
        RETURN_IF_ERROR(read(row, schema.num_key_columns()));
        std::vector<const IColumn*> short_cols;
        short_cols.reserve(schema.num_short_key_columns());
        for (size_t i = 0; i < schema.num_short_key_columns(); ++i) {
            short_cols.push_back(_columns[i].get());
        }
        *prefix = _encoder.encode_short_keys(short_cols, 0);
        tuple->clear();
        *bytes = 0;
        for (const auto& column : _columns) {
            tuple->push_back((*column)[0]);
            *bytes += sizeof(Field) + column->byte_size();
        }
        return Status::OK();
    }

private:
    Status read(uint32_t row, size_t count) {
        for (size_t i = 0; i < count; ++i) {
            if (i == _iterators.size()) {
                const auto& col = _segment->tablet_schema()->column(i);
                std::unique_ptr<segment_v2::ColumnIterator> iter;
                RETURN_IF_ERROR(_segment->new_column_iterator(col, &iter, &_options));
                RETURN_IF_ERROR(iter->init({.use_page_cache = _options.use_page_cache,
                                            .file_reader = _segment->file_reader().get(),
                                            .stats = _options.stats,
                                            .io_ctx = _options.io_ctx}));
                _iterators.push_back(std::move(iter));
                _columns.push_back(
                        DataTypeFactory::instance().create_data_type(col)->create_column());
            }
            _columns[i]->clear();
            RETURN_IF_ERROR(_iterators[i]->seek_to_ordinal(row));
            size_t n = 1;
            RETURN_IF_ERROR(_iterators[i]->next_batch(&n, _columns[i]));
            if (n != 1) {
                return Status::Corruption("Missing key at row {} of segment {}", row,
                                          _segment->id());
            }
        }
        return Status::OK();
    }

    std::shared_ptr<segment_v2::Segment> _segment;
    StorageReadOptions _options;
    RowKeyEncoder _encoder;
    std::vector<std::unique_ptr<segment_v2::ColumnIterator>> _iterators;
    MutableColumns _columns;
};
} // namespace

Status SegmentKeySeeker::validate_schema(const TabletSchema& target, const TabletSchema& stored) {
    if ((target.keys_type() != UNIQUE_KEYS && target.keys_type() != AGG_KEYS) ||
        target.keys_type() != stored.keys_type() || target.sort_type() != SortType::LEXICAL ||
        stored.sort_type() != SortType::LEXICAL || !target.cluster_key_uids().empty() ||
        !stored.cluster_key_uids().empty() ||
        target.num_key_columns() != stored.num_key_columns() ||
        target.num_short_key_columns() != stored.num_short_key_columns() ||
        !stored.num_short_key_columns()) {
        return Status::NotSupported(
                "KEY_RANGE_BLOCK requires compatible lexical MOR or AGG sort keys");
    }
    for (size_t i = 0; i < target.num_key_columns(); ++i) {
        const auto& a = target.column(i);
        const auto& b = stored.column(i);
        // A truncated non-final field can reverse short-key order when the next field differs.
        if (i + 1 < stored.num_short_key_columns() &&
            (b.type() == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
             (b.type() == FieldType::OLAP_FIELD_TYPE_CHAR && b.index_length() != b.length()))) {
            return Status::NotSupported("Unordered intermediate short-key prefix at column {}", i);
        }
        if (a.unique_id() != b.unique_id() || (a.unique_id() < 0 && a.name() != b.name()) ||
            a.type() != b.type() || a.is_nullable() != b.is_nullable() || a.frac() != b.frac() ||
            a.precision() != b.precision() ||
            (a.type() == FieldType::OLAP_FIELD_TYPE_CHAR && a.length() != b.length()) ||
            (i < target.num_short_key_columns() && a.index_length() != b.index_length())) {
            return Status::NotSupported("Incompatible KEY_RANGE_BLOCK sort key at column {}", i);
        }
    }
    return Status::OK();
}

Status SegmentKeySeeker::open(std::shared_ptr<segment_v2::Segment> segment,
                              const TabletSchema& target, const StorageReadOptions& options,
                              std::unique_ptr<SegmentKeySeeker>* out, const KeyBoundsPB* bounds) {
    RETURN_IF_ERROR(validate_schema(target, *segment->tablet_schema()));
    RETURN_IF_ERROR(segment->load_index(options.stats, &options.io_ctx));
    auto seeker = std::make_unique<StoredSegmentKeySeeker>(std::move(segment), options);
    // With a single sort key the short key is a prefix of the full encoding.
    // A full maximum below the requested prefix therefore excludes even the last partial block.
    if (bounds && target.num_key_columns() == 1) {
        seeker->_max_key = bounds->max_key();
    }
    *out = std::move(seeker);
    return Status::OK();
}

Status SegmentKeySeeker::read_key(uint32_t row, std::string* prefix, std::vector<Field>* tuple,
                                  size_t* bytes) {
    RETURN_IF_ERROR(read_prefix(row, prefix));
    return read_tuple(row, tuple, bytes);
}

Status SegmentKeySeeker::lower_bound(const std::string& prefix, const std::vector<Field>& tuple,
                                     uint32_t rows, uint32_t* ordinal) {
    if (!_max_key.empty() && _max_key < prefix) {
        *ordinal = rows;
        return Status::OK();
    }
    auto [lo, hi] = index().ordinal_window(prefix, rows, tuple.empty());
    std::string encoded;
    std::vector<Field> key;
    while (lo < hi) {
        const uint32_t mid = lo + (hi - lo) / 2;
        bool before;
        if (tuple.empty()) {
            RETURN_IF_ERROR(read_prefix(mid, &encoded));
            before = encoded < prefix;
        } else {
            size_t bytes;
            RETURN_IF_ERROR(read_tuple(mid, &key, &bytes));
            before = key < tuple;
        }
        if (before) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    *ordinal = lo;
    return Status::OK();
}
} // namespace doris
