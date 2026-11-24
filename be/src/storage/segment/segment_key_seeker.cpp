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

#include <algorithm>

#include "common/check.h"
#include "common/consts.h"
#include "common/status.h"
#include "core/column/column.h"
#include "core/data_type/data_type_factory.hpp"
#include "storage/index/short_key_index.h"
#include "storage/iterators.h"
#include "storage/key/row_key_encoder.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/sampling/tablet_sample_plan.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_loader.h"
#include "storage/tablet/tablet_reader.h"

namespace doris {
namespace {
class StoredSegmentKeySeeker final : public SegmentKeySeeker {
public:
    StoredSegmentKeySeeker(std::shared_ptr<segment_v2::Segment> segment,
                           const StorageReadOptions& options)
            : _segment(std::move(segment)),
              _options(options),
              _encoder(*_segment->tablet_schema(), false) {
        const auto& schema = *_segment->tablet_schema();
        _effective_short_key_columns = schema.num_short_key_columns();
        for (size_t i = 0; i + 1 < schema.num_short_key_columns(); ++i) {
            const auto& col = schema.column(i);
            if (col.type() == FieldType::OLAP_FIELD_TYPE_CHAR &&
                col.index_length() < col.length()) {
                _effective_short_key_columns = i + 1;
                break;
            }
        }
        if (_effective_short_key_columns < schema.num_short_key_columns()) {
            _prefix_field_widths.reserve(_effective_short_key_columns);
            for (size_t i = 0; i < _effective_short_key_columns; ++i) {
                _prefix_field_widths.push_back(
                        static_cast<uint32_t>(schema.column(i).index_length()));
            }
        }
    }

    const ShortKeyIndexDecoder& index() const override { return *_segment->get_short_key_index(); }

    Slice index_prefix(uint32_t block) const override {
        const Slice raw = index().key(block);
        if (_prefix_field_widths.empty()) {
            return raw;
        }
        size_t offset = 0;
        for (uint32_t width : _prefix_field_widths) {
            DORIS_CHECK(offset < raw.size);
            const auto marker = static_cast<uint8_t>(raw.data[offset]);
            ++offset;
            if (marker != KeyConsts::KEY_NULL_FIRST_MARKER) {
                DORIS_CHECK(offset + width <= raw.size);
                offset += width;
            }
        }
        return {raw.data, offset};
    }

    Status read_prefix(uint32_t row, std::string* prefix) override {
        const size_t count = _effective_short_key_columns;
        RETURN_IF_ERROR(read(row, count));
        std::vector<const IColumn*> columns;
        columns.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            columns.push_back(_columns[i].get());
        }
        *prefix = _encoder.encode_short_keys(columns, 0);
        return Status::OK();
    }

    Status read_tuple(uint32_t row, std::vector<std::string>* tuple, size_t* bytes) override {
        const size_t count = _segment->tablet_schema()->num_key_columns();
        RETURN_IF_ERROR(read(row, count));
        std::vector<const IColumn*> key_cols;
        key_cols.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            key_cols.push_back(_columns[i].get());
        }
        _encoder.encode_sort_key_fields(key_cols, 0, tuple);
        *bytes = 0;
        for (const auto& field : *tuple) {
            *bytes += sizeof(std::string) + field.size();
        }
        return Status::OK();
    }

    Status read_key(uint32_t row, std::string* prefix, std::vector<std::string>* tuple,
                    size_t* bytes) override {
        const auto& schema = *_segment->tablet_schema();
        RETURN_IF_ERROR(read(row, schema.num_key_columns()));
        std::vector<const IColumn*> key_cols;
        key_cols.reserve(schema.num_key_columns());
        for (size_t i = 0; i < schema.num_key_columns(); ++i) {
            key_cols.push_back(_columns[i].get());
        }
        _encoder.encode_sort_key_fields(key_cols, 0, tuple);
        key_cols.resize(_effective_short_key_columns);
        *prefix = _encoder.encode_short_keys(key_cols, 0);
        *bytes = 0;
        for (const auto& field : *tuple) {
            *bytes += sizeof(std::string) + field.size();
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
    size_t _effective_short_key_columns = 0;
    std::vector<uint32_t> _prefix_field_widths;
    std::vector<std::unique_ptr<segment_v2::ColumnIterator>> _iterators;
    MutableColumns _columns;
};
} // namespace

Slice SegmentKeySeeker::index_prefix(uint32_t block) const {
    return index().key(block);
}

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
        // Variable-length intermediate short-key fields lack a field terminator; truncated CHAR
        // fields remain fixed-width and are handled by slicing at _effective_short_key_columns.
        if (i + 1 < stored.num_short_key_columns() &&
            (b.type() == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
             b.type() == FieldType::OLAP_FIELD_TYPE_STRING)) {
            return Status::NotSupported("Unordered intermediate short-key prefix at column {}", i);
        }
        if (b.type() == FieldType::OLAP_FIELD_TYPE_CHAR &&
            (b.index_length() <= 0 || b.index_length() > b.length())) {
            return Status::NotSupported("Invalid CHAR index_length at column {}", i);
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
    // validate_schema requires all non-final short-key columns to be fixed-width and untruncated,
    // so encode_short_keys(last_row) is always a byte prefix of full_encode(last_row) (max_key).
    // For example, ("abc", 42) with 1 short key encodes prefix "\x02abc" <= max_key "\x02abc\x02...".
    // Therefore max_key < prefix implies last_row_short_key < prefix even on composite sort keys.
    if (bounds) {
        seeker->_max_key = bounds->max_key();
    }
    *out = std::move(seeker);
    return Status::OK();
}

Status SegmentKeySeeker::open_read_source(const std::vector<RowSetSplits>& rs_splits,
                                          const TabletSchema& target, StorageReadOptions& options,
                                          bool use_segment_cache,
                                          std::vector<std::unique_ptr<SegmentKeySeeker>>* seekers,
                                          std::vector<SampleSegment>* segments) {
    for (const auto& split : rs_splits) {
        auto rowset = std::static_pointer_cast<BetaRowset>(split.rs_reader->rowset());
        RETURN_IF_ERROR(rowset->load());
        SegmentCacheHandle handle;
        RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(
                rowset, &handle, use_segment_cache, false, options.stats, &options.io_ctx));
        for (auto ref : rowset->segments()) {
            const auto& segment = handle.get_segments()[ref.pos()];
            std::unique_ptr<SegmentKeySeeker> seeker;
            if (segment->num_rows() > 0) {
                const auto* bounds =
                        ref.has_position_key_bounds() && !rowset->is_segments_key_bounds_truncated()
                                ? &ref.key_bounds()
                                : nullptr;
                RETURN_IF_ERROR(open(segment, target, options, &seeker, bounds));
            }
            segments->push_back({rowset->rowset_id(), ref.id(), segment->num_rows(), seeker.get()});
            seekers->push_back(std::move(seeker));
        }
    }
    return Status::OK();
}

Status SegmentKeySeeker::read_key(uint32_t row, std::string* prefix,
                                  std::vector<std::string>* tuple, size_t* bytes) {
    RETURN_IF_ERROR(read_prefix(row, prefix));
    return read_tuple(row, tuple, bytes);
}

std::pair<uint32_t, uint32_t> SegmentKeySeeker::_prefix_ordinal_window(const Slice& prefix,
                                                                       uint32_t limit,
                                                                       bool prefix_start) const {
    const uint32_t n = index().num_items();
    uint32_t lo = 0;
    uint32_t hi = n;
    while (lo < hi) {
        const uint32_t mid = lo + (hi - lo) / 2;
        if (index_prefix(mid).compare(prefix) < 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    const uint32_t first = lo;
    uint32_t past = first;
    if (!prefix_start) {
        lo = first;
        hi = n;
        while (lo < hi) {
            const uint32_t mid = lo + (hi - lo) / 2;
            if (prefix.compare(index_prefix(mid)) < 0) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        past = lo;
    }
    const uint64_t stride = index().num_rows_per_block();
    return {static_cast<uint32_t>(std::min<uint64_t>(limit, (first > 0 ? first - 1 : 0) * stride)),
            static_cast<uint32_t>(std::min<uint64_t>(limit, past * stride))};
}

Status SegmentKeySeeker::lower_bound(const std::string& prefix,
                                     const std::vector<std::string>& tuple, uint32_t rows,
                                     uint32_t* ordinal, uint32_t min_ordinal) {
    if (min_ordinal >= rows || (!_max_key.empty() && _max_key < prefix)) {
        *ordinal = rows;
        return Status::OK();
    }
    auto [lo, hi] = _prefix_ordinal_window(prefix, rows, tuple.empty());
    lo = std::min(rows, std::max(lo, min_ordinal));
    std::string encoded;
    std::vector<std::string> key;
    auto is_before = [&](uint32_t pos, bool* out) -> Status {
        if (tuple.empty()) {
            RETURN_IF_ERROR(read_prefix(pos, &encoded));
            *out = encoded < prefix;
        } else {
            size_t bytes = 0;
            RETURN_IF_ERROR(read_tuple(pos, &key, &bytes));
            *out = key < tuple;
        }
        return Status::OK();
    };
    // Gallop forward from `lo` before binary-searching so monotonic interval bounds stay inside
    // FileColumnIterator's currently decompressed page instead of ping-ponging to `hi` when
    // coarse short-key prefixes (e.g. low-cardinality VARCHAR) span multiple data pages.
    if (hi - lo > 64) {
        uint32_t step = 1;
        while (lo < hi) {
            const uint32_t span = hi - lo;
            const uint32_t probe = step < span ? lo + step - 1 : hi - 1;
            bool before = false;
            RETURN_IF_ERROR(is_before(probe, &before));
            if (!before) {
                hi = probe;
                break;
            }
            lo = probe + 1;
            step = step <= (hi - lo) / 2 ? (step << 1) : (hi - lo);
        }
    }
    while (lo < hi) {
        const uint32_t mid = lo + (hi - lo) / 2;
        bool before = false;
        RETURN_IF_ERROR(is_before(mid, &before));
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
