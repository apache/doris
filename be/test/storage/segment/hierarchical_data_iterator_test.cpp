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

#include "storage/segment/variant/hierarchical_data_iterator.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <iterator>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nothing.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "storage/segment/column_reader_cache.h"
#include "storage/segment/variant/v2/variant_assembler.h"
#include "util/json/path_in_data.h"

using doris::Status;
using doris::segment_v2::ColumnIterator;
using doris::segment_v2::ColumnIteratorOptions;
using doris::segment_v2::HierarchicalDataIterator;
using doris::segment_v2::SubstreamIterator;
using doris::ColumnMap;
using doris::ColumnString;
using doris::ColumnVariant;
using doris::MutableColumnPtr;
using doris::OlapReaderStatistics;
using doris::PathInData;

class DummySparseIterator final : public ColumnIterator {
public:
    Status init(const ColumnIteratorOptions&) override { return Status::OK(); }
    Status seek_to_ordinal(ordinal_t ordinal) override {
        _current_ordinal = ordinal;
        return Status::OK();
    }
    ordinal_t get_current_ordinal() const override { return _current_ordinal; }

    Status next_batch(size_t* rows, MutableColumnPtr& dst, bool*) override {
        if (*rows < 2) {
            return Status::InvalidArgument("Dummy sparse reader requires room for two rows");
        }
        *rows = 2;
        _current_ordinal += *rows;
        return fill(dst);
    }

    Status read_by_rowids(const doris::segment_v2::rowid_t*, const size_t count,
                          MutableColumnPtr& dst) override {
        if (count != 2) {
            return Status::InvalidArgument("Dummy sparse reader requires two rows");
        }
        return fill(dst);
    }

private:
    static Status fill(MutableColumnPtr& dst) {
        auto* map = check_and_get_column<ColumnMap>(dst.get());
        if (map == nullptr) {
            return Status::InvalidArgument("Dummy sparse destination is not a map");
        }
        auto& keys = assert_cast<ColumnString&>(map->get_keys());
        auto& values = assert_cast<ColumnString&>(map->get_values());
        auto& offsets = map->get_offsets();

        doris::DataTypePtr string_type = std::make_shared<doris::DataTypeString>();
        auto strings = string_type->create_column();
        auto serde = string_type->get_serde();
        strings->insert_data("abcvalues", strlen("abcvalues"));
        strings->insert_data("abdvalues", strlen("abdvalues"));
        strings->insert_data("abcvalues", strlen("abcvalues"));
        strings->insert_data("abevalues", strlen("abevalues"));
        strings->insert_data("axvalues", strlen("axvalues"));
        ColumnString::Chars& chars = values.get_chars();
        for (size_t index = 0; index < 5; ++index) {
            serde->write_one_cell_to_binary(*strings, chars, index);
            values.get_offsets().push_back(chars.size());
        }

        keys.insert_data("a.b.c", strlen("a.b.c"));
        keys.insert_data("a.b.d", strlen("a.b.d"));
        offsets.push_back(keys.size());
        keys.insert_data("a.b.c", strlen("a.b.c"));
        keys.insert_data("a.b.e", strlen("a.b.e"));
        keys.insert_data("a.x", strlen("a.x"));
        offsets.push_back(keys.size());
        return Status::OK();
    }

    ordinal_t _current_ordinal = 0;
};

class EmptySparseIterator final : public ColumnIterator {
public:
    explicit EmptySparseIterator(size_t total_rows = std::numeric_limits<size_t>::max(),
                                 bool misreport_next_batch = false)
            : _total_rows(total_rows), _misreport_next_batch(misreport_next_batch) {}

    Status init(const ColumnIteratorOptions&) override { return Status::OK(); }
    Status seek_to_ordinal(ordinal_t ordinal) override {
        _current_ordinal = ordinal;
        return Status::OK();
    }
    ordinal_t get_current_ordinal() const override { return _current_ordinal; }

    Status next_batch(size_t* rows, MutableColumnPtr& dst, bool*) override {
        auto* map = check_and_get_column<ColumnMap>(dst.get());
        if (map == nullptr) {
            return Status::InvalidArgument("Empty sparse destination is not a map");
        }
        const size_t requested_rows = *rows;
        const size_t remaining_rows =
                _current_ordinal < _total_rows ? _total_rows - _current_ordinal : 0;
        const size_t produced_rows = std::min(requested_rows, remaining_rows);
        map->get_offsets().resize_fill(produced_rows, 0);
        _current_ordinal += produced_rows;
        if (!_misreport_next_batch) {
            *rows = produced_rows;
        }
        return Status::OK();
    }

    Status read_by_rowids(const doris::segment_v2::rowid_t*, const size_t count,
                          MutableColumnPtr& dst) override {
        auto* map = check_and_get_column<ColumnMap>(dst.get());
        if (map == nullptr) {
            return Status::InvalidArgument("Empty sparse destination is not a map");
        }
        map->get_offsets().resize_fill(std::min(count, _total_rows), 0);
        return Status::OK();
    }

private:
    ordinal_t _current_ordinal = 0;
    size_t _total_rows;
    bool _misreport_next_batch;
};

struct ConsecutiveBatchState {
    size_t next_batch_calls = 0;
    bool scratch_was_always_empty = true;
    ordinal_t current_ordinal = 0;
    std::vector<size_t> produced_batches;
};

class ThreeRowJsonbIterator final : public ColumnIterator {
public:
    explicit ThreeRowJsonbIterator(std::shared_ptr<ConsecutiveBatchState> state,
                                   bool reported_has_null = false, std::string_view json = "{}")
            : _state(std::move(state)), _reported_has_null(reported_has_null), _json(json) {}

    Status init(const ColumnIteratorOptions&) override { return Status::OK(); }
    Status seek_to_ordinal(ordinal_t ordinal) override {
        _state->current_ordinal = ordinal;
        return Status::OK();
    }
    ordinal_t get_current_ordinal() const override { return _state->current_ordinal; }

    Status next_batch(size_t* rows, MutableColumnPtr& dst, bool* has_null) override {
        _state->scratch_was_always_empty &= dst->empty();
        if (!dst->empty()) {
            return Status::Corruption("JSONB scratch column was not cleared between batches");
        }
        auto* strings = check_and_get_column<ColumnString>(dst.get());
        if (strings == nullptr) {
            return Status::InvalidArgument("JSONB destination is not a string column");
        }

        const size_t produced = std::min<size_t>(*rows, ROWS - _state->current_ordinal);
        auto serde = std::make_shared<doris::DataTypeJsonb>()->get_serde();
        doris::DataTypeSerDe::FormatOptions options;
        for (size_t row = 0; row < produced; ++row) {
            doris::Slice json(_json.data(), _json.size());
            RETURN_IF_ERROR(serde->deserialize_one_cell_from_json(*strings, json, options));
        }
        *rows = produced;
        _state->current_ordinal += produced;
        ++_state->next_batch_calls;
        _state->produced_batches.push_back(produced);
        if (has_null != nullptr) {
            *has_null = _reported_has_null;
        }
        return Status::OK();
    }

    Status read_by_rowids(const doris::segment_v2::rowid_t*, const size_t,
                          MutableColumnPtr&) override {
        return Status::NotSupported("ThreeRowJsonbIterator only supports sequential reads");
    }

private:
    static constexpr size_t ROWS = 3;
    std::shared_ptr<ConsecutiveBatchState> _state;
    bool _reported_has_null;
    std::string _json;
};

class ThreeRowSparseIterator final : public ColumnIterator {
public:
    explicit ThreeRowSparseIterator(std::shared_ptr<ConsecutiveBatchState> state)
            : _state(std::move(state)) {}

    Status init(const ColumnIteratorOptions&) override { return Status::OK(); }
    Status seek_to_ordinal(ordinal_t ordinal) override {
        _state->current_ordinal = ordinal;
        return Status::OK();
    }
    ordinal_t get_current_ordinal() const override { return _state->current_ordinal; }

    Status next_batch(size_t* rows, MutableColumnPtr& dst, bool* has_null) override {
        _state->scratch_was_always_empty &= dst->empty();
        if (!dst->empty()) {
            return Status::Corruption("sparse scratch column was not cleared between batches");
        }
        auto* map = check_and_get_column<ColumnMap>(dst.get());
        if (map == nullptr) {
            return Status::InvalidArgument("sparse destination is not a map");
        }

        const size_t produced = std::min<size_t>(*rows, ROWS - _state->current_ordinal);
        auto& keys = assert_cast<ColumnString&>(map->get_keys());
        auto& values = assert_cast<ColumnString&>(map->get_values());
        auto& map_offsets = map->get_offsets();
        auto source = ColumnString::create();
        for (size_t row = 0; row < produced; ++row) {
            const std::string_view value = VALUES[_state->current_ordinal + row];
            source->insert_data(value.data(), value.size());
        }
        auto serde = std::make_shared<doris::DataTypeString>()->get_serde();
        auto& value_chars = values.get_chars();
        for (size_t row = 0; row < produced; ++row) {
            keys.insert_data("s", 1);
            serde->write_one_cell_to_binary(*source, value_chars, row);
            values.get_offsets().push_back(value_chars.size());
            map_offsets.push_back(keys.size());
        }

        *rows = produced;
        _state->current_ordinal += produced;
        ++_state->next_batch_calls;
        _state->produced_batches.push_back(produced);
        if (has_null != nullptr) {
            *has_null = false;
        }
        return Status::OK();
    }

    Status read_by_rowids(const doris::segment_v2::rowid_t*, const size_t,
                          MutableColumnPtr&) override {
        return Status::NotSupported("ThreeRowSparseIterator only supports sequential reads");
    }

private:
    static constexpr size_t ROWS = 3;
    static constexpr std::array<std::string_view, ROWS> VALUES {"row0", "row1", "row2"};
    std::shared_ptr<ConsecutiveBatchState> _state;
};

class TwoRowJsonbSparseIterator final : public ColumnIterator {
public:
    explicit TwoRowJsonbSparseIterator(std::string path) : _path(std::move(path)) {}

    Status init(const ColumnIteratorOptions&) override { return Status::OK(); }
    Status seek_to_ordinal(ordinal_t ordinal) override {
        _current_ordinal = ordinal;
        return Status::OK();
    }
    ordinal_t get_current_ordinal() const override { return _current_ordinal; }

    Status next_batch(size_t* rows, MutableColumnPtr& dst, bool* has_null) override {
        auto* map = check_and_get_column<ColumnMap>(dst.get());
        if (map == nullptr) {
            return Status::InvalidArgument("JSONB sparse destination is not a map");
        }

        const size_t produced = std::min<size_t>(*rows, ROWS - _current_ordinal);
        auto& keys = assert_cast<ColumnString&>(map->get_keys());
        auto& values = assert_cast<ColumnString&>(map->get_values());
        auto& offsets = map->get_offsets();
        auto jsonb = std::make_shared<doris::DataTypeJsonb>();
        auto source = jsonb->create_column();
        auto serde = jsonb->get_serde();
        doris::DataTypeSerDe::FormatOptions options;
        for (size_t row = 0; row < produced; ++row) {
            const std::string_view value = VALUES[_current_ordinal + row];
            doris::Slice json(value.data(), value.size());
            RETURN_IF_ERROR(serde->deserialize_one_cell_from_json(*source, json, options));
        }

        auto& value_chars = values.get_chars();
        for (size_t row = 0; row < produced; ++row) {
            keys.insert_data(_path.data(), _path.size());
            serde->write_one_cell_to_binary(*source, value_chars, row);
            values.get_offsets().push_back(value_chars.size());
            offsets.push_back(keys.size());
        }

        *rows = produced;
        _current_ordinal += produced;
        if (has_null != nullptr) {
            *has_null = false;
        }
        return Status::OK();
    }

    Status read_by_rowids(const doris::segment_v2::rowid_t*, const size_t,
                          MutableColumnPtr&) override {
        return Status::NotSupported("TwoRowJsonbSparseIterator only supports sequential reads");
    }

private:
    static constexpr size_t ROWS = 2;
    static constexpr std::array<std::string_view, ROWS> VALUES {"null", "1"};
    std::string _path;
    ordinal_t _current_ordinal = 0;
};

class TwoRowNullableJsonbIterator final : public ColumnIterator {
public:
    Status init(const ColumnIteratorOptions&) override { return Status::OK(); }
    Status seek_to_ordinal(ordinal_t ordinal) override {
        _current_ordinal = ordinal;
        return Status::OK();
    }
    ordinal_t get_current_ordinal() const override { return _current_ordinal; }

    Status next_batch(size_t* rows, MutableColumnPtr& dst, bool* has_null) override {
        if (*rows < 2) {
            return Status::InvalidArgument("Nullable JSONB reader requires room for two rows");
        }
        auto* nullable = check_and_get_column<doris::ColumnNullable>(dst.get());
        if (nullable == nullptr) {
            return Status::InvalidArgument("Nullable JSONB destination is not nullable");
        }
        auto& strings = assert_cast<ColumnString&>(nullable->get_nested_column());
        auto serde = std::make_shared<doris::DataTypeJsonb>()->get_serde();
        doris::DataTypeSerDe::FormatOptions options;
        constexpr std::string_view EMPTY_OBJECT = "{}";
        for (size_t row = 0; row < 2; ++row) {
            doris::Slice json(EMPTY_OBJECT.data(), EMPTY_OBJECT.size());
            RETURN_IF_ERROR(serde->deserialize_one_cell_from_json(strings, json, options));
        }
        nullable->get_null_map_data().push_back(1);
        nullable->get_null_map_data().push_back(0);
        *rows = 2;
        _current_ordinal += 2;
        if (has_null != nullptr) {
            *has_null = true;
        }
        return Status::OK();
    }

    Status read_by_rowids(const doris::segment_v2::rowid_t*, const size_t,
                          MutableColumnPtr&) override {
        return Status::NotSupported("TwoRowNullableJsonbIterator only supports sequential reads");
    }

private:
    ordinal_t _current_ordinal = 0;
};

struct OwnedTypedStreamState {
    const doris::IColumn* wrapper = nullptr;
    const doris::IColumn* payload = nullptr;
    ordinal_t current_ordinal = 0;
};

class ThreeRowNullableInt32Iterator final : public ColumnIterator {
public:
    explicit ThreeRowNullableInt32Iterator(std::shared_ptr<OwnedTypedStreamState> state)
            : _state(std::move(state)) {}

    Status init(const ColumnIteratorOptions&) override { return Status::OK(); }
    Status seek_to_ordinal(ordinal_t ordinal) override {
        if (ordinal > VALUES.size()) {
            return Status::InvalidArgument("typed stream ordinal {} exceeds row count {}", ordinal,
                                           VALUES.size());
        }
        _state->current_ordinal = ordinal;
        return Status::OK();
    }
    ordinal_t get_current_ordinal() const override { return _state->current_ordinal; }

    Status next_batch(size_t* rows, MutableColumnPtr& dst, bool* has_null) override {
        auto* nullable = check_and_get_column<doris::ColumnNullable>(dst.get());
        if (nullable == nullptr) {
            return Status::InvalidArgument("typed stream destination is not nullable");
        }
        auto* values = check_and_get_column<doris::ColumnInt32>(&nullable->get_nested_column());
        if (values == nullptr) {
            return Status::InvalidArgument("typed stream payload is not Int32");
        }

        DCHECK_LE(_state->current_ordinal, VALUES.size());
        const size_t produced = std::min(*rows, VALUES.size() - _state->current_ordinal);
        _state->wrapper = nullable;
        _state->payload = values;
        bool batch_has_null = false;
        for (size_t row = 0; row < produced; ++row) {
            const size_t source_row = _state->current_ordinal + row;
            values->insert_value(VALUES[source_row]);
            nullable->get_null_map_data().push_back(NULLS[source_row]);
            batch_has_null = batch_has_null || NULLS[source_row] != 0;
        }
        *rows = produced;
        _state->current_ordinal += produced;
        if (has_null != nullptr) {
            *has_null = batch_has_null;
        }
        return Status::OK();
    }

    Status read_by_rowids(const doris::segment_v2::rowid_t*, const size_t,
                          MutableColumnPtr&) override {
        return Status::NotSupported("ThreeRowNullableInt32Iterator only supports sequential reads");
    }

private:
    static constexpr std::array<doris::Int32, 3> VALUES {10, 0, 30};
    static constexpr std::array<doris::UInt8, 3> NULLS {0, 1, 0};
    std::shared_ptr<OwnedTypedStreamState> _state;
};

class SingleTypedColumnReader final : public doris::segment_v2::ColumnReader {
public:
    explicit SingleTypedColumnReader(std::shared_ptr<OwnedTypedStreamState> state)
            : _state(std::move(state)) {}

    Status new_iterator(doris::segment_v2::ColumnIteratorUPtr* iterator, const doris::TabletColumn*,
                        const doris::StorageReadOptions*) override {
        *iterator = std::make_unique<ThreeRowNullableInt32Iterator>(_state);
        return Status::OK();
    }

private:
    std::shared_ptr<OwnedTypedStreamState> _state;
};

class SingleTypedColumnReaderCache final : public doris::segment_v2::ColumnReaderCache {
public:
    explicit SingleTypedColumnReaderCache(std::shared_ptr<OwnedTypedStreamState> state)
            : ColumnReaderCache(nullptr, {}, {}, 3,
                                [](std::shared_ptr<doris::segment_v2::SegmentFooterPB>&,
                                   OlapReaderStatistics*, const doris::io::IOContext*) {
                                    return Status::InternalError(
                                            "synthetic typed cache has no segment footer");
                                }),
              _reader(std::make_shared<SingleTypedColumnReader>(std::move(state))) {}

    Status get_path_column_reader(int32_t, PathInData relative_path,
                                  std::shared_ptr<doris::segment_v2::ColumnReader>* column_reader,
                                  OlapReaderStatistics*,
                                  const doris::segment_v2::SubcolumnColumnMetaInfo::Node*,
                                  const doris::io::IOContext*) override {
        if (relative_path != PathInData("a")) {
            return Status::InvalidArgument("unexpected synthetic typed path {}",
                                           relative_path.get_path());
        }
        *column_reader = _reader;
        return Status::OK();
    }

private:
    std::shared_ptr<doris::segment_v2::ColumnReader> _reader;
};

static std::string bounded_doc_key(size_t index) {
    constexpr size_t WIDTH = 10;
    std::string suffix = std::to_string(index);
    DORIS_CHECK_LE(suffix.size(), WIDTH);
    std::string key = "k";
    key.append(WIDTH - suffix.size(), '0');
    key.append(suffix);
    return key;
}

class UniqueKeyDocIterator final : public ColumnIterator {
public:
    explicit UniqueKeyDocIterator(size_t total_rows) : _total_rows(total_rows) {}

    Status init(const ColumnIteratorOptions&) override { return Status::OK(); }
    Status seek_to_ordinal(ordinal_t ordinal) override {
        if (ordinal > _total_rows) {
            return Status::InvalidArgument("unique-key DOC ordinal {} exceeds row count {}",
                                           ordinal, _total_rows);
        }
        _current_ordinal = ordinal;
        return Status::OK();
    }
    ordinal_t get_current_ordinal() const override { return _current_ordinal; }

    Status next_batch(size_t* rows, MutableColumnPtr& dst, bool* has_null) override {
        auto* map = check_and_get_column<ColumnMap>(dst.get());
        if (map == nullptr) {
            return Status::InvalidArgument("unique-key DOC destination is not a map");
        }
        if (has_null != nullptr) {
            *has_null = false;
        }
        DCHECK_LE(_current_ordinal, _total_rows);
        const size_t produced = std::min(*rows, _total_rows - _current_ordinal);
        auto& keys = assert_cast<ColumnString&>(map->get_keys());
        auto& values = assert_cast<ColumnString&>(map->get_values());
        auto& offsets = map->get_offsets();
        auto jsonb_type = std::make_shared<doris::DataTypeJsonb>();
        auto source = jsonb_type->create_column();
        auto serde = jsonb_type->get_serde();
        doris::DataTypeSerDe::FormatOptions options;
        for (size_t row = 0; row < produced; ++row) {
            const std::string value = std::to_string(_current_ordinal + row);
            doris::Slice json(value.data(), value.size());
            RETURN_IF_ERROR(serde->deserialize_one_cell_from_json(*source, json, options));
        }

        auto& value_chars = values.get_chars();
        for (size_t row = 0; row < produced; ++row) {
            const std::string key = bounded_doc_key(_current_ordinal + row);
            keys.insert_data(key.data(), key.size());
            serde->write_one_cell_to_binary(*source, value_chars, row);
            values.get_offsets().push_back(value_chars.size());
            offsets.push_back(keys.size());
        }
        *rows = produced;
        _current_ordinal += produced;
        return Status::OK();
    }

    Status read_by_rowids(const doris::segment_v2::rowid_t*, const size_t,
                          MutableColumnPtr&) override {
        return Status::NotSupported("UniqueKeyDocIterator only supports sequential reads");
    }

private:
    ordinal_t _current_ordinal = 0;
    size_t _total_rows;
};

class RejectingPathColumnReaderCache final : public doris::segment_v2::ColumnReaderCache {
public:
    explicit RejectingPathColumnReaderCache(size_t rows)
            : ColumnReaderCache(
                      nullptr, {}, {}, rows,
                      [](std::shared_ptr<doris::segment_v2::SegmentFooterPB>&,
                         OlapReaderStatistics*, const doris::io::IOContext*) {
                          return Status::InternalError("synthetic DOC cache has no segment footer");
                      }) {}

    Status get_path_column_reader(int32_t, PathInData relative_path,
                                  std::shared_ptr<doris::segment_v2::ColumnReader>*,
                                  OlapReaderStatistics*,
                                  const doris::segment_v2::SubcolumnColumnMetaInfo::Node*,
                                  const doris::io::IOContext*) override {
        ++path_reader_calls;
        return Status::InternalError("DOC layout hint opened leaf reader '{}'",
                                     relative_path.get_path());
    }

    size_t path_reader_calls = 0;
};

static std::unique_ptr<SubstreamIterator> make_dummy_sparse_stream() {
    return std::make_unique<SubstreamIterator>(doris::ColumnVariant::create_binary_column_fn(),
                                               std::make_unique<DummySparseIterator>(), nullptr);
}

static std::unique_ptr<SubstreamIterator> make_empty_sparse_stream(
        size_t total_rows = std::numeric_limits<size_t>::max(), bool misreport_next_batch = false) {
    return std::make_unique<SubstreamIterator>(
            doris::ColumnVariant::create_binary_column_fn(),
            std::make_unique<EmptySparseIterator>(total_rows, misreport_next_batch), nullptr);
}

static std::unique_ptr<SubstreamIterator> make_dummy_root_stream() {
    return std::make_unique<SubstreamIterator>(ColumnString::create(),
                                               std::make_unique<DummySparseIterator>(), nullptr);
}

static std::unique_ptr<SubstreamIterator> make_three_row_root_stream(
        const std::shared_ptr<ConsecutiveBatchState>& state, bool reported_has_null = false,
        std::string_view json = "{}") {
    auto type = std::make_shared<doris::DataTypeJsonb>();
    return std::make_unique<SubstreamIterator>(
            type->create_column(),
            std::make_unique<ThreeRowJsonbIterator>(state, reported_has_null, json), type);
}

static std::unique_ptr<SubstreamIterator> make_two_row_nullable_root_stream() {
    auto type = doris::make_nullable(std::make_shared<doris::DataTypeJsonb>());
    return std::make_unique<SubstreamIterator>(
            type->create_column(), std::make_unique<TwoRowNullableJsonbIterator>(), type);
}

static std::unique_ptr<SubstreamIterator> make_three_row_sparse_stream(
        const std::shared_ptr<ConsecutiveBatchState>& state) {
    return std::make_unique<SubstreamIterator>(doris::ColumnVariant::create_binary_column_fn(),
                                               std::make_unique<ThreeRowSparseIterator>(state),
                                               nullptr);
}

static std::unique_ptr<SubstreamIterator> make_two_row_jsonb_sparse_stream(
        std::string_view path = "s") {
    return std::make_unique<SubstreamIterator>(
            doris::ColumnVariant::create_binary_column_fn(),
            std::make_unique<TwoRowJsonbSparseIterator>(std::string(path)), nullptr);
}

struct VariantJsonWriter {
    void write(const char* data, size_t size) { value.append(data, size); }

    std::string value;
};

static std::string variant_v2_json_at(const doris::ColumnVariantV2& column, size_t row) {
    VariantJsonWriter writer;
    if (column.is_encoded()) {
        doris::to_json(column.get_value_ref(row), writer);
    } else {
        auto encoded = column.materialize_encoded_range(row, 1);
        doris::to_json(encoded->get_value_ref(0), writer);
    }
    return writer.value;
}

static std::string variant_v1_json_at(const ColumnVariant& column, size_t row) {
    doris::DataTypeSerDe::FormatOptions options;
    std::string value;
    column.serialize_one_row_to_string(row, &value, options);
    return value;
}

TEST(HierarchicalDataIteratorTest, RejectsDestinationThatDoesNotMatchConfiguredRoute) {
    doris::segment_v2::ColumnIteratorUPtr v2_reader;
    OlapReaderStatistics stats;
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &v2_reader, 0, PathInData("a.b"), nullptr, make_dummy_sparse_stream(),
                        nullptr, nullptr, &stats,
                        HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE,
                        /*use_variant_v2=*/true)
                        .ok());

    ColumnIteratorOptions options;
    options.stats = &stats;
    ASSERT_TRUE(v2_reader->init(options).ok());
    ASSERT_TRUE(v2_reader->seek_to_ordinal(0).ok());

    MutableColumnPtr v1_destination = ColumnVariant::create(3, false);
    size_t rows = 2;
    const Status v2_to_v1 = v2_reader->next_batch(&rows, v1_destination);
    ASSERT_TRUE(v2_to_v1.is<doris::ErrorCode::INVALID_ARGUMENT>()) << v2_to_v1;
    EXPECT_EQ(v2_reader->get_current_ordinal(), 0);

    doris::segment_v2::ColumnIteratorUPtr v1_reader;
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &v1_reader, 0, PathInData("a.b"), nullptr, make_dummy_sparse_stream(),
                        nullptr, nullptr, &stats,
                        HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE,
                        /*use_variant_v2=*/false)
                        .ok());
    ASSERT_TRUE(v1_reader->init(options).ok());
    ASSERT_TRUE(v1_reader->seek_to_ordinal(0).ok());

    MutableColumnPtr v2_destination = doris::ColumnVariantV2::create();
    rows = 2;
    const Status v1_to_v2 = v1_reader->next_batch(&rows, v2_destination);
    EXPECT_TRUE(v1_to_v2.is<doris::ErrorCode::INVALID_ARGUMENT>()) << v1_to_v2;
    EXPECT_EQ(v1_reader->get_current_ordinal(), 0);
}

TEST(HierarchicalDataIteratorTest, NextBatchUsesActualShortFinalBatchSize) {
    doris::segment_v2::ColumnIteratorUPtr iterator;
    OlapReaderStatistics stats;
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &iterator, 0, PathInData("a.b"), nullptr, make_dummy_sparse_stream(),
                        nullptr, nullptr, &stats,
                        HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE, true)
                        .ok());

    ColumnIteratorOptions options;
    options.stats = &stats;
    ASSERT_TRUE(iterator->init(options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

    MutableColumnPtr dst = doris::ColumnVariantV2::create();
    size_t rows = 8;
    ASSERT_TRUE(iterator->next_batch(&rows, dst).ok());
    EXPECT_EQ(rows, 2);
    EXPECT_EQ(dst->size(), 2);
}

TEST(HierarchicalDataIteratorTest, RejectsReportedRowsThatDoNotMatchProducedColumn) {
    doris::segment_v2::ColumnIteratorUPtr iterator;
    OlapReaderStatistics stats;
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &iterator, 0, PathInData("a"), nullptr,
                        make_empty_sparse_stream(/*total_rows=*/1,
                                                 /*misreport_next_batch=*/true),
                        nullptr, nullptr, &stats,
                        HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE, true)
                        .ok());

    ColumnIteratorOptions options;
    options.stats = &stats;
    ASSERT_TRUE(iterator->init(options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

    MutableColumnPtr dst = doris::ColumnVariantV2::create();
    size_t rows = 2;
    const Status status = iterator->next_batch(&rows, dst);
    EXPECT_TRUE(status.is<doris::ErrorCode::CORRUPTION>()) << status;
    EXPECT_NE(status.to_string().find("reported 2 rows but produced 1"), std::string::npos);
    EXPECT_EQ(dst->size(), 0);
}

TEST(HierarchicalDataIteratorTest, RejectsCrossStreamRowCountMismatch) {
    doris::segment_v2::ColumnIteratorUPtr iterator;
    OlapReaderStatistics stats;
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &iterator, 0, PathInData(), nullptr,
                        make_empty_sparse_stream(/*total_rows=*/1),
                        make_empty_sparse_stream(/*total_rows=*/2), nullptr, &stats,
                        HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE, true)
                        .ok());

    ColumnIteratorOptions options;
    options.stats = &stats;
    ASSERT_TRUE(iterator->init(options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

    MutableColumnPtr dst = doris::ColumnVariantV2::create();
    size_t rows = 2;
    const Status status = iterator->next_batch(&rows, dst);
    EXPECT_TRUE(status.is<doris::ErrorCode::CORRUPTION>()) << status;
    EXPECT_NE(status.to_string().find("previous streams returned 2"), std::string::npos);
    EXPECT_EQ(dst->size(), 0);
}

TEST(HierarchicalDataIteratorTest, ReadByRowidsRejectsShortStream) {
    doris::segment_v2::ColumnIteratorUPtr iterator;
    OlapReaderStatistics stats;
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &iterator, 0, PathInData("a"), nullptr,
                        make_empty_sparse_stream(/*total_rows=*/1), nullptr, nullptr, &stats,
                        HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE, true)
                        .ok());

    ColumnIteratorOptions options;
    options.stats = &stats;
    ASSERT_TRUE(iterator->init(options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

    const doris::segment_v2::rowid_t rowids[] = {0, 1};
    MutableColumnPtr dst = doris::ColumnVariantV2::create();
    const Status status = iterator->read_by_rowids(rowids, std::size(rowids), dst);
    EXPECT_TRUE(status.is<doris::ErrorCode::CORRUPTION>()) << status;
    EXPECT_NE(status.to_string().find("returned 1 rows, expected 2"), std::string::npos);
    EXPECT_EQ(dst->size(), 0);
}

TEST(HierarchicalDataIteratorTest, RejectsReadsWithoutPhysicalStreams) {
    doris::segment_v2::ColumnIteratorUPtr iterator;
    OlapReaderStatistics stats;
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &iterator, 0, PathInData(), nullptr, nullptr, nullptr, nullptr, &stats,
                        HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE, true)
                        .ok());

    ColumnIteratorOptions options;
    options.stats = &stats;
    ASSERT_TRUE(iterator->init(options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

    MutableColumnPtr batch_dst = doris::ColumnVariantV2::create();
    size_t rows = 1;
    const Status batch_status = iterator->next_batch(&rows, batch_dst);
    EXPECT_TRUE(batch_status.is<doris::ErrorCode::INTERNAL_ERROR>()) << batch_status;
    EXPECT_NE(batch_status.to_string().find("no physical streams"), std::string::npos);

    const doris::segment_v2::rowid_t rowid = 0;
    MutableColumnPtr rowid_dst = doris::ColumnVariantV2::create();
    const Status rowid_status = iterator->read_by_rowids(&rowid, 1, rowid_dst);
    EXPECT_TRUE(rowid_status.is<doris::ErrorCode::INTERNAL_ERROR>()) << rowid_status;
    EXPECT_NE(rowid_status.to_string().find("no physical streams"), std::string::npos);
}

TEST(HierarchicalDataIteratorTest, TwoPhysicalStreamsAppendTwoPlusOneBatchesForV1AndV2) {
    constexpr std::array<std::string_view, 3> EXPECTED {
            R"({"s":"row0"})",
            R"({"s":"row1"})",
            R"({"s":"row2"})",
    };

    for (const bool use_variant_v2 : {false, true}) {
        SCOPED_TRACE(use_variant_v2 ? "V2" : "V1");
        auto root_state = std::make_shared<ConsecutiveBatchState>();
        auto sparse_state = std::make_shared<ConsecutiveBatchState>();
        doris::segment_v2::ColumnIteratorUPtr iterator;
        OlapReaderStatistics stats;
        ASSERT_TRUE(HierarchicalDataIterator::create(
                            &iterator, 0, PathInData(), nullptr,
                            make_three_row_sparse_stream(sparse_state),
                            make_three_row_root_stream(root_state), nullptr, &stats,
                            HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE,
                            use_variant_v2)
                            .ok());

        ColumnIteratorOptions options;
        options.stats = &stats;
        ASSERT_TRUE(iterator->init(options).ok());
        ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

        MutableColumnPtr dst = use_variant_v2 ? MutableColumnPtr(doris::ColumnVariantV2::create())
                                              : MutableColumnPtr(ColumnVariant::create(3, false));
        size_t rows = 2;
        ASSERT_TRUE(iterator->next_batch(&rows, dst).ok());
        ASSERT_EQ(rows, 2);
        ASSERT_EQ(dst->size(), 2);
        EXPECT_EQ(iterator->get_current_ordinal(), 2);

        rows = 2;
        ASSERT_TRUE(iterator->next_batch(&rows, dst).ok());
        ASSERT_EQ(rows, 1);
        ASSERT_EQ(dst->size(), 3);
        EXPECT_EQ(iterator->get_current_ordinal(), 3);

        EXPECT_TRUE(root_state->scratch_was_always_empty);
        EXPECT_TRUE(sparse_state->scratch_was_always_empty);
        EXPECT_EQ(root_state->next_batch_calls, 2);
        EXPECT_EQ(sparse_state->next_batch_calls, 2);
        EXPECT_EQ(root_state->current_ordinal, 3);
        EXPECT_EQ(sparse_state->current_ordinal, 3);
        EXPECT_EQ(root_state->produced_batches, (std::vector<size_t> {2, 1}));
        EXPECT_EQ(sparse_state->produced_batches, (std::vector<size_t> {2, 1}));

        if (use_variant_v2) {
            const auto& variant = assert_cast<const doris::ColumnVariantV2&>(*dst);
            for (size_t row = 0; row < EXPECTED.size(); ++row) {
                EXPECT_EQ(variant_v2_json_at(variant, row), EXPECTED[row]);
            }
        } else {
            const auto& variant = assert_cast<const ColumnVariant&>(*dst);
            for (size_t row = 0; row < EXPECTED.size(); ++row) {
                EXPECT_EQ(variant_v1_json_at(variant, row), EXPECTED[row]);
            }
        }
    }
}

TEST(HierarchicalDataIteratorTest, VariantV2TransfersPhysicalTypedStreamIntoShreddedChild) {
    auto int_type = doris::make_nullable(std::make_shared<doris::DataTypeInt32>());
    doris::segment_v2::SubcolumnColumnMetaInfo metadata;
    ASSERT_TRUE(metadata.add(PathInData("a"),
                             doris::segment_v2::SubcolumnMeta {.file_column_type = int_type}));

    auto typed_state = std::make_shared<OwnedTypedStreamState>();
    SingleTypedColumnReaderCache cache(typed_state);
    doris::segment_v2::ColumnIteratorUPtr iterator;
    OlapReaderStatistics stats;
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &iterator, 0, PathInData(), metadata.get_root(),
                        make_empty_sparse_stream(3), nullptr, &cache, &stats,
                        HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE,
                        /*use_variant_v2=*/true)
                        .ok());

    ColumnIteratorOptions options;
    options.stats = &stats;
    ASSERT_TRUE(iterator->init(options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

    MutableColumnPtr dst = doris::ColumnVariantV2::create();
    size_t rows = 3;
    ASSERT_TRUE(iterator->next_batch(&rows, dst).ok());
    ASSERT_EQ(rows, 3);
    ASSERT_EQ(dst->size(), 3);

    const auto& variant = assert_cast<const doris::ColumnVariantV2&>(*dst);
    ASSERT_TRUE(variant.is_shredded());
    ASSERT_EQ(variant.shredded_field_count(), 1);
    EXPECT_EQ(variant.shredded_field_path(0).get_path(), "a");
    EXPECT_EQ(variant.shredded_field_presence(0).get_data(),
              (doris::PaddedPODArray<doris::UInt8> {1, 0, 1}));
    const auto& child = variant.shredded_field_values(0);
    ASSERT_TRUE(child.is_typed());
    ASSERT_NE(typed_state->wrapper, nullptr);
    ASSERT_NE(typed_state->payload, nullptr);
    EXPECT_EQ(&child.typed_column(), typed_state->wrapper);
    const auto& nullable = assert_cast<const doris::ColumnNullable&>(child.typed_column());
    EXPECT_EQ(&nullable.get_nested_column(), typed_state->payload);
    EXPECT_EQ(nullable.get_null_map_data(), (doris::NullMap {0, 1, 0}));
    EXPECT_EQ(variant_v2_json_at(variant, 0), R"({"a":10})");
    EXPECT_EQ(variant_v2_json_at(variant, 1), "{}");
    EXPECT_EQ(variant_v2_json_at(variant, 2), R"({"a":30})");
    EXPECT_EQ(doris::ColumnVariantV2::TestAccess::full_shredded_validations(variant), 0);
    EXPECT_EQ(stats.variant_v2_shredded_output_rows, 3);
}

TEST(HierarchicalDataIteratorTest, VariantV2DocLayoutHintsStayBlockLocalAndBounded) {
    const size_t layout_limit = doris::segment_v2::variant_v2::VariantAssembler::TestAccess::
            max_shredded_execution_layout_paths();
    ASSERT_GT(layout_limit, 0);
    const size_t rows = layout_limit + 1;
    auto int_type = doris::make_nullable(std::make_shared<doris::DataTypeInt32>());
    doris::segment_v2::SubcolumnColumnMetaInfo metadata;
    for (size_t row = 0; row < rows; ++row) {
        ASSERT_TRUE(metadata.add(PathInData(bounded_doc_key(row)),
                                 doris::segment_v2::SubcolumnMeta {.file_column_type = int_type}));
    }

    auto doc_stream = std::make_unique<SubstreamIterator>(
            doris::ColumnVariant::create_binary_column_fn(),
            std::make_unique<UniqueKeyDocIterator>(rows), nullptr);
    RejectingPathColumnReaderCache cache(rows);
    doris::segment_v2::ColumnIteratorUPtr iterator;
    OlapReaderStatistics stats;
    ASSERT_TRUE(
            HierarchicalDataIterator::create(&iterator, 0, PathInData(), metadata.get_root(),
                                             std::move(doc_stream), nullptr, &cache, &stats,
                                             HierarchicalDataIterator::ReadType::DOC_VALUE_COLUMN,
                                             /*use_variant_v2=*/true)
                    .ok());
    EXPECT_EQ(cache.path_reader_calls, 0);

    ColumnIteratorOptions options;
    options.stats = &stats;
    ASSERT_TRUE(iterator->init(options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

    MutableColumnPtr dst = doris::ColumnVariantV2::create();
    size_t read_rows = rows;
    ASSERT_TRUE(iterator->next_batch(&read_rows, dst).ok());
    ASSERT_EQ(read_rows, rows);
    EXPECT_EQ(cache.path_reader_calls, 0);

    const auto& variant = assert_cast<const doris::ColumnVariantV2&>(*dst);
    ASSERT_TRUE(variant.is_shredded());
    ASSERT_EQ(variant.shredded_field_count(), layout_limit);
    EXPECT_EQ(variant.shredded_field_path(0), PathInData(bounded_doc_key(0)));
    EXPECT_EQ(variant.shredded_field_path(layout_limit - 1),
              PathInData(bounded_doc_key(layout_limit - 1)));
    EXPECT_EQ(variant_v2_json_at(variant, 0), "{\"" + bounded_doc_key(0) + "\":0}");
    EXPECT_EQ(variant_v2_json_at(variant, layout_limit),
              "{\"" + bounded_doc_key(layout_limit) + "\":" + std::to_string(layout_limit) + "}");
    VariantJsonWriter residual_writer;
    doris::to_json(variant.read_view().residual_value_at(layout_limit), residual_writer);
    EXPECT_EQ(residual_writer.value,
              "{\"" + bounded_doc_key(layout_limit) + "\":" + std::to_string(layout_limit) + "}");
    EXPECT_EQ(doris::ColumnVariantV2::TestAccess::full_shredded_validations(variant), 0);
    EXPECT_EQ(stats.variant_v2_shredded_output_rows, static_cast<int64_t>(rows));
}

TEST(HierarchicalDataIteratorTest, CurrentOrdinalFallsBackToSparseStream) {
    doris::segment_v2::ColumnIteratorUPtr iterator;
    OlapReaderStatistics stats;
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &iterator, 0, PathInData("a.b"), nullptr, make_dummy_sparse_stream(),
                        nullptr, nullptr, &stats,
                        HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE)
                        .ok());

    ColumnIteratorOptions options;
    options.stats = &stats;
    ASSERT_TRUE(iterator->init(options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(17).ok());
    EXPECT_EQ(iterator->get_current_ordinal(), 17);
}

TEST(HierarchicalDataIteratorTest, InitSeekAndCurrentOrdinalSupportRootOnlyStream) {
    doris::segment_v2::ColumnIteratorUPtr iterator;
    OlapReaderStatistics stats;
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &iterator, 0, PathInData(), nullptr, nullptr, make_dummy_root_stream(),
                        nullptr, &stats, HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE)
                        .ok());

    ColumnIteratorOptions options;
    options.stats = &stats;
    ASSERT_TRUE(iterator->init(options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(23).ok());
    EXPECT_EQ(iterator->get_current_ordinal(), 23);
}

TEST(HierarchicalDataIteratorTest, MissingSubtreeRowsDoNotBecomeEmptyObjects) {
    for (const bool use_v2 : {false, true}) {
        doris::segment_v2::ColumnIteratorUPtr iterator;
        OlapReaderStatistics stats;
        ASSERT_TRUE(HierarchicalDataIterator::create(
                            &iterator, 0, PathInData("unrecorded"), nullptr,
                            make_empty_sparse_stream(), nullptr, nullptr, &stats,
                            HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE, use_v2)
                            .ok());

        ColumnIteratorOptions options;
        options.stats = &stats;
        ASSERT_TRUE(iterator->init(options).ok());
        ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

        MutableColumnPtr values = use_v2 ? MutableColumnPtr(doris::ColumnVariantV2::create())
                                         : MutableColumnPtr(doris::ColumnVariant::create(0, false));
        MutableColumnPtr dst =
                doris::ColumnNullable::create(std::move(values), doris::ColumnUInt8::create());
        size_t rows = 2;
        bool has_null = false;
        ASSERT_TRUE(iterator->next_batch(&rows, dst, &has_null).ok());
        ASSERT_EQ(rows, 2);
        EXPECT_TRUE(has_null);
        const auto& nullable = assert_cast<const doris::ColumnNullable&>(*dst);
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_TRUE(nullable.is_null_at(row)) << "use_v2=" << use_v2 << ", row=" << row;
        }
    }
}

TEST(HierarchicalDataIteratorTest, ExactSparseJsonNullUsesVersionNativeSemantics) {
    for (const bool use_variant_v2 : {false, true}) {
        SCOPED_TRACE(use_variant_v2 ? "V2" : "V1");
        doris::segment_v2::ColumnIteratorUPtr iterator;
        OlapReaderStatistics stats;
        ASSERT_TRUE(HierarchicalDataIterator::create(
                            &iterator, 0, PathInData("s"), nullptr,
                            make_two_row_jsonb_sparse_stream(), nullptr, nullptr, &stats,
                            HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE,
                            use_variant_v2)
                            .ok());

        ColumnIteratorOptions options;
        options.stats = &stats;
        ASSERT_TRUE(iterator->init(options).ok());
        ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

        MutableColumnPtr values = use_variant_v2
                                          ? MutableColumnPtr(doris::ColumnVariantV2::create())
                                          : MutableColumnPtr(ColumnVariant::create(0, false));
        MutableColumnPtr dst =
                doris::ColumnNullable::create(std::move(values), doris::ColumnUInt8::create());
        size_t rows = 2;
        bool has_null = false;
        ASSERT_TRUE(iterator->next_batch(&rows, dst, &has_null).ok());
        ASSERT_EQ(rows, 2);
        EXPECT_EQ(has_null, !use_variant_v2);
        const auto& nullable = assert_cast<const doris::ColumnNullable&>(*dst);
        EXPECT_EQ(nullable.is_null_at(0), !use_variant_v2);
        EXPECT_FALSE(nullable.is_null_at(1));
        if (use_variant_v2) {
            const auto& variant =
                    assert_cast<const doris::ColumnVariantV2&>(nullable.get_nested_column());
            EXPECT_EQ(variant_v2_json_at(variant, 0), "null");
        }
    }
}

TEST(HierarchicalDataIteratorTest, ExactDocJsonNullUsesVersionNativeSemantics) {
    for (const bool use_variant_v2 : {false, true}) {
        SCOPED_TRACE(use_variant_v2 ? "V2" : "V1");
        doris::segment_v2::ColumnIteratorUPtr iterator;
        OlapReaderStatistics stats;
        ASSERT_TRUE(HierarchicalDataIterator::create(
                            &iterator, 0, PathInData("s"), nullptr,
                            make_two_row_jsonb_sparse_stream(), nullptr, nullptr, &stats,
                            HierarchicalDataIterator::ReadType::DOC_VALUE_COLUMN, use_variant_v2)
                            .ok());

        ColumnIteratorOptions options;
        options.stats = &stats;
        ASSERT_TRUE(iterator->init(options).ok());
        ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

        MutableColumnPtr values = use_variant_v2
                                          ? MutableColumnPtr(doris::ColumnVariantV2::create())
                                          : MutableColumnPtr(ColumnVariant::create(0, true));
        MutableColumnPtr dst =
                doris::ColumnNullable::create(std::move(values), doris::ColumnUInt8::create());
        size_t rows = 2;
        bool has_null = false;
        ASSERT_TRUE(iterator->next_batch(&rows, dst, &has_null).ok());
        ASSERT_EQ(rows, 2);
        EXPECT_EQ(has_null, !use_variant_v2);
        const auto& nullable = assert_cast<const doris::ColumnNullable&>(*dst);
        EXPECT_EQ(nullable.is_null_at(0), !use_variant_v2);
        EXPECT_FALSE(nullable.is_null_at(1));
        if (use_variant_v2) {
            const auto& variant =
                    assert_cast<const doris::ColumnVariantV2&>(nullable.get_nested_column());
            EXPECT_EQ(variant_v2_json_at(variant, 0), "null");
        }
    }
}

TEST(HierarchicalDataIteratorTest, DescendantJsonNullKeepsObjectVisibleForV1AndV2) {
    constexpr std::array<std::string_view, 2> EXPECTED {R"({"child":null})", R"({"child":1})"};
    for (const bool use_variant_v2 : {false, true}) {
        SCOPED_TRACE(use_variant_v2 ? "V2" : "V1");
        doris::segment_v2::ColumnIteratorUPtr iterator;
        OlapReaderStatistics stats;
        ASSERT_TRUE(HierarchicalDataIterator::create(
                            &iterator, 0, PathInData("s"), nullptr,
                            make_two_row_jsonb_sparse_stream("s.child"), nullptr, nullptr, &stats,
                            HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE,
                            use_variant_v2)
                            .ok());

        ColumnIteratorOptions options;
        options.stats = &stats;
        ASSERT_TRUE(iterator->init(options).ok());
        ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

        MutableColumnPtr values = use_variant_v2
                                          ? MutableColumnPtr(doris::ColumnVariantV2::create())
                                          : MutableColumnPtr(ColumnVariant::create(0, false));
        MutableColumnPtr dst =
                doris::ColumnNullable::create(std::move(values), doris::ColumnUInt8::create());
        size_t rows = 2;
        bool has_null = true;
        ASSERT_TRUE(iterator->next_batch(&rows, dst, &has_null).ok());
        ASSERT_EQ(rows, 2);
        EXPECT_FALSE(has_null);
        const auto& nullable = assert_cast<const doris::ColumnNullable&>(*dst);
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_FALSE(nullable.is_null_at(row));
            if (use_variant_v2) {
                const auto& variant =
                        assert_cast<const doris::ColumnVariantV2&>(nullable.get_nested_column());
                EXPECT_EQ(variant_v2_json_at(variant, row), EXPECTED[row]);
            } else {
                const auto& variant =
                        assert_cast<const ColumnVariant&>(nullable.get_nested_column());
                EXPECT_EQ(variant_v1_json_at(variant, row), EXPECTED[row]);
            }
        }
    }
}

TEST(HierarchicalDataIteratorTest, HasNullComesFromAssembledRowsForV1AndV2) {
    for (const bool use_variant_v2 : {false, true}) {
        SCOPED_TRACE(use_variant_v2 ? "V2" : "V1");
        doris::segment_v2::ColumnIteratorUPtr iterator;
        OlapReaderStatistics stats;
        ASSERT_TRUE(HierarchicalDataIterator::create(
                            &iterator, 0, PathInData(), nullptr,
                            make_empty_sparse_stream(/*total_rows=*/2),
                            make_two_row_nullable_root_stream(), nullptr, &stats,
                            HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE,
                            use_variant_v2)
                            .ok());

        ColumnIteratorOptions options;
        options.stats = &stats;
        ASSERT_TRUE(iterator->init(options).ok());
        ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

        MutableColumnPtr values = use_variant_v2
                                          ? MutableColumnPtr(doris::ColumnVariantV2::create())
                                          : MutableColumnPtr(ColumnVariant::create(2, false));
        MutableColumnPtr dst =
                doris::ColumnNullable::create(std::move(values), doris::ColumnUInt8::create());
        size_t rows = 2;
        bool has_null = false;
        ASSERT_TRUE(iterator->next_batch(&rows, dst, &has_null).ok());
        ASSERT_EQ(rows, 2);
        EXPECT_TRUE(has_null);
        const auto& nullable = assert_cast<const doris::ColumnNullable&>(*dst);
        EXPECT_TRUE(nullable.is_null_at(0));
        EXPECT_FALSE(nullable.is_null_at(1));
    }
}

TEST(HierarchicalDataIteratorTest, HasNullReflectsAssembledOuterNullsForV1AndV2) {
    for (const bool use_variant_v2 : {false, true}) {
        SCOPED_TRACE(use_variant_v2 ? "V2" : "V1");
        auto root_state = std::make_shared<ConsecutiveBatchState>();
        doris::segment_v2::ColumnIteratorUPtr iterator;
        OlapReaderStatistics stats;
        ASSERT_TRUE(HierarchicalDataIterator::create(
                            &iterator, 0, PathInData(), nullptr,
                            make_empty_sparse_stream(/*total_rows=*/3),
                            make_three_row_root_stream(root_state, /*reported_has_null=*/true),
                            nullptr, &stats,
                            HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE,
                            use_variant_v2)
                            .ok());

        ColumnIteratorOptions options;
        options.stats = &stats;
        ASSERT_TRUE(iterator->init(options).ok());
        ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

        MutableColumnPtr values = use_variant_v2
                                          ? MutableColumnPtr(doris::ColumnVariantV2::create())
                                          : MutableColumnPtr(ColumnVariant::create(3, false));
        MutableColumnPtr dst =
                doris::ColumnNullable::create(std::move(values), doris::ColumnUInt8::create());
        size_t rows = 3;
        bool has_null = true;
        ASSERT_TRUE(iterator->next_batch(&rows, dst, &has_null).ok());
        ASSERT_EQ(rows, 3);
        EXPECT_FALSE(has_null);
        const auto& nullable = assert_cast<const doris::ColumnNullable&>(*dst);
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_FALSE(nullable.is_null_at(row));
        }
    }
}

TEST(HierarchicalDataIteratorTest, RootJsonNullUsesVersionNativeSemantics) {
    for (const bool use_variant_v2 : {false, true}) {
        SCOPED_TRACE(use_variant_v2 ? "V2" : "V1");
        auto root_state = std::make_shared<ConsecutiveBatchState>();
        doris::segment_v2::ColumnIteratorUPtr iterator;
        OlapReaderStatistics stats;
        ASSERT_TRUE(HierarchicalDataIterator::create(
                            &iterator, 0, PathInData(), nullptr,
                            make_empty_sparse_stream(/*total_rows=*/3),
                            make_three_row_root_stream(root_state, /*reported_has_null=*/false,
                                                       /*json=*/"null"),
                            nullptr, &stats,
                            HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE,
                            use_variant_v2)
                            .ok());

        ColumnIteratorOptions options;
        options.stats = &stats;
        ASSERT_TRUE(iterator->init(options).ok());
        ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

        MutableColumnPtr values = use_variant_v2
                                          ? MutableColumnPtr(doris::ColumnVariantV2::create())
                                          : MutableColumnPtr(ColumnVariant::create(3, false));
        MutableColumnPtr dst =
                doris::ColumnNullable::create(std::move(values), doris::ColumnUInt8::create());
        size_t rows = 3;
        bool has_null = false;
        ASSERT_TRUE(iterator->next_batch(&rows, dst, &has_null).ok());
        ASSERT_EQ(rows, 3);
        EXPECT_EQ(has_null, !use_variant_v2);
        const auto& nullable = assert_cast<const doris::ColumnNullable&>(*dst);
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_EQ(nullable.is_null_at(row), !use_variant_v2);
            if (use_variant_v2) {
                const auto& variant =
                        assert_cast<const doris::ColumnVariantV2&>(nullable.get_nested_column());
                EXPECT_EQ(variant_v2_json_at(variant, row), "null");
            }
        }
    }
}

TEST(HierarchicalDataIteratorTest, ProcessSparseExtractSubpaths) {
    std::unique_ptr<ColumnIterator> sparse_reader = std::make_unique<DummySparseIterator>();
    doris::segment_v2::ColumnIteratorUPtr iter;
    auto sparse_iter = std::make_unique<SubstreamIterator>(
            doris::ColumnVariant::create_binary_column_fn(), std::move(sparse_reader), nullptr);
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &iter, /*col_uid*/ 0, PathInData("a.b"), /*node*/ nullptr,
                        /*root*/ std::move(sparse_iter), nullptr, nullptr, nullptr,
                        HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE)
                        .ok());

    ColumnIteratorOptions opts;
    ASSERT_TRUE(iter->init(opts).ok());
    ASSERT_TRUE(iter->seek_to_ordinal(0).ok());

    auto* hiter = static_cast<HierarchicalDataIterator*>(iter.get());
    auto& map = assert_cast<ColumnMap&>(*hiter->_binary_column_reader->column);
    auto& keys = assert_cast<ColumnString&>(map.get_keys());
    auto& vals = assert_cast<ColumnString&>(map.get_values());
    auto& offs = map.get_offsets();

    doris::DataTypePtr str_type = std::make_shared<doris::DataTypeString>();
    auto str_col = str_type->create_column();
    auto serde = str_type->get_serde();
    str_col->insert_data("abcvalues", strlen("abcvalues"));
    str_col->insert_data("abdvalues", strlen("abdvalues"));
    str_col->insert_data("abcvalues", strlen("abcvalues"));
    str_col->insert_data("abevalues", strlen("abevalues"));
    str_col->insert_data("axvalues", strlen("axvalues"));
    ColumnString::Chars& chars = vals.get_chars();
    for (size_t i = 0; i < 5; ++i) {
        serde->write_one_cell_to_binary(*str_col, chars, i);
        vals.get_offsets().push_back(chars.size());
    }

    // row0: {"a.b.c": "abcvalues", "a.b.d": "abdvalues"}
    keys.insert_data("a.b.c", strlen("a.b.c"));
    keys.insert_data("a.b.d", strlen("a.b.d"));
    offs.push_back(keys.size());

    // row1: {"a.b.c": "abcvalues", "a.b.e": "abevalues", "a.x": "axvalues"}
    keys.insert_data("a.b.c", strlen("a.b.c"));
    keys.insert_data("a.b.e", strlen("a.b.e"));
    keys.insert_data("a.x", strlen("a.x"));
    offs.push_back(keys.size());

    const size_t nrows = 2;
    MutableColumnPtr dst = ColumnVariant::create(/*max_subcolumns_count*/ 2, false, nrows);

    auto& variant = assert_cast<ColumnVariant&>(*dst);
    ASSERT_TRUE(hiter->_process_binary_column(variant, nrows).ok());

    // root column + 2 subcolumns
    EXPECT_EQ(variant.get_subcolumns().size(), 3);

    auto* abc_subcolumn = variant.get_subcolumn(PathInData("c"));
    auto* abd_subcolumn = variant.get_subcolumn(PathInData("d"));

    EXPECT_TRUE(abc_subcolumn);
    EXPECT_TRUE(abd_subcolumn);

    EXPECT_EQ(abc_subcolumn->get_non_null_value_size(), 2);
    EXPECT_EQ(abd_subcolumn->get_non_null_value_size(), 1);

    const auto& abc_subcolumn_data =
            assert_cast<const doris::ColumnNullable&>(*abc_subcolumn->get_finalized_column_ptr());
    const auto& abd_subcolumn_data =
            assert_cast<const doris::ColumnNullable&>(*abd_subcolumn->get_finalized_column_ptr());
    EXPECT_EQ(abc_subcolumn_data.get_nested_column_ptr()->get_data_at(0).to_string(), "abcvalues");
    EXPECT_EQ(abc_subcolumn_data.get_nested_column_ptr()->get_data_at(1).to_string(), "abcvalues");
    EXPECT_EQ(abd_subcolumn_data.get_nested_column_ptr()->get_data_at(0).to_string(), "abdvalues");

    const auto& read_map = assert_cast<const ColumnMap&>(*variant.get_sparse_column());
    const auto& read_keys = assert_cast<const ColumnString&>(read_map.get_keys());
    const auto& read_vals = assert_cast<const ColumnString&>(read_map.get_values());
    const auto& read_offs = read_map.get_offsets();

    EXPECT_EQ(read_offs.size(), 2);

    EXPECT_EQ(read_keys.get_data_at(0).to_string(), "e");
    auto val = read_vals.get_data_at(0).to_string();
    EXPECT_EQ(val.substr(val.size() - 9, 9), "abevalues");

    EXPECT_EQ(read_offs[0], 0);
    EXPECT_EQ(read_offs[1], 1);
}
