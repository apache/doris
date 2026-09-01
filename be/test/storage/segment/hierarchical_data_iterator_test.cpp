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
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_string.h"
#include "exec/common/variant_util.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "util/json/path_in_data.h"

namespace doris {

using segment_v2::ColumnIterator;
using segment_v2::ColumnIteratorOptions;
using segment_v2::HierarchicalDataIterator;
using segment_v2::SubstreamIterator;

namespace {

struct BatchState {
    size_t next_batch_calls = 0;
    bool scratch_was_always_empty = true;
    ordinal_t current_ordinal = 0;
    std::vector<size_t> produced_batches;
};

class EmptySparseIterator final : public ColumnIterator {
public:
    explicit EmptySparseIterator(size_t total_rows = std::numeric_limits<size_t>::max())
            : _total_rows(total_rows) {}

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
        const size_t remaining =
                _current_ordinal < _total_rows ? _total_rows - _current_ordinal : 0;
        const size_t produced = std::min(*rows, remaining);
        map->get_offsets().resize_fill(produced, 0);
        _current_ordinal += produced;
        *rows = produced;
        return Status::OK();
    }

    Status read_by_rowids(const segment_v2::rowid_t*, const size_t count,
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
};

class ThreeRowJsonbIterator final : public ColumnIterator {
public:
    explicit ThreeRowJsonbIterator(std::shared_ptr<BatchState> state, std::string_view json = "{}")
            : _state(std::move(state)), _json(json) {}

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

        const size_t produced = std::min(*rows, kRows - _state->current_ordinal);
        auto serde = std::make_shared<DataTypeJsonb>()->get_serde();
        DataTypeSerDe::FormatOptions options;
        for (size_t row = 0; row < produced; ++row) {
            Slice json(_json.data(), _json.size());
            RETURN_IF_ERROR(serde->deserialize_one_cell_from_json(*strings, json, options));
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

    Status read_by_rowids(const segment_v2::rowid_t*, const size_t, MutableColumnPtr&) override {
        return Status::NotSupported("ThreeRowJsonbIterator only supports sequential reads");
    }

private:
    static constexpr size_t kRows = 3;
    std::shared_ptr<BatchState> _state;
    std::string _json;
};

class ThreeRowSparseIterator final : public ColumnIterator {
public:
    explicit ThreeRowSparseIterator(std::shared_ptr<BatchState> state) : _state(std::move(state)) {}

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

        const size_t produced = std::min(*rows, kRows - _state->current_ordinal);
        auto& keys = assert_cast<ColumnString&>(map->get_keys());
        auto& values = assert_cast<ColumnString&>(map->get_values());
        auto& offsets = map->get_offsets();
        auto source = ColumnString::create();
        for (size_t row = 0; row < produced; ++row) {
            const std::string_view value = kValues[_state->current_ordinal + row];
            source->insert_data(value.data(), value.size());
        }
        auto serde = std::make_shared<DataTypeString>()->get_serde();
        auto& value_chars = values.get_chars();
        for (size_t row = 0; row < produced; ++row) {
            keys.insert_data("s", 1);
            serde->write_one_cell_to_binary(*source, value_chars, row);
            values.get_offsets().push_back(value_chars.size());
            offsets.push_back(keys.size());
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

    Status read_by_rowids(const segment_v2::rowid_t*, const size_t, MutableColumnPtr&) override {
        return Status::NotSupported("ThreeRowSparseIterator only supports sequential reads");
    }

private:
    static constexpr size_t kRows = 3;
    static constexpr std::array<std::string_view, kRows> kValues {"row0", "row1", "row2"};
    std::shared_ptr<BatchState> _state;
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

        const size_t produced = std::min(*rows, kRows - _current_ordinal);
        auto& keys = assert_cast<ColumnString&>(map->get_keys());
        auto& values = assert_cast<ColumnString&>(map->get_values());
        auto& offsets = map->get_offsets();
        auto jsonb = std::make_shared<DataTypeJsonb>();
        auto source = jsonb->create_column();
        auto serde = jsonb->get_serde();
        DataTypeSerDe::FormatOptions options;
        for (size_t row = 0; row < produced; ++row) {
            const std::string_view value = kValues[_current_ordinal + row];
            Slice json(value.data(), value.size());
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

    Status read_by_rowids(const segment_v2::rowid_t*, const size_t, MutableColumnPtr&) override {
        return Status::NotSupported("TwoRowJsonbSparseIterator only supports sequential reads");
    }

private:
    static constexpr size_t kRows = 2;
    static constexpr std::array<std::string_view, kRows> kValues {"null", "1"};
    std::string _path;
    ordinal_t _current_ordinal = 0;
};

static std::unique_ptr<SubstreamIterator> make_empty_sparse_stream(
        size_t total_rows = std::numeric_limits<size_t>::max()) {
    return std::make_unique<SubstreamIterator>(variant_util::create_variant_binary_column(),
                                               std::make_unique<EmptySparseIterator>(total_rows),
                                               nullptr);
}

static std::unique_ptr<SubstreamIterator> make_three_row_root_stream(
        const std::shared_ptr<BatchState>& state, std::string_view json = "{}") {
    auto type = std::make_shared<DataTypeJsonb>();
    return std::make_unique<SubstreamIterator>(
            type->create_column(), std::make_unique<ThreeRowJsonbIterator>(state, json), type);
}

static std::unique_ptr<SubstreamIterator> make_three_row_sparse_stream(
        const std::shared_ptr<BatchState>& state) {
    return std::make_unique<SubstreamIterator>(variant_util::create_variant_binary_column(),
                                               std::make_unique<ThreeRowSparseIterator>(state),
                                               nullptr);
}

static std::unique_ptr<SubstreamIterator> make_two_row_jsonb_sparse_stream(
        std::string_view path = "s") {
    return std::make_unique<SubstreamIterator>(
            variant_util::create_variant_binary_column(),
            std::make_unique<TwoRowJsonbSparseIterator>(std::string(path)), nullptr);
}

struct VariantJsonWriter {
    void write(const char* data, size_t size) { value.append(data, size); }

    std::string value;
};

static std::string variant_v2_json_at(const ColumnVariantV2& column, size_t row) {
    VariantJsonWriter writer;
    to_json(column.get_value_ref(row), writer);
    return writer.value;
}

} // namespace

TEST(HierarchicalDataIteratorTest, RejectsNonV2Destination) {
    segment_v2::ColumnIteratorUPtr iterator;
    OlapReaderStatistics stats;
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &iterator, 0, PathInData("s"), nullptr, make_two_row_jsonb_sparse_stream(),
                        nullptr, nullptr, &stats,
                        HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE)
                        .ok());

    ColumnIteratorOptions options;
    options.stats = &stats;
    ASSERT_TRUE(iterator->init(options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

    MutableColumnPtr dst = ColumnString::create();
    size_t rows = 2;
    Status st = iterator->next_batch(&rows, dst);
    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("ColumnVariantV2"), std::string::npos);
}

TEST(HierarchicalDataIteratorTest, MissingSubtreeRowsBecomeNullableOuterNulls) {
    segment_v2::ColumnIteratorUPtr iterator;
    OlapReaderStatistics stats;
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &iterator, 0, PathInData("unrecorded"), nullptr, make_empty_sparse_stream(),
                        nullptr, nullptr, &stats,
                        HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE)
                        .ok());

    ColumnIteratorOptions options;
    options.stats = &stats;
    ASSERT_TRUE(iterator->init(options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

    MutableColumnPtr values = ColumnVariantV2::create();
    MutableColumnPtr dst = ColumnNullable::create(std::move(values), ColumnUInt8::create());
    size_t rows = 2;
    bool has_null = false;
    ASSERT_TRUE(iterator->next_batch(&rows, dst, &has_null).ok());
    ASSERT_EQ(rows, 2);
    EXPECT_TRUE(has_null);
    const auto& nullable = assert_cast<const ColumnNullable&>(*dst);
    EXPECT_TRUE(nullable.is_null_at(0));
    EXPECT_TRUE(nullable.is_null_at(1));
}

TEST(HierarchicalDataIteratorTest, ExactSparseJsonNullKeepsVariantNullValue) {
    segment_v2::ColumnIteratorUPtr iterator;
    OlapReaderStatistics stats;
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &iterator, 0, PathInData("s"), nullptr, make_two_row_jsonb_sparse_stream(),
                        nullptr, nullptr, &stats,
                        HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE)
                        .ok());

    ColumnIteratorOptions options;
    options.stats = &stats;
    ASSERT_TRUE(iterator->init(options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

    MutableColumnPtr values = ColumnVariantV2::create();
    MutableColumnPtr dst = ColumnNullable::create(std::move(values), ColumnUInt8::create());
    size_t rows = 2;
    bool has_null = true;
    ASSERT_TRUE(iterator->next_batch(&rows, dst, &has_null).ok());
    ASSERT_EQ(rows, 2);
    EXPECT_FALSE(has_null);
    const auto& nullable = assert_cast<const ColumnNullable&>(*dst);
    EXPECT_FALSE(nullable.is_null_at(0));
    EXPECT_FALSE(nullable.is_null_at(1));
    const auto& variant = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    EXPECT_EQ(variant_v2_json_at(variant, 0), "null");
    EXPECT_EQ(variant_v2_json_at(variant, 1), "1");
}

TEST(HierarchicalDataIteratorTest, DescendantJsonNullKeepsObjectVisible) {
    segment_v2::ColumnIteratorUPtr iterator;
    OlapReaderStatistics stats;
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &iterator, 0, PathInData("s"), nullptr,
                        make_two_row_jsonb_sparse_stream("s.child"), nullptr, nullptr, &stats,
                        HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE)
                        .ok());

    ColumnIteratorOptions options;
    options.stats = &stats;
    ASSERT_TRUE(iterator->init(options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

    MutableColumnPtr values = ColumnVariantV2::create();
    MutableColumnPtr dst = ColumnNullable::create(std::move(values), ColumnUInt8::create());
    size_t rows = 2;
    bool has_null = true;
    ASSERT_TRUE(iterator->next_batch(&rows, dst, &has_null).ok());
    ASSERT_EQ(rows, 2);
    EXPECT_FALSE(has_null);
    const auto& nullable = assert_cast<const ColumnNullable&>(*dst);
    EXPECT_FALSE(nullable.is_null_at(0));
    EXPECT_FALSE(nullable.is_null_at(1));
    const auto& variant = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    EXPECT_EQ(variant_v2_json_at(variant, 0), R"({"child":null})");
    EXPECT_EQ(variant_v2_json_at(variant, 1), R"({"child":1})");
}

TEST(HierarchicalDataIteratorTest, ConsecutiveBatchesClearRootAndSparseScratchColumns) {
    constexpr std::array<std::string_view, 3> expected {
            R"({"s":"row0"})",
            R"({"s":"row1"})",
            R"({"s":"row2"})",
    };
    auto root_state = std::make_shared<BatchState>();
    auto sparse_state = std::make_shared<BatchState>();
    segment_v2::ColumnIteratorUPtr iterator;
    OlapReaderStatistics stats;
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &iterator, 0, PathInData(), nullptr,
                        make_three_row_sparse_stream(sparse_state),
                        make_three_row_root_stream(root_state, R"({"root":1})"), nullptr, &stats,
                        HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE)
                        .ok());

    ColumnIteratorOptions options;
    options.stats = &stats;
    ASSERT_TRUE(iterator->init(options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());

    MutableColumnPtr dst = ColumnVariantV2::create();
    size_t rows = 2;
    ASSERT_TRUE(iterator->next_batch(&rows, dst).ok());
    ASSERT_EQ(rows, 2);
    rows = 2;
    ASSERT_TRUE(iterator->next_batch(&rows, dst).ok());
    ASSERT_EQ(rows, 1);
    ASSERT_EQ(dst->size(), 3);
    EXPECT_TRUE(root_state->scratch_was_always_empty);
    EXPECT_TRUE(sparse_state->scratch_was_always_empty);
    EXPECT_EQ(root_state->produced_batches, (std::vector<size_t> {2, 1}));
    EXPECT_EQ(sparse_state->produced_batches, (std::vector<size_t> {2, 1}));

    const auto& variant = assert_cast<const ColumnVariantV2&>(*dst);
    for (size_t row = 0; row < expected.size(); ++row) {
        EXPECT_EQ(variant_v2_json_at(variant, row), expected[row]);
    }
}

} // namespace doris
