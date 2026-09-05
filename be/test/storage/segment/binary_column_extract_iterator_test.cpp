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

#include <gtest/gtest.h>

#include <algorithm>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type/primitive_type.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/string_buffer.hpp"
#include "core/value/jsonb_value.h"
#include "exec/common/variant_util.h"
#include "storage/iterators.h"

namespace doris::segment_v2 {
namespace {

struct SparseReadCounters {
    size_t init = 0;
    size_t seek = 0;
    size_t next_batch = 0;
    size_t read_by_rowids = 0;
};

class FixedSparseIterator final : public ColumnIterator {
public:
    FixedSparseIterator(MutableColumnPtr source, std::shared_ptr<SparseReadCounters> counters)
            : _source(std::move(source)), _counters(std::move(counters)) {}

    Status init(const ColumnIteratorOptions& opts) override {
        ++_counters->init;
        return ColumnIterator::init(opts);
    }

    Status seek_to_ordinal(ordinal_t ord) override {
        ++_counters->seek;
        _ordinal = ord;
        return Status::OK();
    }

    ordinal_t get_current_ordinal() const override { return _ordinal; }

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override {
        ++_counters->next_batch;
        const size_t rows = std::min(*n, _source->size());
        if (rows == _source->size()) {
            dst = std::move(_source);
        } else {
            dst->insert_range_from(*_source, 0, rows);
        }
        *n = rows;
        _ordinal += rows;
        if (has_null != nullptr) {
            *has_null = false;
        }
        return Status::OK();
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override {
        ++_counters->read_by_rowids;
        for (size_t i = 0; i < count; ++i) {
            DORIS_CHECK_LT(rowids[i], _source->size());
            dst->insert_from(*_source, rowids[i]);
        }
        if (count != 0) {
            _ordinal = rowids[count - 1] + 1;
        }
        return Status::OK();
    }

private:
    MutableColumnPtr _source;
    std::shared_ptr<SparseReadCounters> _counters;
    ordinal_t _ordinal = 0;
};

void append_storage_cell(ColumnMap& map, std::string_view path, const IDataType& type,
                         const IColumn& values, size_t row) {
    auto& paths = assert_cast<ColumnString&>(map.get_keys());
    auto& cells = assert_cast<ColumnString&>(map.get_values());
    paths.insert_data(path.data(), path.size());
    type.get_serde()->write_one_cell_to_binary(values, cells.get_chars(), row);
    cells.get_offsets().push_back(cells.get_chars().size());
}

void append_raw_storage_cell(ColumnMap& map, std::string_view path, std::string_view cell) {
    auto& paths = assert_cast<ColumnString&>(map.get_keys());
    auto& cells = assert_cast<ColumnString&>(map.get_values());
    paths.insert_data(path.data(), path.size());
    cells.insert_data(cell.data(), cell.size());
}

std::string jsonb_storage_cell(std::string_view value) {
    JsonBinaryValue jsonb;
    DORIS_CHECK(jsonb.from_json_string(value.data(), value.size()).ok());
    std::string cell(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_JSONB));
    const size_t size = jsonb.size();
    cell.append(reinterpret_cast<const char*>(&size), sizeof(size));
    cell.append(jsonb.value(), jsonb.size());
    return cell;
}

MutableColumnPtr make_sparse_input() {
    auto sparse = variant_util::create_variant_binary_column();
    auto& map = assert_cast<ColumnMap&>(*sparse);
    auto& offsets = map.get_offsets();

    DataTypeInt64 int_type;
    auto ints = ColumnInt64::create();
    ints->insert_value(11);
    ints->insert_value(22);
    ints->insert_value(33);
    DataTypeString string_type;
    auto strings = ColumnString::create();
    strings->insert_data("mixed", 5);

    append_storage_cell(map, "a", int_type, *ints, 0);
    append_storage_cell(map, "b", int_type, *ints, 1);
    offsets.push_back(2);

    append_storage_cell(map, "b", string_type, *strings, 0);
    offsets.push_back(3);

    append_storage_cell(map, "a", int_type, *ints, 2);
    offsets.push_back(4);
    return sparse;
}

MutableColumnPtr make_rowid_sparse_input() {
    auto sparse = variant_util::create_variant_binary_column();
    auto& map = assert_cast<ColumnMap&>(*sparse);
    auto& offsets = map.get_offsets();

    DataTypeInt64 int_type;
    auto ints = ColumnInt64::create();
    for (const int64_t value : {10, 100, 11, 102, 13, 103}) {
        ints->insert_value(value);
    }

    append_storage_cell(map, "a", int_type, *ints, 0);
    append_storage_cell(map, "b", int_type, *ints, 1);
    offsets.push_back(2);

    append_storage_cell(map, "a", int_type, *ints, 2);
    offsets.push_back(3);

    append_storage_cell(map, "b", int_type, *ints, 3);
    offsets.push_back(4);

    append_storage_cell(map, "a", int_type, *ints, 4);
    append_storage_cell(map, "b", int_type, *ints, 5);
    offsets.push_back(6);
    return sparse;
}

MutableColumnPtr make_v2_destination() {
    return make_nullable(std::make_shared<DataTypeVariantV2>(3, false))->create_column();
}

std::string variant_v2_json_at(const ColumnVariantV2& column, size_t row) {
    auto output = ColumnString::create();
    BufferWritable writer(*output);
    DataTypeSerDe::FormatOptions options;
    DataTypeVariantV2SerDe serde;
    EXPECT_TRUE(serde.serialize_one_cell_to_json(column, row, writer, options).ok());
    writer.commit();
    return output->get_data_at(0).to_string();
}

TEST(BinaryColumnExtractIteratorV2Test, RejectsNonV2Destination) {
    auto counters = std::make_shared<SparseReadCounters>();
    auto cache = std::make_shared<BinaryColumnCache>(
            std::make_unique<FixedSparseIterator>(make_sparse_input(), counters),
            variant_util::create_variant_binary_column());
    OlapReaderStatistics stats;
    StorageReadOptions read_options;
    read_options.stats = &stats;
    BinaryColumnExtractIterator v2_reader("a", cache, &read_options);

    ColumnIteratorOptions iterator_options;
    iterator_options.stats = &stats;
    ASSERT_TRUE(v2_reader.init(iterator_options).ok());
    ASSERT_TRUE(v2_reader.seek_to_ordinal(0).ok());

    MutableColumnPtr v1_destination = ColumnString::create();
    size_t rows = 3;
    const Status v2_to_v1 = v2_reader.next_batch(&rows, v1_destination, nullptr);
    ASSERT_TRUE(v2_to_v1.is<ErrorCode::INVALID_ARGUMENT>()) << v2_to_v1;
    EXPECT_EQ(counters->next_batch, 0);
}

TEST(BinaryColumnExtractIteratorV2Test, SharedCacheProducesTypedAndEncodedResults) {
    auto counters = std::make_shared<SparseReadCounters>();
    auto cache = std::make_shared<BinaryColumnCache>(
            std::make_unique<FixedSparseIterator>(make_sparse_input(), counters),
            variant_util::create_variant_binary_column());
    OlapReaderStatistics stats;
    StorageReadOptions read_options;
    read_options.stats = &stats;
    auto extract_a = std::make_unique<BinaryColumnExtractIterator>("a", cache, &read_options);
    auto extract_b = std::make_unique<BinaryColumnExtractIterator>("b", cache, &read_options);

    ColumnIteratorOptions iterator_options;
    iterator_options.stats = &stats;
    ASSERT_TRUE(extract_a->init(iterator_options).ok());
    ASSERT_TRUE(extract_b->init(iterator_options).ok());
    ASSERT_TRUE(extract_a->seek_to_ordinal(0).ok());
    ASSERT_TRUE(extract_b->seek_to_ordinal(0).ok());

    auto a = make_v2_destination();
    size_t rows = 3;
    bool has_null = false;
    ASSERT_TRUE(extract_a->next_batch(&rows, a, &has_null).ok());
    ASSERT_EQ(rows, 3);
    EXPECT_TRUE(has_null);
    auto& nullable_a = assert_cast<ColumnNullable&>(*a);
    auto& variant_a = assert_cast<ColumnVariantV2&>(nullable_a.get_nested_column());
    EXPECT_FALSE(nullable_a.is_null_at(0));
    EXPECT_TRUE(nullable_a.is_null_at(1));
    EXPECT_FALSE(nullable_a.is_null_at(2));
    EXPECT_EQ(variant_v2_json_at(variant_a, 0), "11");
    EXPECT_EQ(variant_v2_json_at(variant_a, 2), "33");

    auto b = make_v2_destination();
    rows = 3;
    has_null = false;
    ASSERT_TRUE(extract_b->next_batch(&rows, b, &has_null).ok());
    ASSERT_EQ(rows, 3);
    EXPECT_TRUE(has_null);
    auto& nullable_b = assert_cast<ColumnNullable&>(*b);
    auto& variant_b = assert_cast<ColumnVariantV2&>(nullable_b.get_nested_column());
    EXPECT_FALSE(variant_b.is_typed());
    EXPECT_FALSE(nullable_b.is_null_at(0));
    EXPECT_FALSE(nullable_b.is_null_at(1));
    EXPECT_TRUE(nullable_b.is_null_at(2));
    EXPECT_EQ(variant_v2_json_at(variant_b, 0), "22");
    EXPECT_EQ(variant_v2_json_at(variant_b, 1), "\"mixed\"");

    EXPECT_EQ(counters->init, 1);
    EXPECT_EQ(counters->seek, 1);
    EXPECT_EQ(counters->next_batch, 1);
}

TEST(BinaryColumnExtractIteratorV2Test, PhysicalAndEncodedNullUseVersionNativeSemantics) {
    {
        auto sparse = variant_util::create_variant_binary_column();
        auto& map = assert_cast<ColumnMap&>(*sparse);
        map.get_offsets().push_back(0);
        const char none = static_cast<char>(FieldType::OLAP_FIELD_TYPE_NONE);
        append_raw_storage_cell(map, "a", std::string_view(&none, 1));
        map.get_offsets().push_back(1);
        append_raw_storage_cell(map, "a", jsonb_storage_cell("null"));
        map.get_offsets().push_back(2);

        auto counters = std::make_shared<SparseReadCounters>();
        auto cache = std::make_shared<BinaryColumnCache>(
                std::make_unique<FixedSparseIterator>(std::move(sparse), counters),
                variant_util::create_variant_binary_column());
        OlapReaderStatistics stats;
        StorageReadOptions read_options;
        read_options.stats = &stats;
        auto extract =
                std::make_unique<BinaryColumnExtractIterator>("a", std::move(cache), &read_options);
        ColumnIteratorOptions iterator_options;
        ASSERT_TRUE(extract->init(iterator_options).ok());
        ASSERT_TRUE(extract->seek_to_ordinal(0).ok());

        MutableColumnPtr destination = make_v2_destination();
        size_t rows = 3;
        bool has_null = false;
        ASSERT_TRUE(extract->next_batch(&rows, destination, &has_null).ok());
        ASSERT_EQ(rows, 3);
        EXPECT_TRUE(has_null);
        const auto& nullable = assert_cast<const ColumnNullable&>(*destination);
        EXPECT_TRUE(nullable.is_null_at(0));
        EXPECT_FALSE(nullable.is_null_at(1));
        EXPECT_FALSE(nullable.is_null_at(2));
        const auto& variant = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
        EXPECT_EQ(variant_v2_json_at(variant, 1), "null");
        EXPECT_EQ(variant_v2_json_at(variant, 2), "null");
    }
}

TEST(BinaryColumnExtractIteratorV2Test, ShortFinalAllMissingBatchUsesProducedRowCount) {
    {
        auto sparse = variant_util::create_variant_binary_column();
        auto& offsets = assert_cast<ColumnMap&>(*sparse).get_offsets();
        offsets.push_back(0);
        offsets.push_back(0);

        auto counters = std::make_shared<SparseReadCounters>();
        auto cache = std::make_shared<BinaryColumnCache>(
                std::make_unique<FixedSparseIterator>(std::move(sparse), counters),
                variant_util::create_variant_binary_column());
        OlapReaderStatistics stats;
        StorageReadOptions read_options;
        read_options.stats = &stats;
        BinaryColumnExtractIterator extract("a", std::move(cache), &read_options);
        ColumnIteratorOptions iterator_options;
        ASSERT_TRUE(extract.init(iterator_options).ok());
        ASSERT_TRUE(extract.seek_to_ordinal(0).ok());

        MutableColumnPtr destination = make_v2_destination();
        size_t rows = 3;
        bool has_null = false;
        ASSERT_TRUE(extract.next_batch(&rows, destination, &has_null).ok());
        EXPECT_EQ(rows, 2);
        EXPECT_EQ(destination->size(), 2);
        EXPECT_TRUE(has_null);
        const auto& nullable = assert_cast<const ColumnNullable&>(*destination);
        EXPECT_TRUE(nullable.is_null_at(0));
        EXPECT_TRUE(nullable.is_null_at(1));
    }
}

TEST(BinaryColumnExtractIteratorV2Test, SharedCacheReusesRowidBatchAcrossPaths) {
    auto counters = std::make_shared<SparseReadCounters>();
    auto cache = std::make_shared<BinaryColumnCache>(
            std::make_unique<FixedSparseIterator>(make_rowid_sparse_input(), counters),
            variant_util::create_variant_binary_column());
    OlapReaderStatistics stats;
    StorageReadOptions read_options;
    read_options.stats = &stats;
    auto extract_a = std::make_unique<BinaryColumnExtractIterator>("a", cache, &read_options);
    auto extract_b = std::make_unique<BinaryColumnExtractIterator>("b", cache, &read_options);

    ColumnIteratorOptions iterator_options;
    iterator_options.stats = &stats;
    ASSERT_TRUE(extract_a->init(iterator_options).ok());
    ASSERT_TRUE(extract_b->init(iterator_options).ok());

    const rowid_t rowids_a[] {0, 2};
    auto a_first = make_v2_destination();
    ASSERT_TRUE(extract_a->read_by_rowids(rowids_a, std::size(rowids_a), a_first).ok());
    EXPECT_EQ(counters->read_by_rowids, 1);
    auto b_first = make_v2_destination();
    ASSERT_TRUE(extract_b->read_by_rowids(rowids_a, std::size(rowids_a), b_first).ok());
    EXPECT_EQ(counters->read_by_rowids, 1);

    const auto& nullable_a_first = assert_cast<const ColumnNullable&>(*a_first);
    const auto& variant_a_first =
            assert_cast<const ColumnVariantV2&>(nullable_a_first.get_nested_column());
    EXPECT_FALSE(nullable_a_first.is_null_at(0));
    EXPECT_TRUE(nullable_a_first.is_null_at(1));
    EXPECT_EQ(variant_v2_json_at(variant_a_first, 0), "10");

    const auto& nullable_b_first = assert_cast<const ColumnNullable&>(*b_first);
    const auto& variant_b_first =
            assert_cast<const ColumnVariantV2&>(nullable_b_first.get_nested_column());
    EXPECT_FALSE(nullable_b_first.is_null_at(0));
    EXPECT_FALSE(nullable_b_first.is_null_at(1));
    EXPECT_EQ(variant_v2_json_at(variant_b_first, 0), "100");
    EXPECT_EQ(variant_v2_json_at(variant_b_first, 1), "102");

    const rowid_t rowids_b[] {1, 3};
    ASSERT_TRUE(extract_a->read_by_rowids(rowids_b, std::size(rowids_b), a_first).ok());
    EXPECT_EQ(counters->read_by_rowids, 2);
    ASSERT_TRUE(extract_b->read_by_rowids(rowids_b, std::size(rowids_b), b_first).ok());
    EXPECT_EQ(counters->read_by_rowids, 2);

    const auto& nullable_a_all = assert_cast<const ColumnNullable&>(*a_first);
    const auto& variant_a_all =
            assert_cast<const ColumnVariantV2&>(nullable_a_all.get_nested_column());
    ASSERT_EQ(variant_a_all.size(), 4);
    EXPECT_FALSE(nullable_a_all.is_null_at(0));
    EXPECT_TRUE(nullable_a_all.is_null_at(1));
    EXPECT_FALSE(nullable_a_all.is_null_at(2));
    EXPECT_FALSE(nullable_a_all.is_null_at(3));
    EXPECT_EQ(variant_v2_json_at(variant_a_all, 0), "10");
    EXPECT_EQ(variant_v2_json_at(variant_a_all, 2), "11");
    EXPECT_EQ(variant_v2_json_at(variant_a_all, 3), "13");

    const auto& nullable_b_all = assert_cast<const ColumnNullable&>(*b_first);
    const auto& variant_b_all =
            assert_cast<const ColumnVariantV2&>(nullable_b_all.get_nested_column());
    ASSERT_EQ(variant_b_all.size(), 4);
    EXPECT_FALSE(nullable_b_all.is_null_at(0));
    EXPECT_FALSE(nullable_b_all.is_null_at(1));
    EXPECT_TRUE(nullable_b_all.is_null_at(2));
    EXPECT_FALSE(nullable_b_all.is_null_at(3));
    EXPECT_EQ(variant_v2_json_at(variant_b_all, 0), "100");
    EXPECT_EQ(variant_v2_json_at(variant_b_all, 1), "102");
    EXPECT_EQ(variant_v2_json_at(variant_b_all, 3), "103");

    EXPECT_EQ(counters->init, 1);
    EXPECT_EQ(counters->next_batch, 0);
}

TEST(BinaryColumnExtractIteratorV2Test, RejectsInvalidOffsets) {
    auto sparse = variant_util::create_variant_binary_column();
    auto& map = assert_cast<ColumnMap&>(*sparse);
    auto& paths = assert_cast<ColumnString&>(map.get_keys());
    auto& cells = assert_cast<ColumnString&>(map.get_values());
    paths.insert_data("a", 1);
    cells.insert_data("unused", 6);
    map.get_offsets().push_back(2);

    auto counters = std::make_shared<SparseReadCounters>();
    auto cache = std::make_shared<BinaryColumnCache>(
            std::make_unique<FixedSparseIterator>(std::move(sparse), counters),
            variant_util::create_variant_binary_column());
    OlapReaderStatistics stats;
    StorageReadOptions read_options;
    read_options.stats = &stats;
    auto extract = std::make_unique<BinaryColumnExtractIterator>("a", cache, &read_options);
    ColumnIteratorOptions iterator_options;
    iterator_options.stats = &stats;
    ASSERT_TRUE(extract->init(iterator_options).ok());
    ASSERT_TRUE(extract->seek_to_ordinal(0).ok());

    auto dst = make_v2_destination();
    dst->insert_default();
    const size_t initial_size = dst->size();
    size_t rows = 1;
    bool has_null = false;
    const Status status = extract->next_batch(&rows, dst, &has_null);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    EXPECT_EQ(dst->size(), initial_size);
}

TEST(BinaryColumnExtractIteratorV2Test, RejectsDecreasingOffsets) {
    auto sparse = variant_util::create_variant_binary_column();
    auto& map = assert_cast<ColumnMap&>(*sparse);
    auto& paths = assert_cast<ColumnString&>(map.get_keys());
    auto& cells = assert_cast<ColumnString&>(map.get_values());
    paths.insert_data("a", 1);
    paths.insert_data("b", 1);
    cells.insert_data("unused-a", 8);
    cells.insert_data("unused-b", 8);
    map.get_offsets().push_back(2);
    map.get_offsets().push_back(1);

    auto counters = std::make_shared<SparseReadCounters>();
    auto cache = std::make_shared<BinaryColumnCache>(
            std::make_unique<FixedSparseIterator>(std::move(sparse), counters),
            variant_util::create_variant_binary_column());
    OlapReaderStatistics stats;
    StorageReadOptions read_options;
    read_options.stats = &stats;
    auto extract = std::make_unique<BinaryColumnExtractIterator>("a", cache, &read_options);
    ColumnIteratorOptions iterator_options;
    iterator_options.stats = &stats;
    ASSERT_TRUE(extract->init(iterator_options).ok());
    ASSERT_TRUE(extract->seek_to_ordinal(0).ok());

    auto dst = make_v2_destination();
    dst->insert_default();
    const size_t initial_size = dst->size();
    size_t rows = 2;
    bool has_null = false;
    const Status status = extract->next_batch(&rows, dst, &has_null);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    EXPECT_EQ(dst->size(), initial_size);
}

TEST(BinaryColumnExtractIteratorV2Test, RejectsOffsetsThatLeaveUnconsumedCells) {
    auto sparse = variant_util::create_variant_binary_column();
    auto& map = assert_cast<ColumnMap&>(*sparse);
    auto& paths = assert_cast<ColumnString&>(map.get_keys());
    auto& cells = assert_cast<ColumnString&>(map.get_values());
    paths.insert_data("a", 1);
    paths.insert_data("b", 1);
    cells.insert_data("unused-a", 8);
    cells.insert_data("unused-b", 8);
    map.get_offsets().push_back(1);

    auto counters = std::make_shared<SparseReadCounters>();
    auto cache = std::make_shared<BinaryColumnCache>(
            std::make_unique<FixedSparseIterator>(std::move(sparse), counters),
            variant_util::create_variant_binary_column());
    OlapReaderStatistics stats;
    StorageReadOptions read_options;
    read_options.stats = &stats;
    auto extract = std::make_unique<BinaryColumnExtractIterator>("a", cache, &read_options);
    ColumnIteratorOptions iterator_options;
    iterator_options.stats = &stats;
    ASSERT_TRUE(extract->init(iterator_options).ok());
    ASSERT_TRUE(extract->seek_to_ordinal(0).ok());

    auto dst = make_v2_destination();
    dst->insert_default();
    const size_t initial_size = dst->size();
    size_t rows = 1;
    bool has_null = false;
    const Status status = extract->next_batch(&rows, dst, &has_null);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    EXPECT_EQ(dst->size(), initial_size);
}

TEST(BinaryColumnExtractIteratorV2Test, RejectsMismatchedPathAndCellCounts) {
    auto sparse = variant_util::create_variant_binary_column();
    auto& map = assert_cast<ColumnMap&>(*sparse);
    auto& paths = assert_cast<ColumnString&>(map.get_keys());
    auto& cells = assert_cast<ColumnString&>(map.get_values());
    paths.insert_data("a", 1);
    paths.insert_data("b", 1);
    cells.insert_data("unused", 6);
    map.get_offsets().push_back(1);

    auto counters = std::make_shared<SparseReadCounters>();
    auto cache = std::make_shared<BinaryColumnCache>(
            std::make_unique<FixedSparseIterator>(std::move(sparse), counters),
            variant_util::create_variant_binary_column());
    OlapReaderStatistics stats;
    StorageReadOptions read_options;
    read_options.stats = &stats;
    auto extract = std::make_unique<BinaryColumnExtractIterator>("a", cache, &read_options);
    ColumnIteratorOptions iterator_options;
    iterator_options.stats = &stats;
    ASSERT_TRUE(extract->init(iterator_options).ok());
    ASSERT_TRUE(extract->seek_to_ordinal(0).ok());

    auto dst = make_v2_destination();
    dst->insert_default();
    const size_t initial_size = dst->size();
    size_t rows = 1;
    bool has_null = false;
    const Status status = extract->next_batch(&rows, dst, &has_null);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    EXPECT_EQ(dst->size(), initial_size);
}

TEST(BinaryColumnExtractIteratorV2Test, DoesNotRescanFrozenPathOrdering) {
    auto sparse = variant_util::create_variant_binary_column();
    auto& map = assert_cast<ColumnMap&>(*sparse);
    DataTypeInt64 int_type;
    auto ints = ColumnInt64::create();
    ints->insert_value(7);
    ints->insert_value(8);
    ints->insert_value(9);

    // The requested key is searchable by lower_bound. Repeated trailing keys deliberately make
    // this a white-box guard against restoring a separate O(cells) ordering-validation pass.
    append_storage_cell(map, "a", int_type, *ints, 0);
    append_storage_cell(map, "z", int_type, *ints, 1);
    append_storage_cell(map, "z", int_type, *ints, 2);
    map.get_offsets().push_back(3);

    auto counters = std::make_shared<SparseReadCounters>();
    auto cache = std::make_shared<BinaryColumnCache>(
            std::make_unique<FixedSparseIterator>(std::move(sparse), counters),
            variant_util::create_variant_binary_column());
    OlapReaderStatistics stats;
    StorageReadOptions read_options;
    read_options.stats = &stats;
    auto extract = std::make_unique<BinaryColumnExtractIterator>("a", cache, &read_options);
    ColumnIteratorOptions iterator_options;
    iterator_options.stats = &stats;
    ASSERT_TRUE(extract->init(iterator_options).ok());
    ASSERT_TRUE(extract->seek_to_ordinal(0).ok());

    auto dst = make_v2_destination();
    size_t rows = 1;
    bool has_null = false;
    const Status status = extract->next_batch(&rows, dst, &has_null);
    ASSERT_TRUE(status.ok()) << status;
    auto& nullable = assert_cast<ColumnNullable&>(*dst);
    auto& variant = assert_cast<ColumnVariantV2&>(nullable.get_nested_column());
    EXPECT_EQ(variant_v2_json_at(variant, 0), "7");
}

} // namespace
} // namespace doris::segment_v2
