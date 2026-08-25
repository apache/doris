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

#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "exprs/function/functions_comparison.h"
#include "gtest/gtest.h"
#include "storage/index/index_iterator.h"
#include "storage/index/inverted/inverted_index_reader.h"

namespace doris {

class MockInvertedIndexReader : public segment_v2::InvertedIndexReader {
public:
    MockInvertedIndexReader(const TabletIndex& index_meta)
            : segment_v2::InvertedIndexReader(&index_meta, nullptr) {}
    ~MockInvertedIndexReader() override = default;

    segment_v2::InvertedIndexReaderType type() override {
        return segment_v2::InvertedIndexReaderType::BKD;
    }

    Status query(const segment_v2::IndexQueryContextPtr& context, const std::string& column_name,
                 const Field& query_value, segment_v2::InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override {
        return Status::OK();
    }

    Status try_query(const segment_v2::IndexQueryContextPtr& context,
                     const std::string& column_name, const Field& query_value,
                     segment_v2::InvertedIndexQueryType query_type, size_t* count) override {
        return Status::OK();
    }

    Status new_iterator(std::unique_ptr<segment_v2::IndexIterator>* iterator) override {
        return Status::OK();
    }
};

class MockComparisonIndexIterator : public segment_v2::IndexIterator {
public:
    MockComparisonIndexIterator(std::shared_ptr<MockInvertedIndexReader> reader)
            : _reader(reader) {}
    ~MockComparisonIndexIterator() override = default;

    segment_v2::IndexReaderPtr get_reader(segment_v2::IndexReaderType reader_type) const override {
        if (std::holds_alternative<segment_v2::InvertedIndexReaderType>(reader_type)) {
            if (std::get<segment_v2::InvertedIndexReaderType>(reader_type) ==
                segment_v2::InvertedIndexReaderType::BKD) {
                return _reader;
            }
        }
        return nullptr;
    }

    Status read_from_index(const segment_v2::IndexParam& param) override {
        auto* p = std::get<segment_v2::InvertedIndexParam*>(param);
        p->roaring->addRange(0, 10);
        return Status::OK();
    }

    Status read_null_bitmap(segment_v2::InvertedIndexQueryCacheHandle* cache_handle) override {
        return Status::OK();
    }

    Result<bool> has_null() override { return false; }

private:
    std::shared_ptr<MockInvertedIndexReader> _reader;
};

TEST(FunctionComparisonTest, evaluate_inverted_index_with_null_param) {
    FunctionComparison<EqualsOp, NameEquals> func;

    auto nested_col = ColumnInt32::create();
    nested_col->insert_default();

    auto null_map = ColumnUInt8::create();
    null_map->insert_value(1);

    auto nullable_col = ColumnNullable::create(std::move(nested_col), std::move(null_map));

    auto const_nullable_col = ColumnConst::create(std::move(nullable_col), 1);

    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    ColumnsWithTypeAndName arguments = {
            {std::move(const_nullable_col), nullable_type, "null_param"}};

    std::vector<IndexFieldNameAndTypePair> data_type_with_names = {
            {"test_col", std::make_shared<DataTypeInt32>()}};

    TabletIndex index_meta;
    auto reader = std::make_shared<MockInvertedIndexReader>(index_meta);
    auto iter = std::make_unique<MockComparisonIndexIterator>(reader);
    std::vector<segment_v2::IndexIterator*> iterators = {iter.get()};

    segment_v2::InvertedIndexResultBitmap bitmap_result;
    auto status = func.evaluate_inverted_index(arguments, data_type_with_names, iterators, 100,
                                               nullptr, bitmap_result);

    ASSERT_TRUE(status.ok()) << "Status should be OK when param is NULL";

    ASSERT_EQ(bitmap_result.get_data_bitmap(), nullptr)
            << "bitmap_result should not be set when param is NULL";
}

TEST(FunctionComparisonTest, TimestampNsComparesExactlyWithOutOfRangeDateTimeV2) {
    DateV2Value<DateTimeV2ValueType> timestamp_datetime;
    ASSERT_TRUE(timestamp_datetime.check_range_and_set_time(2024, 2, 29, 12, 34, 56, 123456));
    TimeStampNsValue timestamp;
    ASSERT_TRUE(timestamp.from_datetime(timestamp_datetime, 789));
    TimeStampNsValue timestamp_at_microsecond;
    ASSERT_TRUE(timestamp_at_microsecond.from_datetime(timestamp_datetime));

    DateV2Value<DateTimeV2ValueType> old_datetime;
    ASSERT_TRUE(old_datetime.check_range_and_set_time(1600, 1, 1, 0, 0, 0, 0));
    DateV2Value<DateTimeV2ValueType> later_datetime;
    ASSERT_TRUE(later_datetime.check_range_and_set_time(2024, 2, 29, 12, 34, 56, 123457));

    auto timestamp_column = ColumnTimeStampNs::create();
    timestamp_column->insert_value(timestamp);
    timestamp_column->insert_value(timestamp);
    timestamp_column->insert_value(timestamp);
    timestamp_column->insert_value(timestamp_at_microsecond);
    auto datetime_column = ColumnDateTimeV2::create();
    datetime_column->insert_value(
            binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(old_datetime));
    datetime_column->insert_value(
            binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(timestamp_datetime));
    datetime_column->insert_value(
            binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(later_datetime));
    datetime_column->insert_value(
            binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(timestamp_datetime));

    Block block;
    block.insert({std::move(timestamp_column), std::make_shared<DataTypeTimeStampNs>(), "ts"});
    block.insert({std::move(datetime_column), std::make_shared<DataTypeDateTimeV2>(6), "dt"});
    block.insert({nullptr, std::make_shared<DataTypeUInt8>(), "result"});

    FunctionComparison<GreaterOp, NameGreater> greater;
    ASSERT_TRUE(greater.execute_impl(nullptr, block, {0, 1}, 2, 4).ok());
    const auto& result = assert_cast<const ColumnUInt8&>(*block.get_by_position(2).column);
    EXPECT_EQ(result.get_data(), (ColumnUInt8::Container {1, 1, 0, 0}));

    FunctionComparison<LessOp, NameLess> less;
    ASSERT_TRUE(less.execute_impl(nullptr, block, {1, 0}, 2, 4).ok());
    const auto& reversed_result = assert_cast<const ColumnUInt8&>(*block.get_by_position(2).column);
    EXPECT_EQ(reversed_result.get_data(), (ColumnUInt8::Container {1, 1, 0, 0}));

    FunctionComparison<EqualsOp, NameEquals> equals;
    ASSERT_TRUE(equals.execute_impl(nullptr, block, {0, 1}, 2, 4).ok());
    const auto& equals_result = assert_cast<const ColumnUInt8&>(*block.get_by_position(2).column);
    EXPECT_EQ(equals_result.get_data(), (ColumnUInt8::Container {0, 0, 0, 1}));
}

} // namespace doris
