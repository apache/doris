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

#include "gtest/gtest.h"
#include "olap/rowset/segment_v2/index_iterator.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/functions_comparison.h"

namespace doris::vectorized {

class MockInvertedIndexReader : public segment_v2::InvertedIndexReader {
public:
    MockInvertedIndexReader(const TabletIndex& index_meta)
            : segment_v2::InvertedIndexReader(&index_meta, nullptr) {}
    ~MockInvertedIndexReader() override = default;

    segment_v2::InvertedIndexReaderType type() override {
        return segment_v2::InvertedIndexReaderType::BKD;
    }

    Status query(const segment_v2::IndexQueryContextPtr& context, const std::string& column_name,
                 const void* query_value, segment_v2::InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override {
        return Status::OK();
    }

    Status try_query(const segment_v2::IndexQueryContextPtr& context,
                     const std::string& column_name, const void* query_value,
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

} // namespace doris::vectorized
