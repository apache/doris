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

// Function boundary tests only. Reader identity and persisted gram schemes are exercised through
// real writer/reader/iterator/expression chains in like_gram_binding_test.cpp.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/column/column_const.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/like.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "storage/index/index_iterator.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_reader.h"

namespace doris {
namespace {

// Supplies a result or Status at the function's iterator boundary. It owns no index metadata,
// reader or policy and makes no claim about gram compilation or index capabilities.
class RecordingGramIndexIterator : public segment_v2::IndexIterator {
public:
    segment_v2::IndexReaderPtr get_reader(segment_v2::IndexReaderType) const override {
        return nullptr;
    }

    Status read_from_index(const segment_v2::IndexParam& param) override {
        queried = true;
        RETURN_IF_ERROR(query_status);
        auto* request = std::get<segment_v2::InvertedIndexParam*>(param);
        request->roaring->add(3);
        request->roaring->add(5);
        return Status::OK();
    }

    Status read_null_bitmap(segment_v2::InvertedIndexQueryCacheHandle*) override {
        return Status::OK();
    }

    Result<bool> has_null() override { return false; }

    Status query_status = Status::OK();
    bool queried = false;
};

ColumnWithTypeAndName const_string_arg(const std::string& value) {
    auto col = ColumnString::create();
    col->insert_data(value.data(), value.size());
    return {ColumnConst::create(std::move(col), 1), std::make_shared<DataTypeString>(), "arg"};
}

TEST(LikeGramIndexTest, SuccessfulIndexResultIsApproximate) {
    const std::vector<FunctionPtr> functions {FunctionLike::create(), FunctionRegexpLike::create()};
    ColumnsWithTypeAndName args {const_string_arg("hello")};
    std::vector<IndexFieldNameAndTypePair> names {{"msg", std::make_shared<DataTypeString>()}};
    for (const auto& function : functions) {
        RecordingGramIndexIterator iterator;
        segment_v2::InvertedIndexResultBitmap result;
        const auto status =
                function->evaluate_inverted_index(args, names, {&iterator}, 100, nullptr, result);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_TRUE(result.approximate());
        ASSERT_NE(result.get_data_bitmap(), nullptr);
        EXPECT_EQ(result.get_data_bitmap()->cardinality(), 2U);
        EXPECT_TRUE(result.get_data_bitmap()->contains(3));
        EXPECT_TRUE(result.get_data_bitmap()->contains(5));
    }
}

TEST(LikeGramIndexTest, CustomEscapeIsNotPushedDown) {
    FunctionLike function;
    const DataTypePtr type = std::make_shared<DataTypeString>();
    auto value = VSlotRef::create_shared(0, 0, 0, type, "value");
    auto pattern = VLiteral::create_shared(type, Field::create_field<TYPE_STRING>("%abcd%"));
    auto custom_escape = VLiteral::create_shared(type, Field::create_field<TYPE_STRING>("#"));
    auto default_escape = VLiteral::create_shared(type, Field::create_field<TYPE_STRING>("\\"));
    auto variable_escape = VSlotRef::create_shared(1, 1, 1, type, "escape");

    // Eligibility is checked while every operand still has its original position, including
    // an ESCAPE column that has no index and would disappear from the flattened arguments.
    EXPECT_FALSE(function.can_evaluate_inverted_index({value, pattern, custom_escape}));
    EXPECT_FALSE(function.can_evaluate_inverted_index({value, pattern, variable_escape}));
    EXPECT_FALSE(
            function.can_evaluate_inverted_index({value, pattern, default_escape, custom_escape}));
    EXPECT_TRUE(function.can_evaluate_inverted_index({value, pattern, default_escape}));
}

TEST(LikeGramIndexTest, ConfigDisabledSkipsPushDown) {
    RecordingGramIndexIterator iterator;
    FunctionRegexpLike function;
    std::vector<IndexFieldNameAndTypePair> names {{"msg", std::make_shared<DataTypeString>()}};
    segment_v2::InvertedIndexResultBitmap result;
    ColumnsWithTypeAndName args {const_string_arg("hello|world")};

    const bool saved_enable = config::enable_gram_index_regexp;
    config::enable_gram_index_regexp = false;
    const Status status =
            function.evaluate_inverted_index(args, names, {&iterator}, 100, nullptr, result);
    config::enable_gram_index_regexp = saved_enable;

    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(result.is_empty());
    EXPECT_FALSE(iterator.queried);
}

TEST(LikeGramIndexTest, ArbitraryIndexErrorDegradesToNoResult) {
    FunctionRegexpLike function;
    std::vector<IndexFieldNameAndTypePair> names {{"msg", std::make_shared<DataTypeString>()}};
    ColumnsWithTypeAndName args {const_string_arg("hello|world")};
    const std::vector<Status> degradable {
            Status::InternalError<false>("boom"),
            Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>("not supported"),
            Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED, false>("no prunable grams"),
            Status::Error<ErrorCode::CORRUPTION, false>("corrupt index image"),
            Status::Error<ErrorCode::IO_ERROR, false>("s3 read failed"),
    };
    for (const auto& injected : degradable) {
        RecordingGramIndexIterator iterator;
        iterator.query_status = injected;
        segment_v2::InvertedIndexResultBitmap result;
        const Status status =
                function.evaluate_inverted_index(args, names, {&iterator}, 100, nullptr, result);
        ASSERT_TRUE(status.ok()) << injected.to_string() << " -> " << status.to_string();
        EXPECT_TRUE(result.is_empty()) << injected.to_string();
    }
}

TEST(LikeGramIndexTest, CancellationAndMemoryErrorsPropagate) {
    FunctionRegexpLike function;
    std::vector<IndexFieldNameAndTypePair> names {{"msg", std::make_shared<DataTypeString>()}};
    ColumnsWithTypeAndName args {const_string_arg("hello|world")};
    const std::vector<int> propagated {ErrorCode::CANCELLED, ErrorCode::MEM_LIMIT_EXCEEDED,
                                       ErrorCode::MEM_ALLOC_FAILED};
    for (const int code : propagated) {
        RecordingGramIndexIterator iterator;
        iterator.query_status = Status(code, "stop the query");
        segment_v2::InvertedIndexResultBitmap result;
        const Status status =
                function.evaluate_inverted_index(args, names, {&iterator}, 100, nullptr, result);
        ASSERT_FALSE(status.ok()) << code;
        EXPECT_EQ(status.code(), code);
        EXPECT_TRUE(result.is_empty());
    }
}

} // namespace
} // namespace doris
