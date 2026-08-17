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

#include "format/arrow/arrow_array_normalizer.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

namespace doris {

namespace {

std::shared_ptr<arrow::Array> build_large_string(const std::vector<std::string>& vals) {
    arrow::LargeStringBuilder b;
    for (const auto& v : vals) {
        EXPECT_TRUE(b.Append(v).ok());
    }
    std::shared_ptr<arrow::Array> out;
    EXPECT_TRUE(b.Finish(&out).ok());
    return out;
}

} // namespace

// Doris' own Arrow is already in the accepted shape; normalizing must not copy it.
TEST(ArrowArrayNormalizerTest, PlainStringIsAcceptedUnchanged) {
    arrow::StringBuilder b;
    ASSERT_TRUE(b.Append("doris").ok());
    std::shared_ptr<arrow::Array> in;
    ASSERT_TRUE(b.Finish(&in).ok());

    EXPECT_TRUE(is_serde_acceptable_arrow_type(*in->type()));

    std::shared_ptr<arrow::Array> out;
    ASSERT_TRUE(normalize_arrow_array(in, &out).ok());
    EXPECT_EQ(in.get(), out.get());
}

// Go-based drivers emit large_*; the string serde only takes STRING/BINARY/FIXED_SIZE_BINARY.
TEST(ArrowArrayNormalizerTest, LargeStringIsConvertedToString) {
    auto in = build_large_string({"a", "bb", "ccc"});
    EXPECT_FALSE(is_serde_acceptable_arrow_type(*in->type()));

    std::shared_ptr<arrow::Array> out;
    ASSERT_TRUE(normalize_arrow_array(in, &out).ok());
    ASSERT_EQ(out->type_id(), arrow::Type::STRING);
    ASSERT_EQ(out->length(), 3);
    auto sa = std::static_pointer_cast<arrow::StringArray>(out);
    EXPECT_EQ(sa->GetString(0), "a");
    EXPECT_EQ(sa->GetString(2), "ccc");
}

// Dictionary encoding is an encoding, not a type: decode it to the value type.
TEST(ArrowArrayNormalizerTest, DictionaryIsDecodedToValueType) {
    arrow::StringBuilder dict_b;
    ASSERT_TRUE(dict_b.AppendValues({"x", "y"}).ok());
    std::shared_ptr<arrow::Array> dict;
    ASSERT_TRUE(dict_b.Finish(&dict).ok());

    arrow::Int32Builder idx_b;
    ASSERT_TRUE(idx_b.AppendValues({0, 1, 0}).ok());
    std::shared_ptr<arrow::Array> idx;
    ASSERT_TRUE(idx_b.Finish(&idx).ok());

    auto dict_type = arrow::dictionary(arrow::int32(), arrow::utf8());
    auto in = std::make_shared<arrow::DictionaryArray>(dict_type, idx, dict);
    EXPECT_FALSE(is_serde_acceptable_arrow_type(*in->type()));

    std::shared_ptr<arrow::Array> out;
    ASSERT_TRUE(normalize_arrow_array(in, &out).ok());
    ASSERT_EQ(out->type_id(), arrow::Type::STRING);
    auto sa = std::static_pointer_cast<arrow::StringArray>(out);
    ASSERT_EQ(sa->length(), 3);
    EXPECT_EQ(sa->GetString(0), "x");
    EXPECT_EQ(sa->GetString(1), "y");
    EXPECT_EQ(sa->GetString(2), "x");
}

// A conversion that drops nulls corrupts data silently, which is worse than failing.
TEST(ArrowArrayNormalizerTest, NullsArePreservedAcrossConversion) {
    arrow::LargeStringBuilder b;
    ASSERT_TRUE(b.Append("a").ok());
    ASSERT_TRUE(b.AppendNull().ok());
    std::shared_ptr<arrow::Array> in;
    ASSERT_TRUE(b.Finish(&in).ok());

    std::shared_ptr<arrow::Array> out;
    ASSERT_TRUE(normalize_arrow_array(in, &out).ok());
    ASSERT_EQ(out->length(), 2);
    EXPECT_FALSE(out->IsNull(0));
    EXPECT_TRUE(out->IsNull(1));
}

// Decoding a dictionary can expose another variant underneath; one pass is not enough.
TEST(ArrowArrayNormalizerTest, DictionaryOfLargeStringIsFullyNormalized) {
    auto dict = build_large_string({"x", "y"});

    arrow::Int32Builder idx_b;
    ASSERT_TRUE(idx_b.AppendValues({1, 0}).ok());
    std::shared_ptr<arrow::Array> idx;
    ASSERT_TRUE(idx_b.Finish(&idx).ok());

    auto dict_type = arrow::dictionary(arrow::int32(), arrow::large_utf8());
    auto in = std::make_shared<arrow::DictionaryArray>(dict_type, idx, dict);

    std::shared_ptr<arrow::Array> out;
    ASSERT_TRUE(normalize_arrow_array(in, &out).ok());
    ASSERT_EQ(out->type_id(), arrow::Type::STRING);
    auto sa = std::static_pointer_cast<arrow::StringArray>(out);
    ASSERT_EQ(sa->length(), 2);
    EXPECT_EQ(sa->GetString(0), "y");
    EXPECT_EQ(sa->GetString(1), "x");
}

TEST(ArrowArrayNormalizerTest, ListViewIsConvertedToListInLogicalOrder) {
    arrow::Int32Builder offsets_builder;
    ASSERT_TRUE(offsets_builder.AppendValues({2, 0, 1}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());

    arrow::Int32Builder sizes_builder;
    ASSERT_TRUE(sizes_builder.AppendValues({2, 1, 2}).ok());
    std::shared_ptr<arrow::Array> sizes;
    ASSERT_TRUE(sizes_builder.Finish(&sizes).ok());

    arrow::Int32Builder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({1, 2, 3, 4}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    auto in = arrow::ListViewArray::FromArrays(*offsets, *sizes, *values).ValueOrDie();
    EXPECT_FALSE(is_serde_acceptable_arrow_type(*in->type()));

    std::shared_ptr<arrow::Array> out;
    ASSERT_TRUE(normalize_arrow_array(in, &out).ok());
    ASSERT_EQ(out->type_id(), arrow::Type::LIST);
    auto list = std::static_pointer_cast<arrow::ListArray>(out);
    ASSERT_EQ(list->length(), 3);
    EXPECT_EQ(list->value_slice(0)->ToString(), "[\n  3,\n  4\n]");
    EXPECT_EQ(list->value_slice(1)->ToString(), "[\n  1\n]");
    EXPECT_EQ(list->value_slice(2)->ToString(), "[\n  2,\n  3\n]");
}

TEST(ArrowArrayNormalizerTest, LargeListViewIsConvertedToLargeList) {
    arrow::Int64Builder offsets_builder;
    ASSERT_TRUE(offsets_builder.AppendValues({1, 0}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());

    arrow::Int64Builder sizes_builder;
    ASSERT_TRUE(sizes_builder.AppendValues({2, 1}).ok());
    std::shared_ptr<arrow::Array> sizes;
    ASSERT_TRUE(sizes_builder.Finish(&sizes).ok());

    arrow::Int32Builder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({10, 20, 30}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    auto in = arrow::LargeListViewArray::FromArrays(*offsets, *sizes, *values).ValueOrDie();
    EXPECT_FALSE(is_serde_acceptable_arrow_type(*in->type()));

    std::shared_ptr<arrow::Array> out;
    ASSERT_TRUE(normalize_arrow_array(in, &out).ok());
    ASSERT_EQ(out->type_id(), arrow::Type::LARGE_LIST);
    auto list = std::static_pointer_cast<arrow::LargeListArray>(out);
    ASSERT_EQ(list->length(), 2);
    EXPECT_EQ(list->value_slice(0)->ToString(), "[\n  20,\n  30\n]");
    EXPECT_EQ(list->value_slice(1)->ToString(), "[\n  10\n]");
}

// An unsupported type must name itself, otherwise the offending column cannot be found in prod.
TEST(ArrowArrayNormalizerTest, UnsupportedTypeFailsLoudWithTypeName) {
    auto in = arrow::MakeArrayOfNull(arrow::month_interval(), 1).ValueOrDie();
    EXPECT_FALSE(is_serde_acceptable_arrow_type(*in->type()));

    std::shared_ptr<arrow::Array> out;
    Status st = normalize_arrow_array(in, &out);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("interval"), std::string::npos);
}

} // namespace doris
