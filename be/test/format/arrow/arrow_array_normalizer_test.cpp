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
#include <arrow/memory_pool.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <limits>
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

class AllocateBeforeFreeMemoryPool : public arrow::MemoryPool {
public:
    explicit AllocateBeforeFreeMemoryPool(arrow::MemoryPool* delegate) : _delegate(delegate) {}

    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
        auto status = _delegate->Allocate(size, alignment, out);
        if (status.ok()) {
            _record_allocation(size);
        }
        return status;
    }

    arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                             uint8_t** ptr) override {
        uint8_t* replacement = nullptr;
        auto status = _delegate->Allocate(new_size, alignment, &replacement);
        if (!status.ok()) {
            return status;
        }
        std::memcpy(replacement, *ptr, static_cast<size_t>(std::min(old_size, new_size)));
        _record_allocation(new_size);
        _delegate->Free(*ptr, old_size, alignment);
        _bytes_allocated -= old_size;
        *ptr = replacement;
        return arrow::Status::OK();
    }

    void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
        _delegate->Free(buffer, size, alignment);
        _bytes_allocated -= size;
    }

    int64_t bytes_allocated() const override { return _bytes_allocated; }
    int64_t max_memory() const override { return _max_memory; }
    int64_t total_bytes_allocated() const override { return _total_bytes_allocated; }
    int64_t num_allocations() const override { return _num_allocations; }
    std::string backend_name() const override { return "allocate-before-free-test"; }

private:
    void _record_allocation(int64_t size) {
        _bytes_allocated += size;
        _max_memory = std::max(_max_memory, _bytes_allocated);
        _total_bytes_allocated += size;
        ++_num_allocations;
    }

    arrow::MemoryPool* _delegate;
    int64_t _bytes_allocated = 0;
    int64_t _max_memory = 0;
    int64_t _total_bytes_allocated = 0;
    int64_t _num_allocations = 0;
};

} // namespace

// Doris' own Arrow is already in the accepted shape; normalizing must not copy it.
TEST(ArrowArrayNormalizerTest, PlainStringIsAcceptedUnchanged) {
    arrow::StringBuilder b;
    ASSERT_TRUE(b.Append("doris").ok());
    std::shared_ptr<arrow::Array> in;
    ASSERT_TRUE(b.Finish(&in).ok());

    EXPECT_TRUE(is_serde_acceptable_arrow_type(*in->type()));

    std::shared_ptr<arrow::Array> out;
    ASSERT_TRUE(normalize_arrow_array(in, arrow::default_memory_pool(), &out).ok());
    EXPECT_EQ(in.get(), out.get());
}

// Go-based drivers emit large_*; the string serde only takes STRING/BINARY/FIXED_SIZE_BINARY.
TEST(ArrowArrayNormalizerTest, LargeStringIsConvertedToString) {
    auto in = build_large_string({"a", "bb", "ccc"});
    EXPECT_FALSE(is_serde_acceptable_arrow_type(*in->type()));

    std::shared_ptr<arrow::Array> out;
    ASSERT_TRUE(normalize_arrow_array(in, arrow::default_memory_pool(), &out).ok());
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
    ASSERT_TRUE(normalize_arrow_array(in, arrow::default_memory_pool(), &out).ok());
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
    ASSERT_TRUE(normalize_arrow_array(in, arrow::default_memory_pool(), &out).ok());
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
    ASSERT_TRUE(normalize_arrow_array(in, arrow::default_memory_pool(), &out).ok());
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
    ASSERT_TRUE(normalize_arrow_array(in, arrow::default_memory_pool(), &out).ok());
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
    ASSERT_TRUE(normalize_arrow_array(in, arrow::default_memory_pool(), &out).ok());
    ASSERT_EQ(out->type_id(), arrow::Type::LARGE_LIST);
    auto list = std::static_pointer_cast<arrow::LargeListArray>(out);
    ASSERT_EQ(list->length(), 2);
    EXPECT_EQ(list->value_slice(0)->ToString(), "[\n  20,\n  30\n]");
    EXPECT_EQ(list->value_slice(1)->ToString(), "[\n  10\n]");
}

TEST(ArrowArrayNormalizerTest, ListViewCanonicalizationUsesCallerMemoryPool) {
    arrow::Int32Builder offsets_builder;
    ASSERT_TRUE(offsets_builder.AppendValues({0, 0, 0}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());

    arrow::Int32Builder sizes_builder;
    ASSERT_TRUE(sizes_builder.AppendValues({3, 3, 3}).ok());
    std::shared_ptr<arrow::Array> sizes;
    ASSERT_TRUE(sizes_builder.Finish(&sizes).ok());

    arrow::Int32Builder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({10, 20, 30}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    auto in = arrow::ListViewArray::FromArrays(*offsets, *sizes, *values).ValueOrDie();
    arrow::ProxyMemoryPool pool(arrow::default_memory_pool());
    std::shared_ptr<arrow::Array> out;
    ASSERT_TRUE(normalize_arrow_array(in, &pool, &out).ok());
    EXPECT_GT(pool.bytes_allocated(), 0);
    out.reset();
    EXPECT_EQ(pool.bytes_allocated(), 0);
}

TEST(ArrowArrayNormalizerTest, ListViewPreReservesExpandedChildAllocation) {
    constexpr int32_t kRows = 1025;
    constexpr int32_t kValuesPerRow = 1000;
    arrow::Int32Builder offsets_builder;
    ASSERT_TRUE(offsets_builder.AppendValues(std::vector<int32_t>(kRows, 0)).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());

    arrow::Int32Builder sizes_builder;
    ASSERT_TRUE(sizes_builder.AppendValues(std::vector<int32_t>(kRows, kValuesPerRow)).ok());
    std::shared_ptr<arrow::Array> sizes;
    ASSERT_TRUE(sizes_builder.Finish(&sizes).ok());

    arrow::Int32Builder values_builder;
    ASSERT_TRUE(values_builder.AppendValues(std::vector<int32_t>(kValuesPerRow, 7)).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    auto in = arrow::ListViewArray::FromArrays(*offsets, *sizes, *values).ValueOrDie();
    AllocateBeforeFreeMemoryPool pool(arrow::default_memory_pool());
    std::shared_ptr<arrow::Array> out;
    ASSERT_TRUE(normalize_arrow_array(in, &pool, &out).ok());
    EXPECT_LE(pool.max_memory(), pool.bytes_allocated() + 256 * 1024);
}

TEST(ArrowArrayNormalizerTest, NestedListViewOverflowIsRejectedBeforeAllocation) {
    const int64_t range_length = std::numeric_limits<int64_t>::max() - 1;
    arrow::Int64Builder inner_offsets_builder;
    ASSERT_TRUE(inner_offsets_builder.AppendValues({0, 0}).ok());
    std::shared_ptr<arrow::Array> inner_offsets;
    ASSERT_TRUE(inner_offsets_builder.Finish(&inner_offsets).ok());
    arrow::Int64Builder inner_sizes_builder;
    ASSERT_TRUE(inner_sizes_builder.AppendValues({range_length, range_length}).ok());
    std::shared_ptr<arrow::Array> inner_sizes;
    ASSERT_TRUE(inner_sizes_builder.Finish(&inner_sizes).ok());
    auto null_values = std::make_shared<arrow::NullArray>(std::numeric_limits<int64_t>::max());
    auto inner = arrow::LargeListViewArray::FromArrays(*inner_offsets, *inner_sizes, *null_values)
                         .ValueOrDie();

    arrow::Int64Builder outer_offsets_builder;
    ASSERT_TRUE(outer_offsets_builder.Append(0).ok());
    std::shared_ptr<arrow::Array> outer_offsets;
    ASSERT_TRUE(outer_offsets_builder.Finish(&outer_offsets).ok());
    arrow::Int64Builder outer_sizes_builder;
    ASSERT_TRUE(outer_sizes_builder.Append(2).ok());
    std::shared_ptr<arrow::Array> outer_sizes;
    ASSERT_TRUE(outer_sizes_builder.Finish(&outer_sizes).ok());
    auto outer = arrow::LargeListViewArray::FromArrays(*outer_offsets, *outer_sizes, *inner)
                         .ValueOrDie();

    arrow::ProxyMemoryPool pool(arrow::default_memory_pool());
    std::shared_ptr<arrow::Array> out;
    EXPECT_FALSE(normalize_arrow_array(outer, &pool, &out).ok());
    EXPECT_EQ(pool.max_memory(), 0);
}

TEST(ArrowArrayNormalizerTest, NullableListViewSlicePreservesValidity) {
    arrow::Int32Builder offsets_builder;
    ASSERT_TRUE(offsets_builder.Append(0).ok());
    ASSERT_TRUE(offsets_builder.AppendNull().ok());
    ASSERT_TRUE(offsets_builder.Append(1).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());

    arrow::Int32Builder sizes_builder;
    ASSERT_TRUE(sizes_builder.AppendValues({1, 0, 1}).ok());
    std::shared_ptr<arrow::Array> sizes;
    ASSERT_TRUE(sizes_builder.Finish(&sizes).ok());

    arrow::Int32Builder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({10, 20}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    auto full = arrow::ListViewArray::FromArrays(*offsets, *sizes, *values).ValueOrDie();
    auto in = full->Slice(1, 2);
    ASSERT_EQ(in->offset(), 1);

    std::shared_ptr<arrow::Array> out;
    ASSERT_TRUE(normalize_arrow_array(in, arrow::default_memory_pool(), &out).ok());
    ASSERT_EQ(out->type_id(), arrow::Type::LIST);
    EXPECT_TRUE(out->IsNull(0));
    EXPECT_TRUE(out->IsValid(1));
    auto list = std::static_pointer_cast<arrow::ListArray>(out);
    EXPECT_EQ(list->value_slice(1)->ToString(), "[\n  20\n]");
}

TEST(ArrowArrayNormalizerTest, NullableLargeListViewSlicePreservesValidity) {
    arrow::Int64Builder offsets_builder;
    ASSERT_TRUE(offsets_builder.Append(0).ok());
    ASSERT_TRUE(offsets_builder.AppendNull().ok());
    ASSERT_TRUE(offsets_builder.Append(1).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());

    arrow::Int64Builder sizes_builder;
    ASSERT_TRUE(sizes_builder.AppendValues({1, 0, 1}).ok());
    std::shared_ptr<arrow::Array> sizes;
    ASSERT_TRUE(sizes_builder.Finish(&sizes).ok());

    arrow::Int32Builder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({10, 20}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    auto full = arrow::LargeListViewArray::FromArrays(*offsets, *sizes, *values).ValueOrDie();
    auto in = full->Slice(1, 2);
    ASSERT_EQ(in->offset(), 1);

    std::shared_ptr<arrow::Array> out;
    ASSERT_TRUE(normalize_arrow_array(in, arrow::default_memory_pool(), &out).ok());
    ASSERT_EQ(out->type_id(), arrow::Type::LARGE_LIST);
    EXPECT_TRUE(out->IsNull(0));
    EXPECT_TRUE(out->IsValid(1));
    auto list = std::static_pointer_cast<arrow::LargeListArray>(out);
    EXPECT_EQ(list->value_slice(1)->ToString(), "[\n  20\n]");
}

TEST(ArrowArrayNormalizerTest, InvalidListViewRangeIsRejectedBeforeCopy) {
    arrow::Int32Builder offsets_builder;
    ASSERT_TRUE(offsets_builder.AppendValues({0, 2}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());

    arrow::Int32Builder sizes_builder;
    ASSERT_TRUE(sizes_builder.AppendValues({1, 1}).ok());
    std::shared_ptr<arrow::Array> sizes;
    ASSERT_TRUE(sizes_builder.Finish(&sizes).ok());

    arrow::Int32Builder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({10, 20}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    auto in = arrow::ListViewArray::FromArrays(*offsets, *sizes, *values).ValueOrDie();
    std::shared_ptr<arrow::Array> out;
    EXPECT_FALSE(normalize_arrow_array(in, arrow::default_memory_pool(), &out).ok());
}

TEST(ArrowArrayNormalizerTest, InvalidLargeListViewRangeIsRejectedBeforeCopy) {
    arrow::Int64Builder offsets_builder;
    ASSERT_TRUE(offsets_builder.AppendValues({0, 2}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());

    arrow::Int64Builder sizes_builder;
    ASSERT_TRUE(sizes_builder.AppendValues({1, 1}).ok());
    std::shared_ptr<arrow::Array> sizes;
    ASSERT_TRUE(sizes_builder.Finish(&sizes).ok());

    arrow::Int32Builder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({10, 20}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    auto in = arrow::LargeListViewArray::FromArrays(*offsets, *sizes, *values).ValueOrDie();
    std::shared_ptr<arrow::Array> out;
    EXPECT_FALSE(normalize_arrow_array(in, arrow::default_memory_pool(), &out).ok());
}

void expect_dictionary_child_list_view_is_decoded(bool large_offsets) {
    arrow::Int8Builder dictionary_builder;
    ASSERT_TRUE(dictionary_builder.AppendValues({42, 43}).ok());
    std::shared_ptr<arrow::Array> dictionary;
    ASSERT_TRUE(dictionary_builder.Finish(&dictionary).ok());

    arrow::Int8Builder indices_builder;
    ASSERT_TRUE(indices_builder.AppendValues({0, 1}).ok());
    std::shared_ptr<arrow::Array> indices;
    ASSERT_TRUE(indices_builder.Finish(&indices).ok());
    auto dictionary_type = arrow::dictionary(arrow::int8(), arrow::int8());
    auto values = std::make_shared<arrow::DictionaryArray>(dictionary_type, indices, dictionary);

    std::shared_ptr<arrow::Array> input;
    if (large_offsets) {
        arrow::Int64Builder offsets_builder;
        ASSERT_TRUE(offsets_builder.Append(0).ok());
        std::shared_ptr<arrow::Array> offsets;
        ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());
        arrow::Int64Builder sizes_builder;
        ASSERT_TRUE(sizes_builder.Append(2).ok());
        std::shared_ptr<arrow::Array> sizes;
        ASSERT_TRUE(sizes_builder.Finish(&sizes).ok());
        input = arrow::LargeListViewArray::FromArrays(*offsets, *sizes, *values).ValueOrDie();
    } else {
        arrow::Int32Builder offsets_builder;
        ASSERT_TRUE(offsets_builder.Append(0).ok());
        std::shared_ptr<arrow::Array> offsets;
        ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());
        arrow::Int32Builder sizes_builder;
        ASSERT_TRUE(sizes_builder.Append(2).ok());
        std::shared_ptr<arrow::Array> sizes;
        ASSERT_TRUE(sizes_builder.Finish(&sizes).ok());
        input = arrow::ListViewArray::FromArrays(*offsets, *sizes, *values).ValueOrDie();
    }

    std::shared_ptr<arrow::Array> output;
    ASSERT_TRUE(normalize_arrow_array(input, arrow::default_memory_pool(), &output).ok());
    ASSERT_TRUE(is_serde_acceptable_arrow_type(*output->type()));
    const auto decoded_values =
            large_offsets ? std::static_pointer_cast<arrow::LargeListArray>(output)->values()
                          : std::static_pointer_cast<arrow::ListArray>(output)->values();
    ASSERT_EQ(decoded_values->type_id(), arrow::Type::INT8);
    const auto decoded = std::static_pointer_cast<arrow::Int8Array>(decoded_values);
    EXPECT_EQ(decoded->Value(0), 42);
    EXPECT_EQ(decoded->Value(1), 43);
}

TEST(ArrowArrayNormalizerTest, ListViewDictionaryChildIsDecoded) {
    expect_dictionary_child_list_view_is_decoded(false);
}

TEST(ArrowArrayNormalizerTest, LargeListViewDictionaryChildIsDecoded) {
    expect_dictionary_child_list_view_is_decoded(true);
}

// An unsupported type must name itself, otherwise the offending column cannot be found in prod.
TEST(ArrowArrayNormalizerTest, UnsupportedTypeFailsLoudWithTypeName) {
    auto in = arrow::MakeArrayOfNull(arrow::month_interval(), 1).ValueOrDie();
    EXPECT_FALSE(is_serde_acceptable_arrow_type(*in->type()));

    std::shared_ptr<arrow::Array> out;
    Status st = normalize_arrow_array(in, arrow::default_memory_pool(), &out);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("interval"), std::string::npos);
}

} // namespace doris
