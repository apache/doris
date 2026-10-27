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

#include <gtest/gtest.h>

#include <bit>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/value/vdatetime_value.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/index/index_writer.h"
#include "storage/segment/binary_plain_page.h"
#include "storage/segment/column_writer.h"
#include "storage/segment/frame_of_reference_page.h"
#include "storage/segment/plain_page.h"
#include "util/defer_op.h"

namespace doris::segment_v2 {
namespace {

class ObservedPageBuilder final : public PageBuilder {
public:
    explicit ObservedPageBuilder(std::unique_ptr<PageBuilder> builder)
            : _builder(std::move(builder)) {}
    Status init() override { return _builder->init(); }
    bool is_page_full() override { return _builder->is_page_full(); }
    Status add(const uint8_t* values, size_t* count) override {
        inputs.emplace_back(reinterpret_cast<uintptr_t>(values), *count);
        return _builder->add(values, count);
    }
    Status finish(OwnedSlice* slice) override { return _builder->finish(slice); }
    Status reset() override { return _builder->reset(); }
    size_t count() const override { return _builder->count(); }
    uint64_t size() const override { return _builder->size(); }
    uint64_t get_raw_data_size() const override { return _builder->get_raw_data_size(); }
    std::vector<std::pair<uintptr_t, size_t>> inputs;

private:
    std::unique_ptr<PageBuilder> _builder;
};

class ArrayIndexRecorder final : public AnnIndexColumnWriter {
public:
    ArrayIndexRecorder(FieldType type, const ScalarColumnWriter& item_writer, size_t expected_items)
            : AnnIndexColumnWriter(nullptr, nullptr),
              _type(type),
              _item_writer(item_writer),
              _expected_items(expected_items) {}
    Status init() override { return Status::OK(); }
    Status add_values(const uint8_t*, size_t) override {
        return Status::NotSupported("Expected array input");
    }
    Status add_nulls(uint32_t) override { return Status::NotSupported("Expected array input"); }
    Status add_array_nulls(const uint8_t* null_map, size_t n) override {
        outer_nulls.insert(outer_nulls.end(), null_map, null_map + n);
        return Status::OK();
    }
    Status add_array_values(size_t width, const uint8_t* data, const uint8_t* null_map,
                            const uint64_t* offsets, size_t rows) override {
        const size_t n = offsets[rows] - offsets[0];
        // Every element must have been appended before the first array index batch.
        EXPECT_EQ(_item_writer.get_next_rowid(), _expected_items);
        for (size_t row = 0; row < rows; ++row) {
            row_sizes.push_back(offsets[row + 1] - offsets[row]);
        }
        for (size_t i = 0; i < n; ++i) {
            item_nulls.push_back(null_map != nullptr && null_map[i] != 0);
            values.push_back(
                    field_is_slice_type(_type)
                            ? reinterpret_cast<const Slice*>(data)[i].to_string()
                            : std::string(reinterpret_cast<const char*>(data + i * width), width));
        }
        ++calls;
        return Status::OK();
    }
    Status finish() override { return Status::OK(); }
    int64_t size() const override { return 0; }
    void close_on_error() override {}
    size_t calls = 0;
    std::vector<size_t> row_sizes;
    std::vector<uint8_t> outer_nulls;
    std::vector<uint8_t> item_nulls;
    std::vector<std::string> values;

private:
    FieldType _type;
    const ScalarColumnWriter& _item_writer;
    size_t _expected_items;
};

void set_meta(ColumnMetaPB* meta, FieldType type, size_t length, bool nullable,
              EncodingTypePB encoding) {
    meta->set_column_id(0);
    meta->set_unique_id(0);
    meta->set_type(static_cast<int>(type));
    meta->set_length(length);
    meta->set_is_nullable(nullable);
    meta->set_encoding(encoding);
    meta->set_compression(NO_COMPRESSION);
}

struct TestWriter {
    ColumnMetaPB meta;
    io::FileWriterPtr file;
    std::unique_ptr<ColumnWriter> root;
    ScalarColumnWriter* scalar = nullptr;
    ObservedPageBuilder* pages = nullptr;

    template <class Builder>
    Status init(FieldType type, size_t length, bool nullable, EncodingTypePB encoding,
                const std::string& name, bool array = false) {
        auto fs = io::global_local_filesystem();
        RETURN_IF_ERROR(fs->create_directory("./ut_dir/column_writer_storage"));
        RETURN_IF_ERROR(fs->create_file("./ut_dir/column_writer_storage/" + name, &file));
        TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, type, nullable);
        column.set_length(length);
        ColumnWriterOptions options;
        options.meta = &meta;
        set_meta(&meta, type, length, nullable, encoding);
        if (array) {
            TabletColumn parent(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                FieldType::OLAP_FIELD_TYPE_ARRAY, true, 0, 0);
            parent.add_sub_column(column);
            set_meta(&meta, FieldType::OLAP_FIELD_TYPE_ARRAY, 0, true, PLAIN_ENCODING);
            set_meta(meta.add_children_columns(), type, length, nullable, encoding);
            RETURN_IF_ERROR(ColumnWriter::create(options, &parent, file.get(), &root));
        } else {
            root = std::make_unique<ScalarColumnWriter>(
                    options, std::make_shared<TabletColumn>(column), file.get());
        }
        RETURN_IF_ERROR(root->init());
        scalar = array ? dynamic_cast<ScalarColumnWriter*>(
                                 static_cast<ArrayColumnWriter&>(*root)._item_writer.get())
                       : static_cast<ScalarColumnWriter*>(root.get());
        PageBuilderOptions page_options;
        page_options.data_page_size = 64;
        PageBuilder* builder = nullptr;
        RETURN_IF_ERROR(Builder::create(&builder, page_options));
        auto observed =
                std::make_unique<ObservedPageBuilder>(std::unique_ptr<PageBuilder>(builder));
        pages = observed.get();
        scalar->_page_builder = std::move(observed);
        scalar->_compress_codec = nullptr;
        return Status::OK();
    }
};

void expect_same_pages(const ScalarColumnWriter& actual, const ScalarColumnWriter& reference) {
    ASSERT_EQ(actual._pages.size(), reference._pages.size());
    for (size_t i = 0; i < actual._pages.size(); ++i) {
        EXPECT_EQ(actual._pages[i]->footer.SerializeAsString(),
                  reference._pages[i]->footer.SerializeAsString());
        ASSERT_EQ(actual._pages[i]->data.size(), reference._pages[i]->data.size());
        for (size_t j = 0; j < actual._pages[i]->data.size(); ++j) {
            EXPECT_EQ(actual._pages[i]->data[j].slice().to_string(),
                      reference._pages[i]->data[j].slice().to_string());
        }
    }
}

void finish_and_compare(TestWriter& actual, TestWriter& reference) {
    ASSERT_TRUE(actual.root->finish().ok());
    ASSERT_TRUE(reference.root->finish().ok());
    expect_same_pages(*actual.scalar, *reference.scalar);
    ASSERT_TRUE(actual.file->close().ok());
    ASSERT_TRUE(reference.file->close().ok());
}

ColumnOffset64::MutablePtr offsets_for(std::initializer_list<uint64_t> values) {
    auto offsets = ColumnOffset64::create();
    for (auto value : values) {
        offsets->insert_value(value);
    }
    return offsets;
}

MutableColumnPtr array_for(MutableColumnPtr items, std::initializer_list<uint64_t> offsets) {
    auto nulls = ColumnUInt8::create(offsets.size(), 0);
    auto array = ColumnArray::create(std::move(items), offsets_for(offsets));
    return ColumnNullable::create(std::move(array), std::move(nulls));
}

} // namespace

TEST(ColumnWriterStorageTest, ArrayCharKeepsPageAndIndexValuesConsistent) {
    auto strings = ColumnString::create();
    auto nulls = ColumnUInt8::create();
    for (size_t i = 0; i < 2505; ++i) {
        std::string value = i % 2 ? "abcd" : "a";
        if (i == 7) {
            value = "invalid NULL payload";
        }
        strings->insert_data(value.data(), value.size());
        nulls->insert_value(i == 7 || i == 1000);
    }
    auto items = ColumnNullable::create(std::move(strings), std::move(nulls));
    const auto before = items->get_nested_column().get_data_at(7).to_string();
    const auto* source_items = items.get();
    auto array = ColumnArray::create(std::move(items), offsets_for({2, 3, 3, 2006, 2502, 2505}));
    auto outer = ColumnUInt8::create();
    outer->get_data().assign({0, 0, 1, 0, 0, 0});
    auto input = ColumnNullable::create(std::move(array), std::move(outer));
    TestWriter actual;
    ASSERT_TRUE((actual.init<BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_CHAR>>(
                         FieldType::OLAP_FIELD_TYPE_CHAR, 4, true, PLAIN_ENCODING, "char", true))
                        .ok());
    auto recorder = std::make_unique<ArrayIndexRecorder>(FieldType::OLAP_FIELD_TYPE_CHAR,
                                                         *actual.scalar, 2500);
    auto* recorded = recorder.get();
    auto& writer = static_cast<ArrayColumnWriter&>(*actual.root);
    writer._opts.need_inverted_index = true;
    writer._inverted_index_writer = std::move(recorder);
    ASSERT_TRUE(writer.append(*input, 1, 4).ok());
    EXPECT_EQ(recorded->row_sizes, (std::vector<size_t> {1, 0, 2003, 496}));
    EXPECT_EQ(recorded->outer_nulls, (std::vector<uint8_t> {0, 1, 0, 0}));
    EXPECT_EQ(recorded->values[0], std::string("a\0\0\0", 4));
    EXPECT_TRUE(recorded->values[5].empty());
    std::vector<uint8_t> expected_nulls(2500, 0);
    expected_nulls[5] = 1;
    expected_nulls[998] = 1;
    EXPECT_EQ(recorded->item_nulls, expected_nulls);
    EXPECT_EQ(source_items->get_nested_column().get_data_at(7).to_string(), before);
    TestWriter reference;
    ASSERT_TRUE(
            (reference.init<BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_CHAR>>(
                     FieldType::OLAP_FIELD_TYPE_CHAR, 4, true, PLAIN_ENCODING, "char-reference"))
                    .ok());
    ASSERT_TRUE(reference.scalar->append(*source_items, 2, 2500).ok());
    finish_and_compare(actual, reference);
}

TEST(ColumnWriterStorageTest, FixedArrayWritesElementsBeforeIndexingWholeDocuments) {
    auto items = ColumnInt32::create();
    for (int32_t i = 0; i < 4609; ++i) {
        items->insert_value(i * 7);
    }
    const auto* source_items = items.get();
    auto input = array_for(std::move(items), {2, 5, 2006, 2006, 4607, 4609});
    TestWriter actual;
    ASSERT_TRUE((actual.init<FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_INT>>(
                         FieldType::OLAP_FIELD_TYPE_INT, 4, false, FOR_ENCODING, "for", true))
                        .ok());
    auto& writer = static_cast<ArrayColumnWriter&>(*actual.root);
    writer._inverted_index_writer = std::make_unique<ArrayIndexRecorder>(
            FieldType::OLAP_FIELD_TYPE_INT, *actual.scalar, 4605);
    const auto& index = static_cast<const ArrayIndexRecorder&>(*writer._inverted_index_writer);
    writer._opts.need_inverted_index = true;
    ASSERT_TRUE(writer.append(*input, 1, 4).ok());
    EXPECT_EQ(index.row_sizes, (std::vector<size_t> {3, 2001, 0, 2601}));
    EXPECT_EQ(index.values.size(), 4605);
    TestWriter reference;
    ASSERT_TRUE((reference.init<FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_INT>>(
                         FieldType::OLAP_FIELD_TYPE_INT, 4, false, FOR_ENCODING, "for-reference"))
                        .ok());
    ASSERT_TRUE(reference.scalar->append(*source_items, 2, 4605).ok());
    finish_and_compare(actual, reference);
}

TEST(ColumnWriterStorageTest, NullableV1ArrayPreservesScalarPagesAndIndexValues) {
    auto dates = ColumnDate::create();
    auto nulls = ColumnUInt8::create();
    VecDateTimeValue date;
    ASSERT_TRUE(date.from_date_int64(20200101));
    for (size_t i = 0; i < 4005; ++i) {
        dates->insert_value(date);
        nulls->insert_value(i == 2502);
    }
    auto items = ColumnNullable::create(std::move(dates), std::move(nulls));
    const auto* source_items = items.get();
    auto input = array_for(std::move(items), {2, 1002, 1002, 2002, 4002, 4005});
    TestWriter actual;
    ASSERT_TRUE(
            (actual.init<FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_DATE>>(
                     FieldType::OLAP_FIELD_TYPE_DATE, 3, true, FOR_ENCODING, "nullable-for", true))
                    .ok());
    auto& writer = static_cast<ArrayColumnWriter&>(*actual.root);
    writer._inverted_index_writer = std::make_unique<ArrayIndexRecorder>(
            FieldType::OLAP_FIELD_TYPE_DATE, *actual.scalar, 4000);
    const auto& index = static_cast<const ArrayIndexRecorder&>(*writer._inverted_index_writer);
    writer._opts.need_inverted_index = true;
    ASSERT_TRUE(writer.append(*input, 1, 4).ok());
    EXPECT_EQ(index.row_sizes, (std::vector<size_t> {1000, 0, 1000, 2000}));
    EXPECT_EQ(index.item_nulls[2500], 1);
    TestWriter reference;
    ASSERT_TRUE((reference.init<FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_DATE>>(
                         FieldType::OLAP_FIELD_TYPE_DATE, 3, true, FOR_ENCODING,
                         "nullable-for-reference"))
                        .ok());
    ASSERT_TRUE(reference.scalar->append(*source_items, 2, 4000).ok());
    finish_and_compare(actual, reference);
}

TEST(ColumnWriterStorageTest, FloatingArrayFeedsBothIndexesAfterAppendingElements) {
    auto data = ColumnFloat32::create();
    for (uint32_t bits : {0x3f800000U, 0x7f800001U, 0x40000000U, 0xff800005U}) {
        data->insert_value(std::bit_cast<float>(bits));
    }
    auto nulls = ColumnUInt8::create();
    nulls->get_data().assign({0, 0, 0, 1});
    auto items = ColumnNullable::create(std::move(data), std::move(nulls));
    const auto* source_items = items.get();
    auto input = array_for(std::move(items), {2, 4});
    TestWriter actual;
    ASSERT_TRUE((actual.init<PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_FLOAT>>(
                         FieldType::OLAP_FIELD_TYPE_FLOAT, 4, true, PLAIN_ENCODING, "float", true))
                        .ok());
    auto& writer = static_cast<ArrayColumnWriter&>(*actual.root);
    writer._inverted_index_writer = std::make_unique<ArrayIndexRecorder>(
            FieldType::OLAP_FIELD_TYPE_FLOAT, *actual.scalar, 4);
    writer._ann_index_writer = std::make_unique<ArrayIndexRecorder>(
            FieldType::OLAP_FIELD_TYPE_FLOAT, *actual.scalar, 4);
    const auto& inverted = static_cast<const ArrayIndexRecorder&>(*writer._inverted_index_writer);
    const auto& ann = static_cast<const ArrayIndexRecorder&>(*writer._ann_index_writer);
    writer._opts.need_inverted_index = true;
    writer._opts.need_ann_index = true;
    ASSERT_TRUE(writer.append(*input, 0, 2).ok());
    EXPECT_EQ(inverted.values, ann.values);
    EXPECT_EQ(inverted.outer_nulls, (std::vector<uint8_t> {0, 0}));
    EXPECT_TRUE(ann.outer_nulls.empty());
    EXPECT_EQ(ann.item_nulls, (std::vector<uint8_t> {0, 0, 0, 1}));
    EXPECT_EQ(inverted.values[1], inverted.values[3]);
    const auto& source =
            assert_cast<const ColumnFloat32&>(source_items->get_nested_column()).get_data();
    EXPECT_EQ(std::bit_cast<uint32_t>(source[1]), 0x7f800001U);
    EXPECT_EQ(std::bit_cast<uint32_t>(source[3]), 0xff800005U);
    ASSERT_TRUE(actual.root->finish().ok());
    ASSERT_TRUE(actual.file->close().ok());
}

TEST(ColumnWriterStorageTest, EmptyArraysPreserveDocumentsAndOuterNulls) {
    constexpr size_t rows = 2051;
    auto array = ColumnArray::create(ColumnInt32::create(), ColumnOffset64::create(rows, 0));
    auto nulls = ColumnUInt8::create();
    nulls->get_data().resize(rows);
    for (size_t i = 0; i < rows; ++i) {
        nulls->get_data()[i] = i % 3 == 0;
    }
    const std::vector<uint8_t> expected_nulls(nulls->get_data().begin(), nulls->get_data().end());
    auto input = ColumnNullable::create(std::move(array), std::move(nulls));
    TestWriter actual;
    ASSERT_TRUE((actual.init<PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_INT>>(
                         FieldType::OLAP_FIELD_TYPE_INT, 4, false, PLAIN_ENCODING, "empty", true))
                        .ok());
    auto& writer = static_cast<ArrayColumnWriter&>(*actual.root);
    writer._opts.need_inverted_index = true;
    writer._inverted_index_writer =
            std::make_unique<ArrayIndexRecorder>(FieldType::OLAP_FIELD_TYPE_INT, *actual.scalar, 0);
    const auto& index = static_cast<const ArrayIndexRecorder&>(*writer._inverted_index_writer);
    ASSERT_TRUE(writer.append(*input, 0, rows).ok());
    EXPECT_EQ(index.row_sizes, std::vector<size_t>(rows, 0));
    EXPECT_EQ(index.outer_nulls, expected_nulls);
    EXPECT_EQ(index.calls, 1);
    EXPECT_TRUE(index.values.empty());
    EXPECT_TRUE(actual.pages->inputs.empty());
    EXPECT_EQ(writer.get_next_rowid(), rows);
    ASSERT_TRUE(writer.finish().ok());
    ASSERT_TRUE(actual.file->close().ok());
}

TEST(ColumnWriterStorageTest, ArrayStringAdmissionRejectsTheWholeSelection) {
    auto items = ColumnString::create();
    for (size_t i = 0; i < 1500; ++i) {
        items->insert_data("x", 1);
    }
    items->insert_data("too-long", 8);
    auto input = array_for(std::move(items), {750, 1501});
    TestWriter actual;
    ASSERT_TRUE((actual.init<BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_STRING>>(
                         FieldType::OLAP_FIELD_TYPE_STRING, 64, false, PLAIN_ENCODING, "admission",
                         true))
                        .ok());
    auto& writer = static_cast<ArrayColumnWriter&>(*actual.root);
    writer._inverted_index_writer = std::make_unique<ArrayIndexRecorder>(
            FieldType::OLAP_FIELD_TYPE_STRING, *actual.scalar, 1501);
    const auto& index = static_cast<const ArrayIndexRecorder&>(*writer._inverted_index_writer);
    const auto old_limit = config::string_type_length_soft_limit_bytes;
    config::string_type_length_soft_limit_bytes = 7;
    Defer restore([&] { config::string_type_length_soft_limit_bytes = old_limit; });
    writer._opts.need_inverted_index = true;
    EXPECT_FALSE(writer.append(*input, 0, 2).ok());
    EXPECT_EQ(actual.scalar->get_next_rowid(), 0);
    EXPECT_TRUE(actual.pages->inputs.empty());
    EXPECT_EQ(index.calls, 0);
    ASSERT_TRUE(actual.file->close().ok());
}

} // namespace doris::segment_v2
