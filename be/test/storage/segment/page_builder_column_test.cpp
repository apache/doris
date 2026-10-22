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

// ColumnWriter converts compute-layer rows before feeding a physical page builder.
// Compare its pages with independently materialized storage values, including
// the page boundaries across append calls.

#include <gtest/gtest.h>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_complex.h"
#include "core/column/column_decimal.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/value/bitmap_value.h"
#include "core/value/decimalv2_value.h"
#include "core/value/hll.h"
#include "core/value/jsonb_value.h"
#include "core/value/vdatetime_value.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/segment/binary_dict_page.h"
#include "storage/segment/binary_plain_page.h"
#include "storage/segment/binary_plain_page_v2.h"
#include "storage/segment/binary_plain_page_v3.h"
#include "storage/segment/binary_prefix_page.h"
#include "storage/segment/bitshuffle_page.h"
#include "storage/segment/column_writer.h"
#include "storage/segment/frame_of_reference_page.h"
#include "storage/segment/options.h"
#include "storage/segment/page_builder.h"
#include "storage/segment/plain_page.h"
#include "storage/segment/rle_page.h"
#include "storage/storage_layout.h"
#include "util/defer_op.h"
#include "util/slice.h"

namespace doris::segment_v2 {
namespace {

// The pages a builder produced for its input, in order, and each one's count.
struct Pages {
    std::vector<std::string> bytes;
    std::vector<size_t> counts;
};

template <typename Builder>
std::unique_ptr<Builder> make_builder(const PageBuilderOptions& options) {
    PageBuilder* raw = nullptr;
    EXPECT_TRUE(Builder::create(&raw, options).ok());
    return std::unique_ptr<Builder>(static_cast<Builder*>(raw));
}

// Observe the public page input contract without exposing concrete writer scratch buffers.
class RecordingPageBuilder final : public PageBuilder {
public:
    explicit RecordingPageBuilder(std::unique_ptr<PageBuilder> builder)
            : _builder(std::move(builder)) {}

    Status init() override { return _builder->init(); }
    bool is_page_full() override { return _builder->is_page_full(); }
    Status add(const uint8_t* values, size_t* count) override {
        inputs.emplace_back(values, *count);
        max_requested = std::max(max_requested, *count);
        return _builder->add(values, count);
    }
    Status finish(OwnedSlice* slice) override { return _builder->finish(slice); }
    Status reset() override { return _builder->reset(); }
    size_t count() const override { return _builder->count(); }
    uint64_t size() const override { return _builder->size(); }
    uint64_t get_raw_data_size() const override { return _builder->get_raw_data_size(); }

    std::vector<std::pair<const uint8_t*, size_t>> inputs;
    size_t max_requested = 0;

private:
    std::unique_ptr<PageBuilder> _builder;
};

// Drives a builder the way ScalarColumnWriter does over appends of up to kAppendRows rows: add
// until every row is taken, finishing a page whenever the builder reports it full. The RLE and
// FOR builders take every row offered, so only the appends make them start past row 0 and cut a
// page between calls.
constexpr size_t kAppendRows = 37;

template <typename AddFn>
Pages drain(PageBuilder& builder, size_t num_rows, AddFn add, size_t append_rows = kAppendRows) {
    Pages pages;
    auto flush = [&] {
        pages.counts.push_back(builder.count());
        OwnedSlice page;
        EXPECT_TRUE(builder.finish(&page).ok());
        pages.bytes.emplace_back(page.slice().to_string());
        EXPECT_TRUE(builder.reset().ok());
    };
    size_t pos = 0;
    while (pos < num_rows) {
        size_t taken = std::min(num_rows - pos, append_rows);
        EXPECT_TRUE(add(pos, &taken).ok());
        pos += taken;
        if (builder.is_page_full()) {
            flush();
        }
    }
    if (builder.count() > 0) {
        flush();
    }
    return pages;
}

// The cells of a column as the cell entry takes them: the stored values back to back
// for a fixed-width FieldType, one Slice per row otherwise; CHAR padded to char_length.
template <FieldType FT>
struct ReferenceCells {
    static constexpr bool kSlices = std::is_same_v<typename CppTypeTraits<FT>::CppType, Slice>;

    std::vector<uint8_t> bytes;
    std::vector<Slice> slices;
    std::vector<StorageBuffers> buffers;

    explicit ReferenceCells(const IColumn& column, size_t char_length = 0,
                            FieldType storage_type = FT) {
        const size_t n = column.size();
        if constexpr (kSlices) {
            // One row at a time through buffers of its own, so every Slice stays valid.
            buffers.resize(n);
            for (size_t i = 0; i < n; ++i) {
                const uint8_t* cell = nullptr;
                if constexpr (FT == FieldType::OLAP_FIELD_TYPE_CHAR) {
                    cell = StorageLayout<FT>::column_to_storage(column, i, 1, char_length,
                                                                /*null_map=*/nullptr, buffers[i]);
                } else {
                    cell = column_to_storage(storage_type, column, i, 1, buffers[i]);
                }
                slices.push_back(*reinterpret_cast<const Slice*>(cell));
            }
        } else {
            using Column = typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(
                    FT)>::ColumnType;
            const auto& data = assert_cast<const Column&>(column).get_data();
            bytes.resize(n * sizeof(typename CppTypeTraits<FT>::CppType));
            for (size_t i = 0; i < n; ++i) {
                const auto value = StorageLayout<FT>::to_storage(data[i]);
                memcpy(bytes.data() + i * sizeof(value), &value, sizeof(value));
            }
        }
    }

    // The cells from `pos` on, through the builder's own cell entry.
    template <class Builder>
    Status add(Builder& builder, size_t pos, size_t* n) const {
        if constexpr (kSlices) {
            return builder.add(reinterpret_cast<const uint8_t*>(slices.data() + pos), n);
        } else {
            return builder.add(bytes.data() + pos * sizeof(typename CppTypeTraits<FT>::CppType), n);
        }
    }
};

template <typename Builder, FieldType FT>
constexpr EncodingTypePB builder_encoding() {
    if constexpr (std::is_same_v<Builder, BitshufflePageBuilder<FT>>) {
        return BIT_SHUFFLE;
    } else if constexpr (std::is_same_v<Builder, FrameOfReferencePageBuilder<FT>>) {
        return FOR_ENCODING;
    } else if constexpr (std::is_same_v<Builder, RlePageBuilder<FieldType::OLAP_FIELD_TYPE_BOOL>>) {
        return RLE;
    } else if constexpr (std::is_same_v<Builder, BinaryPlainPageV2Builder<FT>>) {
        return PLAIN_ENCODING_V2;
    } else if constexpr (std::is_same_v<Builder, BinaryPlainPageV3Builder<FT>>) {
        return PLAIN_ENCODING_V3;
    } else if constexpr (std::is_same_v<Builder, BinaryDictPageBuilder>) {
        return DICT_ENCODING;
    } else if constexpr (std::is_same_v<Builder, BinaryPrefixPageBuilder>) {
        return PREFIX_ENCODING;
    } else {
        return PLAIN_ENCODING;
    }
}

template <FieldType FT>
void expect_page_inputs(const RecordingPageBuilder& builder, const IColumn& column) {
    if constexpr (std::is_floating_point_v<typename CppTypeTraits<FT>::CppType>) {
        using Column =
                typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::ColumnType;
        const auto& values = assert_cast<const Column&>(column).get_data();
        if (std::none_of(values.begin(), values.end(),
                         [](auto value) { return std::isnan(value); })) {
            const auto begin = reinterpret_cast<uintptr_t>(values.data());
            const auto end = begin + values.size() * sizeof(values[0]);
            EXPECT_TRUE(std::all_of(builder.inputs.begin(), builder.inputs.end(), [&](auto input) {
                const auto address = reinterpret_cast<uintptr_t>(input.first);
                return address >= begin && address + input.second * sizeof(values[0]) <= end;
            }));
        }
    }
    if constexpr (FT == FieldType::OLAP_FIELD_TYPE_AGG_STATE) {
        EXPECT_GT(builder.max_requested, 1) << "Borrowed states must reach the page in batches";
    }
}

// Feed stored cells directly to the encoder and the same input through ColumnWriter.
template <FieldType FT, class Builder>
void expect_same_pages(const IColumn& source, PageBuilderOptions options, size_t min_pages = 1,
                       size_t append_rows = kAppendRows, FieldType storage_type = FT) {
    auto tablet_column = std::make_shared<TabletColumn>(
            FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, FT, false);
    // The writer pads CHAR to the column's length; the reference cells are padded the same.
    size_t char_length = 0;
    if constexpr (FT == FieldType::OLAP_FIELD_TYPE_CHAR) {
        const auto& strings = assert_cast<const ColumnString&>(source);
        for (size_t row = 0; row < strings.size(); ++row) {
            char_length = std::max(char_length, strings.get_data_at(row).size);
        }
        tablet_column->set_length(char_length);
    }
    const IColumn& column = source;
    const ReferenceCells<FT> cells(column, char_length, storage_type);

    auto by_cells = make_builder<Builder>(options);
    const Pages expected = drain(
            *by_cells, column.size(),
            [&](size_t pos, size_t* n) { return cells.add(*by_cells, pos, n); }, append_rows);

    auto fs = io::global_local_filesystem();
    ASSERT_TRUE(fs->create_directory("./ut_dir/page_builder_column_test").ok());
    io::FileWriterPtr file;
    ASSERT_TRUE(fs->create_file("./ut_dir/page_builder_column_test/data", &file).ok());
    ColumnMetaPB meta;
    meta.set_column_id(0);
    meta.set_unique_id(0);
    meta.set_type(int(FT));
    meta.set_length(sizeof(typename CppTypeTraits<FT>::CppType));
    meta.set_encoding(builder_encoding<Builder, FT>());
    meta.set_compression(NO_COMPRESSION);
    meta.set_is_nullable(false);
    ColumnWriterOptions writer_options;
    writer_options.meta = &meta;
    auto column_writer = std::make_unique<ScalarColumnWriter>(writer_options, tablet_column,
                                                              file.get(), storage_type);
    auto& writer = *column_writer;
    // Inject the encoder under test; no indexes or compression affect the comparison.
    auto page_builder = std::make_unique<RecordingPageBuilder>(make_builder<Builder>(options));
    const auto* recorded = page_builder.get();
    writer._page_builder = std::move(page_builder);
    writer._compress_codec = nullptr;
    for (size_t pos = 0; pos < column.size(); pos += append_rows) {
        ASSERT_TRUE(writer.append(column, pos, std::min(append_rows, column.size() - pos)).ok());
    }
    ASSERT_TRUE(writer.finish().ok());
    expect_page_inputs<FT>(*recorded, column);
    Pages actual;
    for (const auto& page : writer._pages) {
        actual.counts.push_back(page->footer.data_page_footer().num_values());
        actual.bytes.emplace_back(page->data.front().slice().to_string());
    }
    ASSERT_TRUE(file->close().ok());

    EXPECT_GE(expected.bytes.size(), min_pages);
    EXPECT_EQ(expected.counts, actual.counts);
    ASSERT_EQ(expected.bytes.size(), actual.bytes.size());
    for (size_t i = 0; i < expected.bytes.size(); ++i) {
        EXPECT_EQ(expected.bytes[i], actual.bytes[i]) << "page " << i;
    }
}

PageBuilderOptions small_pages(size_t data_page_size) {
    PageBuilderOptions options;
    options.data_page_size = data_page_size;
    return options;
}

ColumnPtr int32_column(size_t n) {
    auto column = ColumnInt32::create();
    for (size_t i = 0; i < n; ++i) {
        column->insert_value(static_cast<int32_t>(i * 7) - 100);
    }
    return column;
}

ColumnPtr date_column(size_t n) {
    auto column = ColumnDate::create();
    for (size_t i = 0; i < n; ++i) {
        VecDateTimeValue value;
        EXPECT_TRUE(value.from_date_int64(20200101 + static_cast<int64_t>(i % 28)));
        column->insert_value(value);
    }
    return column;
}

ColumnPtr datetime_column(size_t n) {
    auto column = ColumnDateTime::create();
    for (size_t i = 0; i < n; ++i) {
        VecDateTimeValue value;
        EXPECT_TRUE(value.from_date_int64(20200101000000 + static_cast<int64_t>(i % 59)));
        column->insert_value(value);
    }
    return column;
}

ColumnPtr string_column(size_t n, size_t max_length) {
    auto column = ColumnString::create();
    for (size_t i = 0; i < n; ++i) {
        std::string value = "v" + std::to_string(i);
        value.resize(std::min(max_length, 1 + (i % max_length)), 'x');
        column->insert_data(value.data(), value.size());
    }
    return column;
}

Status create_string_writer(FieldType type, ColumnMetaPB* meta, io::FileWriter* file,
                            std::unique_ptr<ColumnWriter>* writer) {
    meta->set_column_id(0);
    meta->set_unique_id(0);
    meta->set_type(int(type));
    meta->set_length(64);
    meta->set_encoding(PLAIN_ENCODING);
    meta->set_compression(NO_COMPRESSION);
    meta->set_is_nullable(true);
    ColumnWriterOptions options;
    options.meta = meta;
    TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, type, true);
    RETURN_IF_ERROR(ColumnWriter::create(options, &column, file, writer));
    return (*writer)->init();
}

void expect_batch_rejected_before_writing(ColumnWriter& writer, const IColumn& input,
                                          size_t row_pos, size_t num_rows) {
    EXPECT_FALSE(writer.append(input, row_pos, num_rows).ok());
    EXPECT_EQ(writer.get_next_rowid(), 0);
}

} // namespace

TEST(PageBuilderColumnTest, StringWriterValidatesOnlySelectedNonNullRows) {
    const auto saved_limit = config::string_type_length_soft_limit_bytes;
    config::string_type_length_soft_limit_bytes = 8;
    Defer restore {[&] { config::string_type_length_soft_limit_bytes = saved_limit; }};
    auto strings = ColumnString::create();
    for (const std::string value :
         {"too long to store", "ok", "ignored NULL payload", "tail", "too long to store"}) {
        strings->insert_data(value.data(), value.size());
    }
    auto nulls = ColumnUInt8::create();
    nulls->get_data().assign({0, 0, 1, 0, 0});
    auto input = ColumnNullable::create(std::move(strings), std::move(nulls));

    auto fs = io::global_local_filesystem();
    ASSERT_TRUE(fs->create_directory("./ut_dir/page_builder_column_test").ok());
    io::FileWriterPtr file;
    ASSERT_TRUE(fs->create_file("./ut_dir/page_builder_column_test/string_admission", &file).ok());
    ColumnMetaPB meta;
    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(create_string_writer(FieldType::OLAP_FIELD_TYPE_STRING, &meta, file.get(), &writer)
                        .ok());

    // A rejected later run must not leave the preceding values or NULLs written.
    expect_batch_rejected_before_writing(*writer, *input, 1, 4);
    ASSERT_TRUE(writer->append(*input, 1, 2).ok());
    EXPECT_EQ(writer->get_next_rowid(), 2);
    const auto status = writer->append(*input, 0, 1);
    EXPECT_NE(status.to_string().find("string_type_length_soft_limit_bytes"), std::string::npos)
            << status;
    EXPECT_EQ(writer->get_next_rowid(), 2);
    ASSERT_TRUE(writer->append(*input, 3, 1).ok());
    EXPECT_EQ(writer->get_next_rowid(), 3);
    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(file->close().ok());
}

TEST(PageBuilderColumnTest, JsonbWriterRejectsInvalidDocumentsAndSkipsNullPayloads) {
    JsonBinaryValue document;
    ASSERT_TRUE(document.from_json_string(R"({"key": [1, 2, 3]})").ok());
    auto strings = ColumnString::create();
    const std::string invalid = "not a jsonb document";
    strings->insert_data(invalid.data(), invalid.size());
    strings->insert_data(document.value(), document.size());
    strings->insert_data(invalid.data(), invalid.size());
    strings->insert_data(invalid.data(), invalid.size());
    auto nulls = ColumnUInt8::create();
    nulls->get_data().assign({0, 0, 1, 0});
    auto input = ColumnNullable::create(std::move(strings), std::move(nulls));

    auto fs = io::global_local_filesystem();
    ASSERT_TRUE(fs->create_directory("./ut_dir/page_builder_column_test").ok());
    io::FileWriterPtr file;
    ASSERT_TRUE(fs->create_file("./ut_dir/page_builder_column_test/jsonb_admission", &file).ok());
    ColumnMetaPB meta;
    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(create_string_writer(FieldType::OLAP_FIELD_TYPE_JSONB, &meta, file.get(), &writer)
                        .ok());

    expect_batch_rejected_before_writing(*writer, *input, 1, 3);
    ASSERT_TRUE(writer->append(*input, 1, 2).ok());
    EXPECT_EQ(writer->get_next_rowid(), 2);
    const auto status = writer->append(*input, 3, 1);
    EXPECT_NE(status.to_string().find("Invalid JSONB document"), std::string::npos) << status;
    EXPECT_EQ(writer->get_next_rowid(), 2);
    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(file->close().ok());
}

TEST(PageBuilderColumnTest, AlternatingNullStringsAcrossPages) {
    auto strings = ColumnString::create();
    auto nulls = ColumnUInt8::create();
    auto selected = ColumnString::create();
    for (size_t row = 0; row < 1100; ++row) {
        const auto value = std::to_string(row);
        const bool is_null = row % 2 == 0;
        strings->insert_data(value.data(), value.size());
        nulls->insert_value(is_null);
        if (row >= 3 && row < 1028 && !is_null) {
            selected->insert_data(value.data(), value.size());
        }
    }
    auto input = ColumnNullable::create(std::move(strings), std::move(nulls));
    auto fs = io::global_local_filesystem();
    ASSERT_TRUE(fs->create_directory("./ut_dir/page_builder_column_test").ok());
    io::FileWriterPtr file;
    ASSERT_TRUE(
            fs->create_file("./ut_dir/page_builder_column_test/alternating_strings", &file).ok());
    ColumnMetaPB meta;
    std::unique_ptr<ColumnWriter> column_writer;
    ASSERT_TRUE(create_string_writer(FieldType::OLAP_FIELD_TYPE_STRING, &meta, file.get(),
                                     &column_writer)
                        .ok());
    auto& writer = dynamic_cast<ScalarColumnWriter&>(*column_writer);
    const auto options = small_pages(64);
    writer._page_builder =
            make_builder<BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_STRING>>(options);
    writer._compress_codec = nullptr;
    ASSERT_TRUE(writer.append(*input, 3, 1025).ok());
    ASSERT_TRUE(writer.finish().ok());
    EXPECT_EQ(writer.get_next_rowid(), 1025);
    std::vector<std::string> actual;
    for (const auto& page : writer._pages) {
        actual.push_back(page->data.front().slice().to_string());
    }
    auto reference =
            make_builder<BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_STRING>>(options);
    const ReferenceCells<FieldType::OLAP_FIELD_TYPE_STRING> cells(*selected);
    const auto expected = drain(
            *reference, selected->size(),
            [&](size_t pos, size_t* n) { return cells.add(*reference, pos, n); }, 1);
    EXPECT_EQ(actual, expected.bytes);
    ASSERT_TRUE(file->close().ok());
}

TEST(PageBuilderColumnTest, AggregateStatesBorrowSerializedBatches) {
    auto counts = ColumnFixedLengthObject::create(sizeof(int64_t));
    auto strings = ColumnString::create();
    for (int64_t value = 0; value < 2500; ++value) {
        counts->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
        const auto bytes = std::to_string(value);
        strings->insert_data(bytes.data(), bytes.size());
    }
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_AGG_STATE,
                      BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_AGG_STATE>>(
            *counts, small_pages(128), 2, 2500);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_AGG_STATE,
                      BinaryPlainPageV3Builder<FieldType::OLAP_FIELD_TYPE_AGG_STATE>>(
            *strings, small_pages(128), 2, 2500, FieldType::OLAP_FIELD_TYPE_VARCHAR);
}

TEST(PageBuilderColumnTest, PlainInt) {
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_INT,
                      PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_INT>>(*int32_column(200),
                                                                        small_pages(256), 3);
}

TEST(PageBuilderColumnTest, PlainDateV1) {
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_DATE,
                      PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_DATE>>(*date_column(50),
                                                                         small_pages(64), 2);
}

TEST(PageBuilderColumnTest, LargeConvertedRunKeepsPageBoundaries) {
    auto dates = date_column(12000);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_DATE,
                      PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_DATE>>(*dates, small_pages(4096),
                                                                         3, 5000);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_DATE,
                      BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_DATE>>(
            *dates, small_pages(4096), 3, 5000);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_DATE,
                      PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_DATE>>(*dates, small_pages(3072),
                                                                         3, 5000);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_DATE,
                      FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_DATE>>(
            *dates, small_pages(64), 3, 5000);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_STRING,
                      BinaryPlainPageV3Builder<FieldType::OLAP_FIELD_TYPE_STRING>>(
            *string_column(12000, 12), small_pages(32768), 3, 5000);
}

TEST(PageBuilderColumnTest, BitshuffleFloatWithAndWithoutNaN) {
    auto column = ColumnFloat32::create();
    const auto payload_nan = std::bit_cast<float>(0x7FC12345U);
    for (size_t i = 0; i < 100; ++i) {
        column->insert_value(i % 9 == 0 ? payload_nan : static_cast<float>(i) * 0.5F);
    }
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_FLOAT,
                      BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_FLOAT>>(*column,
                                                                               small_pages(128), 2);

    auto without_nan = ColumnFloat32::create();
    for (size_t i = 0; i < 100; ++i) {
        without_nan->insert_value(static_cast<float>(i) * 0.5F);
    }
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_FLOAT,
                      BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_FLOAT>>(*without_nan,
                                                                               small_pages(128), 2);
}

TEST(PageBuilderColumnTest, BitshuffleDoubleBorrowsAndConvertsAcrossPages) {
    auto values = ColumnFloat64::create();
    auto with_nan = ColumnFloat64::create();
    const auto payload_nan = std::bit_cast<double>(uint64_t(0xFFF8000000000001ULL));
    for (size_t i = 0; i < 5000; ++i) {
        const double value = static_cast<double>(i) * 0.5;
        values->insert_value(value);
        with_nan->insert_value(i == 1030 || i == 4100 ? payload_nan : value);
    }
    // A batch with a NaN is converted into the writer's buffer, one without is
    // borrowed, and each page accepts only part of a batch. Both must keep the
    // remaining rows.
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_DOUBLE,
                      BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_DOUBLE>>(
            *with_nan, small_pages(128), 3, 5000);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_DOUBLE,
                      BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_DOUBLE>>(
            *values, small_pages(128), 3, 5000);
}

TEST(PageBuilderColumnTest, BitshuffleDateTimeV1AndDecimalV1) {
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_DATETIME,
                      BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_DATETIME>>(
            *datetime_column(70), small_pages(128), 2);

    auto decimals = ColumnDecimal128V2::create(0, 9);
    for (size_t i = 0; i < 40; ++i) {
        decimals->insert_value(DecimalV2Value(static_cast<int64_t>(i) - 20,
                                              static_cast<int64_t>(i) * 1000000 - 5000000));
    }
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_DECIMAL,
                      BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_DECIMAL>>(
            *decimals, small_pages(128), 2);
}

TEST(PageBuilderColumnTest, RleBool) {
    auto column = ColumnUInt8::create();
    for (size_t i = 0; i < 300; ++i) {
        column->insert_value((i / 17) % 2);
    }
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_BOOL,
                      RlePageBuilder<FieldType::OLAP_FIELD_TYPE_BOOL>>(*column, small_pages(16), 2);
}

TEST(PageBuilderColumnTest, FrameOfReferenceBigIntAndDateV1) {
    auto bigints = ColumnInt64::create();
    for (size_t i = 0; i < 300; ++i) {
        bigints->insert_value(static_cast<int64_t>(i) * 1000 + 12345);
    }
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_BIGINT,
                      FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_BIGINT>>(
            *bigints, small_pages(64), 2);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_DATE,
                      FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_DATE>>(
            *date_column(300), small_pages(64), 2);
}

TEST(PageBuilderColumnTest, BinaryPlainStrings) {
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_VARCHAR,
                      BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_VARCHAR>>(
            *string_column(60, 12), small_pages(96), 3);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_CHAR,
                      BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_CHAR>>(
            *string_column(60, 8), small_pages(96), 3);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_STRING,
                      BinaryPlainPageV2Builder<FieldType::OLAP_FIELD_TYPE_STRING>>(
            *string_column(60, 12), small_pages(96), 3);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_CHAR,
                      BinaryPlainPageV3Builder<FieldType::OLAP_FIELD_TYPE_CHAR>>(
            *string_column(60, 8), small_pages(96), 3);
}

TEST(PageBuilderColumnTest, BinaryPlainObjects) {
    auto bitmaps = ColumnBitmap::create();
    auto hlls = ColumnHLL::create();
    for (size_t i = 0; i < 30; ++i) {
        BitmapValue bitmap;
        for (size_t j = 0; j <= i % 5; ++j) {
            bitmap.add(i * 10 + j);
        }
        bitmaps->insert_value(bitmap);
        hlls->insert_value(HyperLogLog(static_cast<uint64_t>(i * 31)));
    }
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_BITMAP,
                      BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_BITMAP>>(
            *bitmaps, small_pages(128), 2);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_BITMAP,
                      BinaryPlainPageV2Builder<FieldType::OLAP_FIELD_TYPE_BITMAP>>(
            *bitmaps, small_pages(128), 2);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_BITMAP,
                      BinaryPlainPageV3Builder<FieldType::OLAP_FIELD_TYPE_BITMAP>>(
            *bitmaps, small_pages(128), 2);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_HLL,
                      BinaryPlainPageV3Builder<FieldType::OLAP_FIELD_TYPE_HLL>>(
            *hlls, small_pages(128), 2);
}

TEST(PageBuilderColumnTest, DictionaryOverflowFallsBackToPlain) {
    PageBuilderOptions options = small_pages(256);
    options.dict_page_size = 128;
    // Few distinct values first, then many: the dictionary page fills up and the
    // builder falls back to plain pages of this column's own cells.
    auto column = ColumnString::create();
    for (size_t i = 0; i < 200; ++i) {
        const std::string value = i < 100 ? "k" + std::to_string(i % 4) : "u" + std::to_string(i);
        column->insert_data(value.data(), value.size());
    }
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_CHAR, BinaryDictPageBuilder>(*column, options, 2);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_VARCHAR, BinaryDictPageBuilder>(*column, options,
                                                                                 2);
}

TEST(PageBuilderColumnTest, PrefixStrings) {
    auto column = ColumnString::create();
    for (size_t i = 0; i < 120; ++i) {
        const std::string value = "prefix-" + std::to_string(1000 + i);
        column->insert_data(value.data(), value.size());
    }
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_VARCHAR, BinaryPrefixPageBuilder>(
            *column, small_pages(128), 2);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_CHAR, BinaryPrefixPageBuilder>(
            *column, small_pages(128), 2);
}

} // namespace doris::segment_v2
