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

#include "storage/segment/binary_plain_page.h"

#include <gtest/gtest.h>

#include <iostream>
#include <vector>

#include "common/logging.h"
#include "core/column/column_string.h"
#include "storage/olap_common.h"
#include "storage/segment/page_builder.h"
#include "storage/segment/page_decoder.h"
#include "storage/storage_layout.h"
#include "storage/types.h"

namespace doris {
namespace segment_v2 {

class BinaryPlainPageTest : public testing::Test {
public:
    BinaryPlainPageTest() {}

    virtual ~BinaryPlainPageTest() {}

    template <class PageBuilderType, class PageDecoderType>
    void TestBinarySeekByValueSmallPage() {
        std::vector<Slice> slices;
        slices.emplace_back("Hello");
        slices.emplace_back(",");
        slices.emplace_back("Doris");

        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        PageBuilder* builder = nullptr;
        ASSERT_TRUE(PageBuilderType::create(&builder, options).ok());
        std::unique_ptr<PageBuilder> page_builder(builder);
        size_t count = slices.size();

        Slice* ptr = &slices[0];
        Status ret = page_builder->add(reinterpret_cast<const uint8_t*>(ptr), &count);
        EXPECT_TRUE(ret.ok());

        OwnedSlice owned_slice;
        EXPECT_TRUE(page_builder->finish(&owned_slice).ok());

        PageDecoderOptions decoder_options;
        PageDecoderType page_decoder(owned_slice.slice(), decoder_options);
        Status status = page_decoder.init();
        EXPECT_TRUE(status.ok());

        //test1
        size_t size = 3;
        MutableColumnPtr column = ColumnString::create();
        status = page_decoder.next_batch(&size, column);
        EXPECT_TRUE(status.ok());

        EXPECT_EQ(3, size);
        EXPECT_EQ("Hello", column->get_data_at(0).to_string());
        EXPECT_EQ(",", column->get_data_at(1).to_string());
        EXPECT_EQ("Doris", column->get_data_at(2).to_string());

        MutableColumnPtr column2 = ColumnString::create();
        size_t fetch_num = 1;
        EXPECT_TRUE(page_decoder.seek_to_position_in_page(2).ok());
        status = page_decoder.next_batch(&fetch_num, column2);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(1, fetch_num);
        EXPECT_EQ("Doris", column2->get_data_at(0).to_string());
    }
};

TEST_F(BinaryPlainPageTest, TestBinaryPlainPageBuilderSeekByValueSmallPage) {
    TestBinarySeekByValueSmallPage<BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_VARCHAR>,
                                   BinaryPlainPageDecoder<FieldType::OLAP_FIELD_TYPE_VARCHAR>>();
}

static void check_segment_bitmap(const BitmapValue& row, const Slice& expected) {
    EXPECT_FALSE(row.needs_optimization());
    std::string bytes(row.getSizeInBytes(), '\0');
    EXPECT_EQ(row.write_to(bytes.data()), bytes.size());
    EXPECT_EQ(bytes, expected.to_string());
    BitmapValue external;
    ASSERT_TRUE(external.deserialize(bytes.data()));
    EXPECT_TRUE(external.needs_optimization());
}

TEST_F(BinaryPlainPageTest, BitmapSegmentReadsPreserveOptimizedRepresentation) {
    constexpr auto type = FieldType::OLAP_FIELD_TYPE_BITMAP;
    auto source = ColumnBitmap::create();
    for (uint64_t base : {uint64_t {0}, uint64_t {1} << 40}) {
        auto& bitmap = source->get_data().emplace_back();
        for (uint64_t i = 0; i < 1024; ++i) {
            bitmap.add(base + i);
        }
    }
    StorageBuffers buffers;
    const auto* cells = StorageLayout<type>::column_to_storage(*source, 0, 2, buffers);
    PageBuilderOptions options;
    PageBuilder* builder = nullptr;
    ASSERT_TRUE(BinaryPlainPageBuilder<type>::create(&builder, options).ok());
    std::unique_ptr<PageBuilder> page(builder);
    size_t count = 2;
    ASSERT_TRUE(page->add(cells, &count).ok());
    ASSERT_EQ(count, 2);
    OwnedSlice encoded;
    ASSERT_TRUE(page->finish(&encoded).ok());
    BinaryPlainPageDecoder<type> decoder(encoded.slice());
    ASSERT_TRUE(decoder.init().ok());

    MutableColumnPtr dst = ColumnBitmap::create();
    ASSERT_TRUE(decoder.next_batch(&count, dst).ok());
    ASSERT_EQ(count, 2);
    rowid_t ids[] = {1};
    count = 1;
    ASSERT_TRUE(decoder.read_by_rowids(ids, 0, &count, dst).ok());
    ASSERT_EQ(count, 1);
    auto& rows = assert_cast<ColumnBitmap&>(*dst).get_data();
    ASSERT_EQ(rows.size(), 3);
    check_segment_bitmap(rows[0], buffers.cells[0]);
    check_segment_bitmap(rows[1], buffers.cells[1]);
    check_segment_bitmap(rows[2], buffers.cells[1]);
    rows[2].remove(uint64_t {1} << 40);
    EXPECT_TRUE(rows[2].needs_optimization());
}

} // namespace segment_v2
} // namespace doris
