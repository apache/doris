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

} // namespace segment_v2
} // namespace doris
