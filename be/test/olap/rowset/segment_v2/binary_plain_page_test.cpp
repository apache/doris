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

#include "olap/rowset/segment_v2/binary_plain_page.h"

#include <gtest/gtest.h>

#include <iostream>
#include <vector>

#include "common/logging.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/types.h"

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
        PageBuilderType page_builder(options);
        Status ret0 = page_builder.init();
        EXPECT_TRUE(ret0.ok());
        size_t count = slices.size();

        Slice* ptr = &slices[0];
        Status ret = page_builder.add(reinterpret_cast<const uint8_t*>(ptr), &count);
        EXPECT_TRUE(ret.ok());

        OwnedSlice owned_slice = page_builder.finish();

        //check first value and last value
        Slice first_value;
        page_builder.get_first_value(&first_value);
        EXPECT_EQ(slices[0], first_value);
        Slice last_value;
        page_builder.get_last_value(&last_value);
        EXPECT_EQ(slices[count - 1], last_value);

        PageDecoderOptions decoder_options;
        PageDecoderType page_decoder(owned_slice.slice(), decoder_options);
        Status status = page_decoder.init();
        EXPECT_TRUE(status.ok());

        //test1
        vectorized::Arena pool;
        size_t size = 3;
        std::unique_ptr<ColumnVectorBatch> cvb;
        ColumnVectorBatch::create(size, true,
                                  get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_VARCHAR), nullptr,
                                  &cvb);
        ColumnBlock block(cvb.get(), &pool);
        ColumnBlockView column_block_view(&block);

        status = page_decoder.next_batch(&size, &column_block_view);
        Slice* values = reinterpret_cast<Slice*>(block.data());
        EXPECT_TRUE(status.ok());

        Slice* value = reinterpret_cast<Slice*>(values);
        EXPECT_EQ(3, size);
        EXPECT_EQ("Hello", value[0].to_string());
        EXPECT_EQ(",", value[1].to_string());
        EXPECT_EQ("Doris", value[2].to_string());

        std::unique_ptr<ColumnVectorBatch> cvb2;
        ColumnVectorBatch::create(1, true, get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_VARCHAR),
                                  nullptr, &cvb2);
        ColumnBlock block2(cvb2.get(), &pool);
        ColumnBlockView column_block_view2(&block2);

        size_t fetch_num = 1;
        page_decoder.seek_to_position_in_page(2);
        status = page_decoder.next_batch(&fetch_num, &column_block_view2);
        Slice* values2 = reinterpret_cast<Slice*>(block2.data());
        EXPECT_TRUE(status.ok());
        Slice* value2 = reinterpret_cast<Slice*>(values2);
        EXPECT_EQ(1, fetch_num);
        EXPECT_EQ("Doris", value2[0].to_string());
    }
};

TEST_F(BinaryPlainPageTest, TestBinaryPlainPageBuilderSeekByValueSmallPage) {
    TestBinarySeekByValueSmallPage<BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_VARCHAR>,
                                   BinaryPlainPageDecoder<FieldType::OLAP_FIELD_TYPE_VARCHAR>>();
}

} // namespace segment_v2
} // namespace doris
