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
#include <iostream>
#include <vector>

#include "common/logging.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/olap_common.h"
#include "olap/types.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"

namespace doris {
namespace segment_v2 {

class BinaryPlainPageTest : public testing::Test {
public:
    BinaryPlainPageTest() {}

    virtual ~BinaryPlainPageTest() {
    }

    template <class PageBuilderType, class PageDecoderType>
    void TestBinarySeekByValueSmallPage() {
        std::vector<Slice> slices;
        slices.emplace_back("Hello");
        slices.emplace_back(",");
        slices.emplace_back("Doris");
        
        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        PageBuilderType page_builder(options);
        size_t count = slices.size();

        Slice *ptr = &slices[0];
        Status ret = page_builder.add(reinterpret_cast<const uint8_t *>(ptr), &count);

        OwnedSlice owned_slice = page_builder.finish();

        //check first value and last value
        Slice first_value;
        page_builder.get_first_value(&first_value);
        ASSERT_EQ(slices[0], first_value);
        Slice last_value;
        page_builder.get_last_value(&last_value);
        ASSERT_EQ(slices[count - 1], last_value);

        PageDecoderOptions decoder_options;
        PageDecoderType page_decoder(owned_slice.slice(), decoder_options);
        Status status = page_decoder.init();
        ASSERT_TRUE(status.ok());

        //test1
        auto tracker = std::make_shared<MemTracker>();
        MemPool pool(tracker.get());
        size_t size = 3;
        Slice* values = reinterpret_cast<Slice*>(pool.allocate(size * sizeof(Slice)));
        uint8_t* null_bitmap = reinterpret_cast<uint8_t*>(pool.allocate(BitmapSize(size)));
        ColumnBlock block(get_type_info(OLAP_FIELD_TYPE_VARCHAR), (uint8_t*)values, null_bitmap, size, &pool);
        ColumnBlockView column_block_view(&block);

        status = page_decoder.next_batch(&size, &column_block_view);
        ASSERT_TRUE(status.ok());

        Slice* value = reinterpret_cast<Slice*>(values);
        ASSERT_EQ (3, size);
        ASSERT_EQ ("Hello", value[0].to_string());
        ASSERT_EQ (",", value[1].to_string());
        ASSERT_EQ ("Doris", value[2].to_string());
        
        Slice* values2 = reinterpret_cast<Slice*>(pool.allocate(size * sizeof(Slice)));
        ColumnBlock block2(get_type_info(OLAP_FIELD_TYPE_VARCHAR), (uint8_t*)values2, null_bitmap, 1, &pool);
        ColumnBlockView column_block_view2(&block2);

        size_t fetch_num = 1;
        page_decoder.seek_to_position_in_page(2);
        status = page_decoder.next_batch(&fetch_num, &column_block_view2);
        ASSERT_TRUE(status.ok());
        Slice* value2 = reinterpret_cast<Slice*>(values2);
        ASSERT_EQ (1, fetch_num);
        ASSERT_EQ ("Doris", value2[0].to_string());
    }
};

TEST_F(BinaryPlainPageTest, TestBinaryPlainPageBuilderSeekByValueSmallPage) {
    TestBinarySeekByValueSmallPage<BinaryPlainPageBuilder, BinaryPlainPageDecoder>();
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
