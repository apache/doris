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

#include "common/logging.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/olap_common.h"
#include "olap/types.h"

namespace doris {
namespace segment_v2 {

class BinaryPlainPageTest : public testing::Test {
public:
    BinaryPlainPageTest() {}

    virtual ~BinaryPlainPageTest() {
    }

    template <class PageBuilderType, class PageDecoderType>
    void TestBinarySeekByValueSmallPage() {
        vector<Slice> slices;
        slices.emplace_back("Hello");
        slices.emplace_back(",");
        slices.emplace_back("Doris");
        
        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        PageBuilderType page_builder(options);
        size_t count = slices.size();

        Slice *ptr = &slices[0];
        Status ret = page_builder.add(reinterpret_cast<const uint8_t *>(ptr), &count);
        
        Slice s = page_builder.finish();
        PageDecoderOptions decoder_options;
        PageDecoderType page_decoder(s, decoder_options);
        Status status = page_decoder.init();
        ASSERT_TRUE(status.ok());
        
        //test1
        std::unique_ptr<ColumnVector> dst_vector(new ColumnVector());
        std::unique_ptr<MemTracker> mem_tracer(new MemTracker(-1));
        std::unique_ptr<MemPool> mem_pool(new MemPool(mem_tracer.get()));
        
        size_t size = 3;
        
        Slice* values = reinterpret_cast<Slice*>(mem_pool->allocate(size * sizeof(Slice)));
        dst_vector->set_col_data(values);
        ColumnVectorView column_vector_view(dst_vector.get(), 0, mem_pool.get());

        status = page_decoder.next_batch(&size, &column_vector_view);
        ASSERT_TRUE(status.ok());

        Slice* value = reinterpret_cast<Slice*>(dst_vector->col_data());
        ASSERT_EQ (3, size);
        ASSERT_EQ ("Hello", value[0].to_string());
        ASSERT_EQ (",", value[1].to_string());
        ASSERT_EQ ("Doris", value[2].to_string());
        
        Slice* values2 = reinterpret_cast<Slice*>(mem_pool->allocate(size * sizeof(Slice)));
        dst_vector->set_col_data(values2);
        ColumnVectorView column_vector_view2(dst_vector.get(), 0, mem_pool.get());
        size_t fetch_num = 1;
        page_decoder.seek_to_position_in_page(2);
        status = page_decoder.next_batch(&fetch_num, &column_vector_view2);
        ASSERT_TRUE(status.ok());
        Slice* value2 = reinterpret_cast<Slice*>(dst_vector->col_data());
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
