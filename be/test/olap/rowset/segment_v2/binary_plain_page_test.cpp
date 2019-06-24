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
    /*
    PageBuilderOptions* new_builder_options() {
            auto ret = new PageBuilderOptions();
            ret->data_page_size = 256 * 1024;
            return ret;
    }
    
    template<FieldType type, class PageDecoderType>
    void copy_one(PageDecoderType* decoder, typename TypeTraits<type>::CppType* ret) {
        std::unique_ptr<ColumnVector> dst_vector(new ColumnVector());
        dst_vector->set_col_data((void*)ret);
        std::unique_ptr<MemTracker> mem_tracer(new MemTracker(-1)); 
        std::unique_ptr<MemPool> mem_pool(new MemPool(mem_tracer.get()));
        ColumnVectorView column_vector_view(dst_vector.get(), 0, mem_pool.get());
        size_t n = 1;
        decoder->next_batch(&n, &column_vector_view);
        ASSERT_EQ(1, n);
    }

    template <FieldType Type, class PageBuilderType, class PageDecoderType>
    void test_encode_decode_page_template(typename TypeTraits<Type>::CppType* src,
                    size_t size) {
        typedef typename TypeTraits<Type>::CppType CppType;
        const size_t ordinal_pos_base = 12345;
        
        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        PageBuilderType page_builder(options);

        page_builder.add(reinterpret_cast<const uint8_t *>(src), &size);
        Slice s = page_builder.finish(ordinal_pos_base);
    
        PageDecoderType page_decoder(s);
        Status status = page_decoder.init();
        ASSERT_TRUE(status.ok());
        
        ASSERT_EQ(ordinal_pos_base, page_decoder.get_first_rowid());
        ASSERT_EQ(0, page_decoder.current_index());
        
        std::unique_ptr<ColumnVector> dst_vector(new ColumnVector());
        std::unique_ptr<MemTracker> mem_tracer(new MemTracker(-1));
        std::unique_ptr<MemPool> mem_pool(new MemPool(mem_tracer.get()));
        CppType* values = reinterpret_cast<CppType*>(mem_pool->allocate(size * sizeof(CppType)));
        dst_vector->set_col_data(values);
        ColumnVectorView column_vector_view(dst_vector.get(), 0, mem_pool.get());
        status = page_decoder.next_batch(&size, &column_vector_view);
        ASSERT_TRUE(status.ok());
    
        CppType* decoded = (CppType*)dst_vector->col_data();
        for (uint i = 0; i < size; i++) {
            if (src[i] != decoded[i]) {
                FAIL() << "Fail at index " << i <<
                        " inserted=" << src[i] << " got=" << decoded[i];
                }
        }
    
        // Test Seek within block by ordinal
        for (int i = 0; i < 100; i++) {
            int seek_off = random() % size;
            page_decoder.seek_to_position_in_page(seek_off);
            EXPECT_EQ((int32_t )(seek_off), page_decoder.current_index());
            CppType ret;
            copy_one<Type, PageDecoderType>(&page_decoder, &ret);
            EXPECT_EQ(decoded[seek_off], ret);
       }
    }
    */

    
    template <class PageBuilderType, class PageDecoderType>
    void TestBinarySeekByValueSmallBlock() {
        vector<Slice> slices;
        slices.emplace_back("Hello");
        slices.emplace_back(",");
        slices.emplace_back("Doris");
        
        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        PageBuilderType page_builder(options);
        size_t count = slices.size();
        std::cout<< count <<std::endl;

        Slice *ptr = &slices[0];
        Status ret = page_builder.add(reinterpret_cast<const uint8_t *>(ptr), &count);
        
        Slice s = page_builder.finish(0);
        PageDecoderType page_decoder(s);
        Status status = page_decoder.init();
        ASSERT_TRUE(status.ok());
        
        
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
        std::cout<<"size : " << size << ","<<value->size <<" , string : " <<value->to_string()<<std::endl;
    }
};
/*
TEST_F(BinaryPlainPageTest, TestInt32PlainPageRandom) {
    const uint32_t size = 10000;
    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = random();
    }

    test_encode_decode_page_template<OLAP_FIELD_TYPE_INT, segment_v2::BinaryPlainPageBuilder<OLAP_FIELD_TYPE_INT>,
        segment_v2::BinaryPlainPageDecoder<OLAP_FIELD_TYPE_INT> >(ints.get(), size);
}
*/

TEST_F(BinaryPlainPageTest, TestBinaryPlainPageBuilderSeekByValueSmallBlock) {
    TestBinarySeekByValueSmallBlock<BinaryPlainPageBuilder, BinaryPlainPageDecoder>();
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


