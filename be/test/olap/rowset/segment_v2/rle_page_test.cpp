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
#include <memory>

#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/rowset/segment_v2/rle_page.h"
#include "util/arena.h"
#include "util/logging.h"

using doris::segment_v2::PageBuilderOptions;
using doris::segment_v2::PageDecoderOptions;

namespace doris {

class RlePageTest : public testing::Test {
public:
    virtual ~RlePageTest() { }

    template<FieldType type, class PageDecoderType>
    void copy_one(PageDecoderType* decoder, typename TypeTraits<type>::CppType* ret) {
        Arena arena;
        uint8_t null_bitmap = 0;
        ColumnBlock block(get_type_info(type), (uint8_t*)ret, &null_bitmap, &arena);
        ColumnBlockView column_block_view(&block);

        size_t n = 1;
        decoder->next_batch(&n, &column_block_view);
        ASSERT_EQ(1, n);
    }

    template <FieldType Type, class PageBuilderType, class PageDecoderType>
    void test_encode_decode_page_template(typename TypeTraits<Type>::CppType* src,
            size_t size) {
        typedef typename TypeTraits<Type>::CppType CppType;
        PageBuilderOptions builder_options;
        builder_options.data_page_size = 256 * 1024;
        PageBuilderType rle_page_builder(builder_options);
        rle_page_builder.add(reinterpret_cast<const uint8_t *>(src), &size);
        Slice s = rle_page_builder.finish();
        ASSERT_EQ(size, rle_page_builder.count());
        LOG(INFO) << "RLE Encoded size for 10k values: " << s.size
                << ", original size:" << size * sizeof(CppType);

        PageDecoderOptions decodeder_options;
        PageDecoderType rle_page_decoder(s, decodeder_options);
        Status status = rle_page_decoder.init();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(0, rle_page_decoder.current_index());
        ASSERT_EQ(size, rle_page_decoder.count());

        Arena arena;

        CppType* values = reinterpret_cast<CppType*>(arena.Allocate(size * sizeof(CppType)));
        uint8_t* null_bitmap = reinterpret_cast<uint8_t*>(arena.Allocate(BitmapSize(size)));
        ColumnBlock block(get_type_info(Type), (uint8_t*)values, null_bitmap, &arena);
        ColumnBlockView column_block_view(&block);
        size_t size_to_fetch = size;
        status = rle_page_decoder.next_batch(&size_to_fetch, &column_block_view);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(size, size_to_fetch);

        for (uint i = 0; i < size; i++) {
            if (src[i] != values[i]) {
                FAIL() << "Fail at index " << i <<
                    " inserted=" << src[i] << " got=" << values[i];
            }
        }

        // Test Seek within block by ordinal
        for (int i = 0; i < 100; i++) {
            int seek_off = random() % size;
            rle_page_decoder.seek_to_position_in_page(seek_off);
            EXPECT_EQ((int32_t )(seek_off), rle_page_decoder.current_index());
            CppType ret;
            copy_one<Type, PageDecoderType>(&rle_page_decoder, &ret);
            EXPECT_EQ(values[seek_off], ret);
        }
    }
};

// Test for rle block, for INT32, BOOL
TEST_F(RlePageTest, TestRleInt32BlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = random();
    }

    test_encode_decode_page_template<OLAP_FIELD_TYPE_INT, segment_v2::RlePageBuilder<OLAP_FIELD_TYPE_INT>,
        segment_v2::RlePageDecoder<OLAP_FIELD_TYPE_INT> >(ints.get(), size);
}

TEST_F(RlePageTest, TestRleInt32BlockEncoderEqual) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 12345;
    }

    test_encode_decode_page_template<OLAP_FIELD_TYPE_INT, segment_v2::RlePageBuilder<OLAP_FIELD_TYPE_INT>,
        segment_v2::RlePageDecoder<OLAP_FIELD_TYPE_INT> >(ints.get(), size);
}

TEST_F(RlePageTest, TestRleInt32BlockEncoderSequence) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 12345 + i;
    }

    test_encode_decode_page_template<OLAP_FIELD_TYPE_INT, segment_v2::RlePageBuilder<OLAP_FIELD_TYPE_INT>,
        segment_v2::RlePageDecoder<OLAP_FIELD_TYPE_INT> >(ints.get(), size);
}

TEST_F(RlePageTest, TestRleInt32BlockEncoderSize) {
    size_t size = 100;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 0;
    }
    PageBuilderOptions builder_options;
    builder_options.data_page_size = 256 * 1024;
    segment_v2::RlePageBuilder<OLAP_FIELD_TYPE_INT> rle_page_builder(builder_options);
    rle_page_builder.add(reinterpret_cast<const uint8_t *>(ints.get()), &size);
    Slice s = rle_page_builder.finish();
    // 4 bytes header
    // 2 bytes indicate_value(): 0x64 << 1 | 1 = 201
    // 4 bytes values
    ASSERT_EQ(10, s.size);
}

TEST_F(RlePageTest, TestRleBoolBlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<bool[]> bools(new bool[size]);
    for (int i = 0; i < size; i++) {
        if (random() % 2 == 0) {
            bools.get()[i] = true;
        } else {
            bools.get()[i] = false;
        }
    }

    test_encode_decode_page_template<OLAP_FIELD_TYPE_BOOL, segment_v2::RlePageBuilder<OLAP_FIELD_TYPE_BOOL>,
        segment_v2::RlePageDecoder<OLAP_FIELD_TYPE_BOOL> >(bools.get(), size);
}

TEST_F(RlePageTest, TestRleBoolBlockEncoderSize) {
    size_t size = 100;

    std::unique_ptr<bool[]> bools(new bool[size]);
    for (int i = 0; i < size; i++) {
        bools.get()[i] = true;
    }
    PageBuilderOptions builder_options;
    builder_options.data_page_size = 256 * 1024;
    segment_v2::RlePageBuilder<OLAP_FIELD_TYPE_BOOL> rle_page_builder(builder_options);
    rle_page_builder.add(reinterpret_cast<const uint8_t *>(bools.get()), &size);
    Slice s = rle_page_builder.finish();
    // 4 bytes header
    // 2 bytes indicate_value(): 0x64 << 1 | 1 = 201
    // 1 bytes values
    ASSERT_EQ(7, s.size);
}

}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
