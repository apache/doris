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

#include "olap/rowset/segment_v2/bloom_filter_page.h"

#include <gtest/gtest.h>

#include <memory>

#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/logging.h"

using doris::segment_v2::PageBuilderOptions;
using doris::segment_v2::PageDecoderOptions;

namespace doris {

namespace segment_v2 {

class BloomFilterPageTest : public testing::Test {
public:
    virtual ~BloomFilterPageTest() {}

    template <FieldType Type, class PageBuilderType>
    void test_encode_decode_page_template(typename TypeTraits<Type>::CppType* src, size_t size,
                                          bool has_null, bool is_slice_type = false) {
        typedef typename TypeTraits<Type>::CppType CppType;
        PageBuilderOptions builder_options;
        builder_options.data_page_size = 256 * 1024;
        PageBuilderType bf_page_builder(builder_options);
        EXPECT_FALSE(bf_page_builder.is_page_full());
        bf_page_builder.add(reinterpret_cast<const uint8_t*>(src), &size);
        if (has_null) {
            size_t num = 1;
            bf_page_builder.add(nullptr, &num);
        }
        OwnedSlice s = bf_page_builder.finish();
        EXPECT_EQ(size + has_null, bf_page_builder.count());

        BloomFilterPageDecoder bf_page_decoder(s.slice());
        auto status = bf_page_decoder.init();
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(0, bf_page_decoder.current_index());
        EXPECT_EQ(1, bf_page_decoder.count());
        status = bf_page_decoder.seek_to_position_in_page(0);
        EXPECT_TRUE(status.ok());

        auto tracker = std::make_shared<MemTracker>();
        MemPool pool(tracker.get());
        Slice* values = reinterpret_cast<Slice*>(pool.allocate(sizeof(Slice)));
        ColumnBlock block(get_type_info(OLAP_FIELD_TYPE_VARCHAR), (uint8_t*)values, nullptr, 2,
                          &pool);
        ColumnBlockView column_block_view(&block);
        size_t size_to_fetch = 1;
        status = bf_page_decoder.next_batch(&size_to_fetch, &column_block_view);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(1, size_to_fetch);

        std::unique_ptr<BloomFilter> bf;
        BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
        EXPECT_NE(nullptr, bf);
        auto ret = bf->init(values->data, values->size, HASH_MURMUR3_X64_64);
        EXPECT_TRUE(ret);
        EXPECT_EQ(has_null, bf->has_null());
        for (size_t i = 0; i < size; ++i) {
            if (is_slice_type) {
                Slice* value = (Slice*)(src + i);
                EXPECT_TRUE(bf->test_bytes(value->data, value->size));
            } else {
                EXPECT_TRUE(bf->test_bytes((char*)(src + i), sizeof(CppType)));
            }
        }
    }
};

// Test for rle block, for INT32, BOOL
TEST_F(BloomFilterPageTest, TestIntFieldBloomFilterPage) {
    const uint32_t size = 1024;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = random();
    }

    // without null
    test_encode_decode_page_template<OLAP_FIELD_TYPE_INT,
                                     segment_v2::BloomFilterPageBuilder<OLAP_FIELD_TYPE_INT>>(
            ints.get(), size, false);
    // with null
    test_encode_decode_page_template<OLAP_FIELD_TYPE_INT,
                                     segment_v2::BloomFilterPageBuilder<OLAP_FIELD_TYPE_INT>>(
            ints.get(), size, true);
}

TEST_F(BloomFilterPageTest, TestBigIntFieldBloomFilterPage) {
    const uint32_t size = 1024;

    std::unique_ptr<int64_t[]> big_ints(new int64_t[size]);
    for (int i = 0; i < size; i++) {
        big_ints.get()[i] = random();
    }

    // without null
    test_encode_decode_page_template<OLAP_FIELD_TYPE_BIGINT,
                                     segment_v2::BloomFilterPageBuilder<OLAP_FIELD_TYPE_BIGINT>>(
            big_ints.get(), size, false);
    // with null
    test_encode_decode_page_template<OLAP_FIELD_TYPE_BIGINT,
                                     segment_v2::BloomFilterPageBuilder<OLAP_FIELD_TYPE_BIGINT>>(
            big_ints.get(), size, true);
}

TEST_F(BloomFilterPageTest, TestVarcharFieldBloomFilterPage) {
    const uint32_t size = 1024;

    std::vector<std::string> strings;
    strings.resize(size);
    for (int i = 0; i < size; ++i) {
        strings.push_back("prefix_" + std::to_string(random()));
    }

    std::unique_ptr<Slice[]> slices(new Slice[size]);
    for (int i = 0; i < size; i++) {
        slices.get()[i] = Slice(strings[i]);
    }

    // without null
    test_encode_decode_page_template<OLAP_FIELD_TYPE_VARCHAR,
                                     segment_v2::BloomFilterPageBuilder<OLAP_FIELD_TYPE_VARCHAR>>(
            slices.get(), size, false, true);
    // with null
    test_encode_decode_page_template<OLAP_FIELD_TYPE_VARCHAR,
                                     segment_v2::BloomFilterPageBuilder<OLAP_FIELD_TYPE_VARCHAR>>(
            slices.get(), size, true, true);
}

TEST_F(BloomFilterPageTest, TestCharFieldBloomFilterPage) {
    const uint32_t size = 1024;

    std::vector<std::string> strings;
    strings.resize(size);
    for (int i = 0; i < size; ++i) {
        strings.push_back("prefix_" + std::to_string(i % 10));
    }

    std::unique_ptr<Slice[]> slices(new Slice[size]);
    for (int i = 0; i < size; i++) {
        slices.get()[i] = Slice(strings[i]);
    }

    // without null
    test_encode_decode_page_template<OLAP_FIELD_TYPE_CHAR,
                                     segment_v2::BloomFilterPageBuilder<OLAP_FIELD_TYPE_CHAR>>(
            slices.get(), size, false, true);
    // with null
    test_encode_decode_page_template<OLAP_FIELD_TYPE_CHAR,
                                     segment_v2::BloomFilterPageBuilder<OLAP_FIELD_TYPE_CHAR>>(
            slices.get(), size, true, true);
}

} // namespace segment_v2
} // namespace doris
