
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/rowset/segment_v2/plain_page.h"

#include <gtest/gtest.h>
#include <iostream>

#include "common/logging.h"
#include "olap/olap_common.h"
#include "olap/types.h"

namespace doris {
namespace segment_v2 {

class PlainPageTest : public testing::Test {
public:
    PlainPageTest() {}

    virtual ~PlainPageTest() {
    }

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
};

TEST_F(PlainPageTest, TestInt32PlainPageRandom) {
    const uint32_t size = 10000;
    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = random();
    }

    test_encode_decode_page_template<OLAP_FIELD_TYPE_INT, segment_v2::PlainPageBuilder<OLAP_FIELD_TYPE_INT>,
        segment_v2::PlainPageDecoder<OLAP_FIELD_TYPE_INT> >(ints.get(), size);
}

TEST_F(PlainPageTest, TestInt64PlainPageRandom) {
    const uint32_t size = 10000;            
    std::unique_ptr<int64_t[]> ints(new int64_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = random();                                    
    }                    

    test_encode_decode_page_template<OLAP_FIELD_TYPE_BIGINT, segment_v2::PlainPageBuilder<OLAP_FIELD_TYPE_BIGINT>,
        segment_v2::PlainPageDecoder<OLAP_FIELD_TYPE_BIGINT> >(ints.get(), size);                            
}

TEST_F(PlainPageTest, TestPlainFloatBlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<float[]> floats(new float[size]);
    for (int i = 0; i < size; i++) {
        floats.get()[i] = random() + static_cast<float>(random())/INT_MAX;
    }

    test_encode_decode_page_template<OLAP_FIELD_TYPE_FLOAT, segment_v2::PlainPageBuilder<OLAP_FIELD_TYPE_FLOAT>,
        segment_v2::PlainPageDecoder<OLAP_FIELD_TYPE_FLOAT> >(floats.get(), size);
}

TEST_F(PlainPageTest, TestDoublePageEncoderRandom) {
    const uint32_t size = 10000;
    std::unique_ptr<double[]> doubles(new double[size]);
    for (int i = 0; i < size; i++) {
        doubles.get()[i] = random() + static_cast<double>(random())/INT_MAX;                                    
    }
    test_encode_decode_page_template<OLAP_FIELD_TYPE_DOUBLE, segment_v2::PlainPageBuilder<OLAP_FIELD_TYPE_DOUBLE>,
        segment_v2::PlainPageDecoder<OLAP_FIELD_TYPE_DOUBLE> >(doubles.get(), size);                        
}

TEST_F(PlainPageTest, TestDoublePageEncoderEqual) {
    const uint32_t size = 10000;
    
    std::unique_ptr<double[]> doubles(new double[size]);
    for (int i = 0; i < size; i++) {
        doubles.get()[i] = 19880217.19890323;
    }
    
    test_encode_decode_page_template<OLAP_FIELD_TYPE_DOUBLE, segment_v2::PlainPageBuilder<OLAP_FIELD_TYPE_DOUBLE>,
        segment_v2::PlainPageDecoder<OLAP_FIELD_TYPE_DOUBLE> >(doubles.get(), size);
}

TEST_F(PlainPageTest, TestDoublePageEncoderSequence) {
    const uint32_t size = 10000;
    
    double base = 19880217.19890323;
    double delta = 13.14;
    std::unique_ptr<double[]> doubles(new double[size]);
    for (int i = 0; i < size; i++) {
        base = base + delta;
        doubles.get()[i] = base;
    }
   
    test_encode_decode_page_template<OLAP_FIELD_TYPE_DOUBLE, segment_v2::PlainPageBuilder<OLAP_FIELD_TYPE_DOUBLE>,
        segment_v2::PlainPageDecoder<OLAP_FIELD_TYPE_DOUBLE> >(doubles.get(), size);
}
    
TEST_F(PlainPageTest, TestPlainInt32PageEncoderEqual) {
    const uint32_t size = 10000;
    
    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 12345;
    }
    
    test_encode_decode_page_template<OLAP_FIELD_TYPE_INT, segment_v2::PlainPageBuilder<OLAP_FIELD_TYPE_INT>,
        segment_v2::PlainPageDecoder<OLAP_FIELD_TYPE_INT> >(ints.get(), size);
}

TEST_F(PlainPageTest, TestInt32PageEncoderSequence) {
    const uint32_t size = 10000;
    
    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    int32_t number = 0;
    for (int i = 0; i < size; i++) {
        ints.get()[i] = ++number;
    }
    
    test_encode_decode_page_template<OLAP_FIELD_TYPE_INT, segment_v2::PlainPageBuilder<OLAP_FIELD_TYPE_INT>,
        segment_v2::PlainPageDecoder<OLAP_FIELD_TYPE_INT> >(ints.get(), size);
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


