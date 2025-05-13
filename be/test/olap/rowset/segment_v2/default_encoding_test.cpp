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

#include <random>

#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/rowset/segment_v2/plain_page.h"
#include "olap/tablet_schema.h"
#include "testutil/test_util.h"
#include "util/block_compression.h"

namespace doris {
using namespace ErrorCode;
namespace segment_v2 {
class DefaultEncodingTest : public testing::Test {
private:
    size_t DATA_SIZE = 512 * 1024;

public:
    DefaultEncodingTest() = default;
    virtual ~DefaultEncodingTest() = default;

    std::string get_string_by_encoding(EncodingTypePB encoding) {
        switch (encoding) {
        case EncodingTypePB::PLAIN_ENCODING:
            return "PLAIN";
        case EncodingTypePB::PREFIX_ENCODING:
            return "PREFIX";
        case EncodingTypePB::RLE:
            return "RLE";
        case EncodingTypePB::DICT_ENCODING:
            return "DICT";
        case EncodingTypePB::BIT_SHUFFLE:
            return "BIT_SHUFFLE";
        case EncodingTypePB::FOR_ENCODING:
            return "FOR";
        default:
            return "UNKNOWN";
        }
    }

    std::string get_string_by_compression(CompressionTypePB compression) {
        switch (compression) {
        case CompressionTypePB::SNAPPY:
            return "SNAPPY";
        case CompressionTypePB::LZ4:
            return "LZ4";
        case CompressionTypePB::LZ4F:
            return "LZ4F";
        case CompressionTypePB::ZLIB:
            return "ZLIB";
        case CompressionTypePB::ZSTD:
            return "ZSTD";
        case CompressionTypePB::LZ4HC:
            return "LZ4HC";
        default:
            return "UNKNOWN";
        }
    }

    template <FieldType FType>
    void _test_encode_decode_page_template(EncodingTypePB& encoding_type,
                                           CompressionTypePB& compress_type,
                                           std::vector<typename TypeTraits<FType>::CppType>& src,
                                           size_t count, size_t origin_size,
                                           std::string random = "random") {
        // size_t origin_count = count;
        const EncodingInfo* encoding_info;
        auto status = EncodingInfo::get(FType, encoding_type, &encoding_info);
        if (!status.ok()) {
            std::cerr << "type=" << TabletColumn::get_string_by_field_type(FType)
                      << " encoding_type=" << encoding_type << " get encoding fail" << std::endl;
        }
        PageBuilderOptions opts;
        opts.data_page_size = 5 * 1024 * 1024;
        opts.dict_page_size = 5 * 1024 * 1024;
        PageBuilder* page_builder = nullptr;
        status = encoding_info->create_page_builder(opts, &page_builder);
        if (!status.ok() || page_builder == nullptr) {
            std::cerr << "type=" << TabletColumn::get_string_by_field_type(FType)
                      << " encoding_type=" << encoding_type << " create encoding info fail "
                      << status << std::endl;
        }
        std::unique_ptr<PageBuilder> page_builder_p;
        page_builder_p.reset(page_builder);
        OwnedSlice encoded_values;
        std::vector<Slice> body;
        int64_t encode_time = 0;
        {
            SCOPED_RAW_TIMER(&encode_time);
            status = page_builder_p->add(reinterpret_cast<const uint8_t*>(src.data()), &count);
            if (!status.ok()) {
                std::cerr << "type=" << TabletColumn::get_string_by_field_type(FType)
                          << " encoding_type=" << get_string_by_encoding(encoding_type)
                          << " encoding dd fail. status=" << status << std::endl;
            }
            status = page_builder_p->finish(&encoded_values);
            body.push_back(encoded_values.slice());
            if (!status.ok()) {
                std::cerr << "type=" << TabletColumn::get_string_by_field_type(FType)
                          << " encoding_type=" << get_string_by_encoding(encoding_type)
                          << " encoding finish fail. status=" << status << std::endl;
            }
        }
        size_t encoded_size = Slice::compute_total_size(body);
        BlockCompressionCodec* compress_codec;
        status = get_block_compression_codec(compress_type, &compress_codec);
        int64_t compression_time = 0;
        size_t compress_size = 0;
        OwnedSlice compressed_body;
        if (!status.ok() || compress_codec == nullptr) {
            std::cerr << "type=" << TabletColumn::get_string_by_field_type(FType)
                      << " encoding_type=" << get_string_by_encoding(encoding_type)
                      << ", compress_type=" << get_string_by_compression(compress_type)
                      << " create compression info fail. status=" << status << std::endl;
        } else {
            faststring buf;
            SCOPED_RAW_TIMER(&compression_time);
            status = compress_codec->compress(body, encoded_size, &buf);
            if (!status.ok()) {
                std::cerr << "type=" << TabletColumn::get_string_by_field_type(FType)
                          << " encoding_type=" << get_string_by_encoding(encoding_type)
                          << ", compress_type=" << get_string_by_compression(compress_type)
                          << " compression err."
                          << " status=" << status << std::endl;
            } else {
                compressed_body = buf.build();
                compress_size = compressed_body.slice().size;
            }
        }

        // decoding
        //        OwnedSlice decompressed_body(encoded_size);
        //        Slice decompressed_slice(decompressed_body.slice());
        //        Slice compressed_slice(compressed_body.slice());
        //
        //        int64_t decompress_time = 0;
        //        {
        //            SCOPED_RAW_TIMER(&decompress_time);
        //            status = compress_codec->decompress(compressed_slice, &decompressed_slice);
        //            if (!status.ok()) {
        //                LOG(WARNING) << "type=" << TabletColumn::get_string_by_field_type(FType)
        //                             << " encoding_type=" << encoding_type << ", compress_type="
        //                             << compress_type << " create compression info fail";
        //            }
        //        }
        //
        //
        //        int64_t decode_time = 0;
        //        {
        //            SCOPED_RAW_TIMER(&decode_time);
        //            auto* pre_decoder = encoding_info->get_data_page_pre_decoder();
        //            if (pre_decoder) {
        //                std::unique_ptr<DataPage> page =
        //                        std::make_unique<DataPage>(encoded_size, false, DATA_PAGE);
        //                status = pre_decoder->decode(&page, &decompressed_slice, 0, false, DATA_PAGE);
        //                if (!status.ok()) {
        //                    LOG(WARNING) << "type=" << TabletColumn::get_string_by_field_type(FType)
        //                                 << " encoding_type=" << encoding_type << ", compress_type="
        //                                 << compress_type << " pre decode fail";
        //                }
        //            }
        //            PageDecoder* decoder;
        //            PageDecoderOptions read_opts = PageDecoderOptions();
        //            status = encoding_info->create_page_decoder(decompressed_slice, read_opts, &decoder);
        //            if (!status.ok()) {
        //                LOG(WARNING) << "type=" << TabletColumn::get_string_by_field_type(FType)
        //                             << " encoding_type=" << encoding_type << ", compress_type="
        //                             << compress_type << " create decoder fail";
        //            }
        //            status = decoder->init();
        //            if (!status.ok()) {
        //                LOG(WARNING) << "type=" << TabletColumn::get_string_by_field_type(FType)
        //                             << " encoding_type=" << encoding_type << ", compress_type="
        //                             << compress_type << " decode fail";
        //            }
        //        }

        //        std::cout << "type=" << TabletColumn::get_string_by_field_type(FType)
        //                  << "\tencoding=" << get_string_by_encoding(encoding_type) << "\tcompression="
        //                  << get_string_by_compression(compress_type)
        ////                  << "\tencode_time="<< encode_time
        ////                  << "\tcompress_time=" << compression_time
        //                  << "\torigin_size=" << origin_size
        //                  << "\tencode size=" << encoded_size
        //                  << "\tcompress_size=" << compress_size
        //                  //<< decompress_time << "\t" << decode_time << "\t"
        //                  << "\tcompress_ratio=" << double(origin_size) / double(compress_size)
        //                  << "\tencode_ratio=" << double(origin_size) / double(encoded_size)
        //                  << "\torigin_count=" << origin_count << "\tencode_count=" << count
        //                  << std::endl;

        std::cout << TabletColumn::get_string_by_field_type(FType) << ", "
                  << get_string_by_encoding(encoding_type) << ", " << random << ", "
                  << get_string_by_compression(compress_type)
                  //                  << ", "<< encode_time
                  //                  << ", " << compression_time
                  << "," << origin_size << ", " << encoded_size << ", "
                  << compress_size
                  //<< decompress_time << ", " << decode_time << ", "
                  << ", " << double(origin_size) / double(compress_size) << ", "
                  << double(origin_size) / double(encoded_size) << std::endl;
    }

    template <FieldType FType>
    void test_encode_decode_page_template(std::vector<typename TypeTraits<FType>::CppType>& src,
                                          size_t count, size_t origin_size,
                                          std::vector<EncodingTypePB> encoding_types,
                                          std::string random = "random") {
        static CompressionTypePB compressions[6] = {LZ4, LZ4F, SNAPPY, ZSTD, ZLIB, LZ4HC};
        for (int i = 0; i < 6; ++i) {
            for (EncodingTypePB& encode_type : encoding_types) {
                auto compression = compressions[i];
                _test_encode_decode_page_template<FType>(encode_type, compression, src, count,
                                                         origin_size, random);
            }
        }
    }

    template <FieldType FType>
    void test_integer_template() {
        std::vector<EncodingTypePB> encoding_types;
        encoding_types.emplace_back(BIT_SHUFFLE);
        encoding_types.emplace_back(PLAIN_ENCODING);
        encoding_types.emplace_back(FOR_ENCODING);
        using INT_TYPE = typename TypeTraits<FType>::CppType;
        auto int_count = DATA_SIZE / sizeof(INT_TYPE);
        // random
        std::vector<INT_TYPE> ints(int_count);
        std::srand(static_cast<unsigned int>(std::time(nullptr)));
        for (int i = 0; i < int_count; ++i) {
            ints[i] = std::rand() % 10000 + 10000;
        }
        test_encode_decode_page_template<FType>(ints, int_count, sizeof(INT_TYPE) * int_count,
                                                encoding_types, "random");

        // repeat
        std::srand(static_cast<unsigned int>(std::time(nullptr)));
        INT_TYPE value = std::rand() % 10000 + 10000;
        for (int i = 0; i < int_count; ++i) {
            if (i % 30 == 0) {
                value = std::rand() % 10000 + 10000;
            }
            ints[i] = value;
        }
        test_encode_decode_page_template<FType>(ints, int_count, sizeof(INT_TYPE) * int_count,
                                                encoding_types, "repeat");
    }

    template <FieldType FType>
    void test_double_template() {
        std::vector<EncodingTypePB> encoding_types;
        encoding_types.emplace_back(BIT_SHUFFLE);
        encoding_types.emplace_back(PLAIN_ENCODING);
        using DOUBLE_TYPE = typename TypeTraits<FType>::CppType;
        auto double_count = DATA_SIZE / sizeof(DOUBLE_TYPE);

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> dis(100.0, 10000.0);
        std::vector<DOUBLE_TYPE> doubles(double_count);

        // random
        for (int i = 0; i < double_count; ++i) {
            doubles[i] = dis(gen);
        }
        test_encode_decode_page_template<FType>(doubles, double_count,
                                                sizeof(DOUBLE_TYPE) * double_count, encoding_types,
                                                "random");

        // repeat
        double start = dis(gen);
        for (int i = 0; i < double_count; ++i) {
            doubles[i] = start;
            if (i % 30 == 0) {
                start = dis(gen);
            }
        }
        test_encode_decode_page_template<FType>(doubles, double_count,
                                                sizeof(DOUBLE_TYPE) * double_count, encoding_types,
                                                "repeat");
    }

    template <FieldType FType>
    void test_decimal_template() {
        std::vector<EncodingTypePB> encoding_types;
        encoding_types.emplace_back(BIT_SHUFFLE);
        encoding_types.emplace_back(PLAIN_ENCODING);
        using DECIMAL_TYPE = typename TypeTraits<FType>::CppType;
        auto int_count = DATA_SIZE / sizeof(DECIMAL_TYPE);
        // random
        std::vector<DECIMAL_TYPE> decimals(int_count);
        std::srand(static_cast<unsigned int>(std::time(nullptr)));
        for (int i = 0; i < int_count; ++i) {
            decimal12_t decimal;
            decimal.integer = std::rand() % 100000 + 100000;
            decimal.fraction = std::rand() % 10000 + 10000;
            decimals[i] = decimal;
        }
        test_encode_decode_page_template<FType>(
                decimals, int_count, sizeof(DECIMAL_TYPE) * int_count, encoding_types, "random");

        // repeat
        std::srand(static_cast<unsigned int>(std::time(nullptr)));
        decimal12_t value;
        value.integer = std::rand() % 100000 + 100000;
        value.fraction = std::rand() % 10000 + 10000;
        for (int i = 0; i < int_count; ++i) {
            if (i % 30 == 0) {
                value.integer = std::rand() % 100000 + 100000;
                value.fraction = std::rand() % 10000 + 10000;
            }
            decimals[i] = value;
        }
        test_encode_decode_page_template<FType>(
                decimals, int_count, sizeof(DECIMAL_TYPE) * int_count, encoding_types, "repeat");
    }

    template <FieldType FType>
    void test_new_decimal_template(int128_t mod_value) {
        std::vector<EncodingTypePB> encoding_types;
        encoding_types.emplace_back(BIT_SHUFFLE);
        encoding_types.emplace_back(PLAIN_ENCODING);
        using DECIMAL_TYPE = typename TypeTraits<FType>::CppType;
        auto int_count = DATA_SIZE / sizeof(DECIMAL_TYPE);
        // random
        std::vector<DECIMAL_TYPE> decimals(int_count);
        std::srand(static_cast<unsigned int>(std::time(nullptr)));
        for (int i = 0; i < int_count; ++i) {
            int128_t high = std::rand();
            decimals[i] = std::rand() % ((high << 64) | std::rand()) % mod_value;
        }
        test_encode_decode_page_template<FType>(
                decimals, int_count, sizeof(DECIMAL_TYPE) * int_count, encoding_types, "random");

        // repeat
        int128_t value = std::rand();
        value = std::rand() % ((value << 64) | std::rand()) % mod_value;
        for (int i = 0; i < int_count; ++i) {
            if (i % 30 == 0) {
                value = std::rand();
                value = std::rand() % ((value << 64) | std::rand()) % mod_value;
            }
            decimals[i] = value;
        }
        test_encode_decode_page_template<FType>(
                decimals, int_count, sizeof(DECIMAL_TYPE) * int_count, encoding_types, "repeat");
    }

    template <FieldType FType>
    void test_char_template() {
        std::vector<EncodingTypePB> encoding_types;
        encoding_types.emplace_back(DICT_ENCODING);
        encoding_types.emplace_back(PLAIN_ENCODING);
        encoding_types.emplace_back(PREFIX_ENCODING);
        using CHAR_TYPE = typename TypeTraits<FType>::CppType;
        auto char_count = DATA_SIZE / sizeof(CHAR_TYPE);
        // random
        std::vector<std::string> strings;
        size_t real_count = 0;
        size_t real_size = 0;
        while (true) {
            strings.emplace_back(rand_rng_by_type(FType));
            real_size += strings.back().size();
            ++real_count;
            if (real_size > DATA_SIZE) {
                break;
            }
        }
        std::vector<CHAR_TYPE> slices(real_count);
        for (int i = 0; i < real_count; ++i) {
            slices[i] = strings[i];
        }

        test_encode_decode_page_template<FType>(slices, real_count, real_size, encoding_types,
                                                "random");

        // repeat
        real_count = 0;
        real_size = 0;
        strings.clear();
        std::string str = rand_rng_by_type(FType);
        for (int i = 0; i < char_count; ++i) {
            if (i % 30 == 0) {
                str = rand_rng_by_type(FType);
            }
            strings.emplace_back(str);
            real_size += str.size();
            ++real_count;
            if (real_size > DATA_SIZE) {
                break;
            }
        }
        std::vector<CHAR_TYPE> new_slices(real_count);
        for (int i = 0; i < real_count; ++i) {
            new_slices[i] = strings[i];
        }
        test_encode_decode_page_template<FType>(new_slices, real_count, real_size, encoding_types,
                                                "repeat");
    }
};

TEST_F(DefaultEncodingTest, test) {
    std::cout << "DataType, EncodingType, randomness, CompressionType"
              //                  << ", EncodingTime"
              //                  << ", CompressionTime"
              << ", OriginSize, EncodedSize, CompressSize"
              //<< ", DecompressTime, DecodeTime"
              << ", CompressRatio, EncodeRatio" << std::endl;

    test_integer_template<FieldType::OLAP_FIELD_TYPE_TINYINT>();
    test_integer_template<FieldType::OLAP_FIELD_TYPE_SMALLINT>();
    test_integer_template<FieldType::OLAP_FIELD_TYPE_INT>();
    test_integer_template<FieldType::OLAP_FIELD_TYPE_BIGINT>();
    test_integer_template<FieldType::OLAP_FIELD_TYPE_LARGEINT>();

    test_double_template<FieldType::OLAP_FIELD_TYPE_DOUBLE>();
    test_double_template<FieldType::OLAP_FIELD_TYPE_FLOAT>();

    test_decimal_template<FieldType::OLAP_FIELD_TYPE_DECIMAL>();

    test_new_decimal_template<FieldType::OLAP_FIELD_TYPE_DECIMAL32>(1000);
    test_new_decimal_template<FieldType::OLAP_FIELD_TYPE_DECIMAL64>(21474836470);
    test_new_decimal_template<FieldType::OLAP_FIELD_TYPE_DECIMAL128I>(9223372036854775807);

    test_char_template<FieldType::OLAP_FIELD_TYPE_CHAR>();
    test_char_template<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
    test_char_template<FieldType::OLAP_FIELD_TYPE_STRING>();
}

} // namespace segment_v2
} // namespace doris