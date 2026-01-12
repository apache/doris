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

#include "util/frame_of_reference_coding.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <cstring>
#include <random>
#include <vector>

#include "gtest/gtest_pred_impl.h"

namespace doris {

// original bit_pack function
template <typename T>
void bit_pack(const T* input, uint8_t in_num, int bit_width, uint8_t* output) {
    if (in_num == 0 || bit_width == 0) {
        return;
    }

    T in_mask = 0;
    int bit_index = 0;
    *output = 0;
    for (int i = 0; i < in_num; i++) {
        in_mask = ((T)1) << (bit_width - 1);
        for (int k = 0; k < bit_width; k++) {
            if (bit_index > 7) {
                bit_index = 0;
                output++;
                *output = 0;
            }
            *output |= (((input[i] & in_mask) >> (bit_width - k - 1)) << (7 - bit_index));
            in_mask >>= 1;
            bit_index++;
        }
    }
}

class TestForCoding : public testing::Test {
public:
    static void test_frame_of_reference_encode_decode(int32_t element_size) {
        faststring buffer(1);
        ForEncoder<int32_t> encoder(&buffer);

        std::vector<int32_t> data;
        for (int32_t i = 0; i < element_size; ++i) {
            data.push_back(i);
        }
        encoder.put_batch(data.data(), element_size);
        encoder.flush();

        ForDecoder<int32_t> decoder(buffer.data(), buffer.length());
        decoder.init();
        std::vector<int32_t> actual_result(element_size);
        decoder.get_batch(actual_result.data(), element_size);

        EXPECT_EQ(data, actual_result);
    }

    static void test_skip(int32_t skip_num) {
        faststring buffer(1);
        ForEncoder<uint32_t> encoder(&buffer);

        std::vector<uint32_t> input_data;
        std::vector<uint32_t> expect_result;
        for (uint32_t i = 0; i < 256; ++i) {
            input_data.push_back(i);
            if (i >= skip_num) {
                expect_result.push_back(i);
            }
        }
        encoder.put_batch(input_data.data(), 256);
        encoder.flush();

        ForDecoder<uint32_t> decoder(buffer.data(), buffer.length());
        decoder.init();
        decoder.skip(skip_num);

        std::vector<uint32_t> actual_result(256 - skip_num);
        decoder.get_batch(actual_result.data(), 256 - skip_num);

        EXPECT_EQ(expect_result, actual_result);
    }
};

TEST_F(TestForCoding, TestHalfFrame) {
    test_frame_of_reference_encode_decode(64);
}

TEST_F(TestForCoding, TestOneFrame) {
    test_frame_of_reference_encode_decode(128);
}

TEST_F(TestForCoding, TestTwoFrame) {
    test_frame_of_reference_encode_decode(256);
}

TEST_F(TestForCoding, TestTwoHlafFrame) {
    test_frame_of_reference_encode_decode(320);
}

TEST_F(TestForCoding, TestSkipZero) {
    test_skip(0);
}

TEST_F(TestForCoding, TestSkipHalfFrame) {
    test_skip(64);
}

TEST_F(TestForCoding, TestSkipOneFrame) {
    test_skip(128);
}

TEST_F(TestForCoding, TestInt64) {
    faststring buffer(1);
    ForEncoder<int64_t> encoder(&buffer);

    std::vector<int64_t> data;
    for (int64_t i = 0; i < 320; ++i) {
        data.push_back(i);
    }
    encoder.put_batch(data.data(), 320);
    encoder.flush();

    ForDecoder<int64_t> decoder(buffer.data(), buffer.length());
    decoder.init();
    std::vector<int64_t> actual_result(320);
    decoder.get_batch(actual_result.data(), 320);

    EXPECT_EQ(data, actual_result);
}

TEST_F(TestForCoding, TestOneMinValue) {
    faststring buffer(1);
    ForEncoder<int32_t> encoder(&buffer);
    encoder.put(2019);
    encoder.flush();

    ForDecoder<int32_t> decoder(buffer.data(), buffer.length());
    decoder.init();
    int32_t actual_value;
    decoder.get(&actual_value);
    EXPECT_EQ(2019, actual_value);
}

TEST_F(TestForCoding, TestZeroValue) {
    faststring buffer(1);
    ForEncoder<int32_t> encoder(&buffer);
    encoder.flush();

    EXPECT_EQ(buffer.length(), 4 + 1);

    ForDecoder<int32_t> decoder(buffer.data(), buffer.length());
    decoder.init();
    int32_t actual_value;
    bool result = decoder.get(&actual_value);
    EXPECT_EQ(result, false);
}

TEST_F(TestForCoding, TestBytesAlign) {
    faststring buffer(1);
    ForEncoder<int32_t> encoder(&buffer);
    encoder.put(2019);
    encoder.put(2020);
    encoder.flush();

    ForDecoder<int32_t> decoder(buffer.data(), buffer.length());
    decoder.init();

    int32_t actual_value;
    decoder.get(&actual_value);
    EXPECT_EQ(2019, actual_value);
    decoder.get(&actual_value);
    EXPECT_EQ(2020, actual_value);
}

TEST_F(TestForCoding, TestValueSeekSpecialCase) {
    faststring buffer(1);
    ForEncoder<int64_t> encoder(&buffer);

    std::vector<int64_t> data;
    for (int64_t i = 0; i < 128; ++i) {
        data.push_back(i);
    }

    for (int64_t i = 300; i < 500; ++i) {
        data.push_back(i);
    }

    encoder.put_batch(data.data(), data.size());
    encoder.flush();

    ForDecoder<int64_t> decoder(buffer.data(), buffer.length());
    decoder.init();

    int64_t target = 160;
    bool exact_match;
    bool has_value = decoder.seek_at_or_after_value(&target, &exact_match);
    EXPECT_EQ(has_value, true);
    EXPECT_EQ(exact_match, false);

    int64_t next_value;
    decoder.get(&next_value);
    EXPECT_EQ(300, next_value);
}

TEST_F(TestForCoding, TestValueSeek) {
    faststring buffer(1);
    ForEncoder<int64_t> encoder(&buffer);

    const int64_t SIZE = 320;
    std::vector<int64_t> data;
    for (int64_t i = 0; i < SIZE; ++i) {
        data.push_back(i);
    }
    encoder.put_batch(data.data(), SIZE);
    encoder.flush();

    ForDecoder<int64_t> decoder(buffer.data(), buffer.length());
    decoder.init();

    int64_t target = 160;
    bool exact_match;
    bool found = decoder.seek_at_or_after_value(&target, &exact_match);
    EXPECT_EQ(found, true);
    EXPECT_EQ(exact_match, true);

    int64_t actual_value;
    decoder.get(&actual_value);
    EXPECT_EQ(target, actual_value);

    target = -1;
    found = decoder.seek_at_or_after_value(&target, &exact_match);
    EXPECT_EQ(found, true);
    EXPECT_EQ(exact_match, false);

    std::vector<int64_t> actual_result(SIZE);
    decoder.get_batch(actual_result.data(), SIZE);
    EXPECT_EQ(data, actual_result);

    target = 0;
    found = decoder.seek_at_or_after_value(&target, &exact_match);
    EXPECT_EQ(found, true);
    EXPECT_EQ(exact_match, true);

    decoder.get_batch(actual_result.data(), SIZE);
    EXPECT_EQ(data, actual_result);

    target = 319;
    found = decoder.seek_at_or_after_value(&target, &exact_match);
    EXPECT_EQ(found, true);
    EXPECT_EQ(exact_match, true);

    decoder.get(&actual_value);
    EXPECT_EQ(target, actual_value);

    target = 320;
    found = decoder.seek_at_or_after_value(&target, &exact_match);
    EXPECT_EQ(found, false);
}

TEST_F(TestForCoding, accuracy_unpack_64_test) {
    std::default_random_engine e;
    std::uniform_int_distribution<int64_t> u;

    for (int n = 1; n <= 255; n++) {
        for (int w = 1; w <= 64; w++) {
            faststring buffer(1);
            ForEncoder<int64_t> encoder(&buffer);

            std::vector<int64_t> test_data(n);
            int64_t in_mask = (((__int128_t)1) << w) - 1;
            for (int i = 0; i < n; i++) {
                test_data[i] = u(e) & in_mask;
                encoder.put(test_data[i]);
            }
            encoder.flush();

            ForDecoder<int64_t> decoder(buffer.data(), buffer.length());
            decoder.init();
            int64_t actual_value;
            for (int i = 0; i < n; i++) {
                decoder.get(&actual_value);
                EXPECT_EQ(test_data[i], actual_value);
            }
        }
    }
}

TEST_F(TestForCoding, accuracy_unpack_128_test) {
    std::default_random_engine e;
    std::uniform_int_distribution<__int128_t> u;

    for (int n = 1; n <= 255; n++) {
        for (int w = 64; w <= 127; w++) {
            faststring buffer(1);
            ForEncoder<__int128_t> encoder(&buffer);

            std::vector<__int128_t> test_data(n);
            __int128_t in_mask = (((__int128_t)1) << w) - 1;
            for (int i = 0; i < n; i++) {
                test_data[i] = u(e) & in_mask;
                encoder.put(test_data[i]);
            }
            encoder.flush();

            ForDecoder<__int128_t> decoder(buffer.data(), buffer.length());
            decoder.init();
            __int128_t actual_value;
            for (int i = 0; i < n; i++) {
                decoder.get(&actual_value);
                EXPECT_EQ(test_data[i], actual_value);
            }
        }
    }
}

TEST_F(TestForCoding, accuracy_test) {
    std::default_random_engine e;
    std::uniform_int_distribution<int64_t> u;
    ForEncoder<__int128_t> forEncoder(nullptr);
    for (int T = 1; T <= 5; T++) {
        for (int n = 1; n <= 255; n++) {
            std::vector<__int128_t> test_data(n);
            for (int w = 1; w <= 127; w++) {
                __int128_t in_mask = (((__int128_t)1) << w) - 1;
                for (int i = 0; i < n; i++) {
                    test_data[i] = u(e) & in_mask;
                }
                int size = (n * w + 7) / 8;
                std::vector<uint8_t> output_1(size), output_2(size);
                bit_pack<__int128_t>(test_data.data(), n, w, output_1.data());
                forEncoder.bit_pack(test_data.data(), n, w, output_2.data());
                for (int i = 0; i < size; i++) {
                    EXPECT_EQ(output_1[i], output_2[i]);
                }
            }
        }
    }
}

TEST_F(TestForCoding, accuracy2_test) {
    ForEncoder<__int128_t> encoder(nullptr);
    ForDecoder<__int128_t> decoder(nullptr, 0);

    for (int n = 1; n <= 255; n++) {
        for (int w = 1; w <= 127; w++) {
            std::vector<__int128_t> test_data(n);
            __int128_t in_mask = (((__int128_t)1) << w) - 1;
            for (int i = 0; i < n; i++) {
                test_data[i] = i & in_mask;
            }
            std::vector<uint8_t> o((n * w + 7) / 8);
            encoder.bit_pack(test_data.data(), n, w, o.data());

            std::vector<__int128_t> output(n);
            decoder.bit_unpack(o.data(), n, w, output.data());
            for (int i = 0; i < n; i++) {
                EXPECT_EQ(i & in_mask, output[i]);
            }
        }
    }
}

} // namespace doris