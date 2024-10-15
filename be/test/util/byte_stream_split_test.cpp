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

#include "util/byte_stream_split.h"

#include <glog/logging.h>

#include <cstdint>
#include <limits>

#include "gtest/gtest.h"

namespace doris {

// just for test
void byte_stream_split_encode(const uint8_t* src, int64_t length, int width, uint8_t* dest) {
    int nums = length / width;
    for (int i = 0; i < nums; i++) {
        for (int j = 0; j < width; j++) {
            dest[j * nums + i] = src[i * width + j];
        }
    }
}

template <typename T>
void test_byte_stream_split(const T* data, int64_t length) {
    int width = sizeof(T);
    int nums = length / width;
    T* encoded_data = new T[nums];
    T* decoded_data = new T[nums];
    byte_stream_split_encode((const uint8_t*)data, length, width, (uint8_t*)encoded_data);
    int offset = 0;
    int run_length = 4;
    while (offset < nums) {
        int num_values = std::min(run_length, nums - offset);
        byte_stream_split_decode((const uint8_t*)encoded_data, width, offset, num_values, nums,
                                 (uint8_t*)decoded_data + offset * width);
        offset += run_length;
    }

    for (int i = 0; i < nums; i++) {
        EXPECT_EQ(data[i], decoded_data[i]);
    }
    delete[] encoded_data;
    delete[] decoded_data;
}

TEST(ByteStreamSplit, ByteStreamSplitDecode) {
    int8_t data1[] = {0, -1, 1, INT8_MIN / 2, INT8_MAX / 2, INT8_MIN, INT8_MAX};
    test_byte_stream_split(data1, sizeof(data1));
    int16_t data2[] = {0, -1, 1, INT16_MIN / 2, INT16_MAX / 2, INT16_MIN, INT16_MAX};
    test_byte_stream_split(data2, sizeof(data2));
    int32_t data3[] = {0, -1, 1, INT32_MIN / 2, INT32_MAX / 2, INT32_MIN, INT32_MAX};
    test_byte_stream_split(data3, sizeof(data3));
    int64_t data4[] = {
            0,         -1,       1, INT64_MIN / 4, INT64_MAX / 4, INT64_MIN / 2, INT64_MAX / 2,
            INT64_MIN, INT64_MAX};
    test_byte_stream_split(data4, sizeof(data4));
    float data5[] = {0.0,
                     -1.0,
                     1.0,
                     0.0000001,
                     -0.0000001,
                     std::numeric_limits<float>::min() / 2,
                     std::numeric_limits<float>::max() / 2,
                     std::numeric_limits<float>::min(),
                     std::numeric_limits<float>::max()};
    test_byte_stream_split(data5, sizeof(data5));
    double data6[] = {0.0,
                      -1.0,
                      1.0,
                      0.000000000000001,
                      -0.000000000000001,
                      std::numeric_limits<double>::min() / 2,
                      std::numeric_limits<double>::max() / 2,
                      std::numeric_limits<double>::min(),
                      std::numeric_limits<double>::max()};
    test_byte_stream_split(data6, sizeof(data6));
}

} // namespace doris
