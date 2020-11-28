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

#include "olap/serialize.h"

#include <gtest/gtest.h>

namespace doris {
namespace ser {

class SerializeTest : public testing::Test {
public:
    SerializeTest() {}
    virtual ~SerializeTest() {}
};

TEST_F(SerializeTest, get_closet_fixed_bits) {
    ASSERT_EQ(1, get_closet_fixed_bits(0));
    for (int i = 1; i <= 24; ++i) {
        ASSERT_EQ(i, get_closet_fixed_bits(i));
    }
    for (int i = 25; i <= 26; ++i) {
        ASSERT_EQ(26, get_closet_fixed_bits(i));
    }
    for (int i = 27; i <= 28; ++i) {
        ASSERT_EQ(28, get_closet_fixed_bits(i));
    }
    for (int i = 29; i <= 30; ++i) {
        ASSERT_EQ(30, get_closet_fixed_bits(i));
    }
    for (int i = 31; i <= 32; ++i) {
        ASSERT_EQ(32, get_closet_fixed_bits(i));
    }
    for (int i = 33; i <= 40; ++i) {
        ASSERT_EQ(40, get_closet_fixed_bits(i));
    }
    for (int i = 41; i <= 48; ++i) {
        ASSERT_EQ(48, get_closet_fixed_bits(i));
    }
    for (int i = 49; i <= 56; ++i) {
        ASSERT_EQ(56, get_closet_fixed_bits(i));
    }
    for (int i = 57; i <= 64; ++i) {
        ASSERT_EQ(64, get_closet_fixed_bits(i));
    }
}

TEST_F(SerializeTest, find_closet_num_bits) {
    ASSERT_EQ(1, find_closet_num_bits(0));
    for (int i = 1; i <= 24; ++i) {
        uint64_t val = (1l << (i - 1));
        ASSERT_EQ(i, find_closet_num_bits(val));
    }
    for (int i = 25; i <= 26; ++i) {
        uint64_t val = (1l << (i - 1));
        ASSERT_EQ(26, find_closet_num_bits(val));
    }
    for (int i = 27; i <= 28; ++i) {
        uint64_t val = (1l << (i - 1));
        ASSERT_EQ(28, find_closet_num_bits(val));
    }
    for (int i = 29; i <= 30; ++i) {
        uint64_t val = (1l << (i - 1));
        ASSERT_EQ(30, find_closet_num_bits(val));
    }
    for (int i = 31; i <= 32; ++i) {
        uint64_t val = (1l << (i - 1));
        ASSERT_EQ(32, find_closet_num_bits(val));
    }
    for (int i = 33; i <= 40; ++i) {
        uint64_t val = (1l << (i - 1));
        ASSERT_EQ(40, find_closet_num_bits(val));
    }
    for (int i = 41; i <= 48; ++i) {
        uint64_t val = (1l << (i - 1));
        ASSERT_EQ(48, find_closet_num_bits(val));
    }
    for (int i = 49; i <= 56; ++i) {
        uint64_t val = (1l << (i - 1));
        ASSERT_EQ(56, find_closet_num_bits(val));
    }
    for (int i = 57; i <= 64; ++i) {
        uint64_t val = (1l << (i - 1));
        ASSERT_EQ(64, find_closet_num_bits(val));
    }
}

TEST_F(SerializeTest, encode_bit_width) {
    ASSERT_EQ(ONE, encode_bit_width(0));
    for (int i = 1; i <= 24; ++i) {
        ASSERT_EQ(i - 1, encode_bit_width(i));
    }
    for (int i = 25; i <= 26; ++i) {
        ASSERT_EQ(TWENTYSIX, encode_bit_width(i));
    }
    for (int i = 27; i <= 28; ++i) {
        ASSERT_EQ(TWENTYEIGHT, encode_bit_width(i));
    }
    for (int i = 29; i <= 30; ++i) {
        ASSERT_EQ(THIRTY, encode_bit_width(i));
    }
    for (int i = 31; i <= 32; ++i) {
        ASSERT_EQ(THIRTYTWO, encode_bit_width(i));
    }
    for (int i = 33; i <= 40; ++i) {
        ASSERT_EQ(FORTY, encode_bit_width(i));
    }
    for (int i = 41; i <= 48; ++i) {
        ASSERT_EQ(FORTYEIGHT, encode_bit_width(i));
    }
    for (int i = 49; i <= 56; ++i) {
        ASSERT_EQ(FIFTYSIX, encode_bit_width(i));
    }
    for (int i = 57; i <= 64; ++i) {
        ASSERT_EQ(SIXTYFOUR, encode_bit_width(i));
    }
}

TEST_F(SerializeTest, decode_bit_width) {
    for (int i = 0; i <= TWENTYFOUR; ++i) {
        ASSERT_EQ(i + 1, decode_bit_width(i));
    }
    ASSERT_EQ(26, decode_bit_width(TWENTYSIX));
    ASSERT_EQ(28, decode_bit_width(TWENTYEIGHT));
    ASSERT_EQ(30, decode_bit_width(THIRTY));
    ASSERT_EQ(32, decode_bit_width(THIRTYTWO));
    ASSERT_EQ(40, decode_bit_width(FORTY));
    ASSERT_EQ(48, decode_bit_width(FORTYEIGHT));
    ASSERT_EQ(56, decode_bit_width(FIFTYSIX));
    ASSERT_EQ(64, decode_bit_width(SIXTYFOUR));
}

TEST_F(SerializeTest, percentile_bits) {
    int64_t data[100];

    {
        for (int i = 0; i < 5; ++i) {
            data[i] = (1l << 58);
        }
        for (int i = 5; i < 100; ++i) {
            data[i] = 1;
        }
        ASSERT_EQ(0, percentile_bits(data, 100, 0.0));
        ASSERT_EQ(1, percentile_bits(data, 100, 0.95));
        ASSERT_EQ(64, percentile_bits(data, 100, 0.99));
        ASSERT_EQ(64, percentile_bits(data, 100, 1.0));
    }
    {
        for (int i = 0; i < 11; ++i) {
            data[i] = (1l << 26);
        }
        for (int i = 11; i < 100; ++i) {
            data[i] = 1;
        }
        ASSERT_EQ(0, percentile_bits(data, 100, 0.0));
        ASSERT_EQ(1, percentile_bits(data, 100, 0.8));
        ASSERT_EQ(28, percentile_bits(data, 100, 0.9));
    }
    {
        for (int i = 0; i < 11; ++i) {
            data[i] = (1l << 26);
        }
        for (int i = 11; i < 100; ++i) {
            data[i] = 0;
        }
        ASSERT_EQ(0, percentile_bits(data, 100, 0.0));
        ASSERT_EQ(1, percentile_bits(data, 100, 0.1));
        ASSERT_EQ(1, percentile_bits(data, 100, 0.8));
        ASSERT_EQ(28, percentile_bits(data, 100, 0.9));
    }
}

TEST_F(SerializeTest, new_percentile_bits) {
    int64_t data[100];

    {
        for (int i = 0; i < 5; ++i) {
            data[i] = (1l << 58);
        }
        for (int i = 5; i < 100; ++i) {
            data[i] = 1;
        }
        uint16_t hists[65];
        compute_hists(data, 100, hists);
        ASSERT_EQ(0, percentile_bits_with_hist(hists, 100, 0.0));
        ASSERT_EQ(1, percentile_bits_with_hist(hists, 100, 0.95));
        ASSERT_EQ(64, percentile_bits_with_hist(hists, 100, 0.99));
        ASSERT_EQ(64, percentile_bits_with_hist(hists, 100, 1.0));
    }
    {
        for (int i = 0; i < 11; ++i) {
            data[i] = (1l << 26);
        }
        for (int i = 11; i < 100; ++i) {
            data[i] = 1;
        }
        uint16_t hists[65];
        compute_hists(data, 100, hists);
        ASSERT_EQ(0, percentile_bits_with_hist(hists, 100, 0.0));
        ASSERT_EQ(1, percentile_bits_with_hist(hists, 100, 0.8));
        ASSERT_EQ(28, percentile_bits_with_hist(hists, 100, 0.9));
    }
    {
        for (int i = 0; i < 11; ++i) {
            data[i] = (1l << 26);
        }
        for (int i = 11; i < 100; ++i) {
            data[i] = 0;
        }
        uint16_t hists[65];
        compute_hists(data, 100, hists);
        ASSERT_EQ(0, percentile_bits_with_hist(hists, 100, 0.0));
        ASSERT_EQ(1, percentile_bits_with_hist(hists, 100, 0.1));
        ASSERT_EQ(1, percentile_bits_with_hist(hists, 100, 0.8));
        ASSERT_EQ(28, percentile_bits_with_hist(hists, 100, 0.9));
    }
}

} // namespace ser
} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
