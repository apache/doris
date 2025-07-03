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

#include <regex>

#include "vec/exec/format/parquet/parquet_pred_cmp.h"

namespace doris {
namespace vectorized {
class ParquetStatisticsTest : public testing::Test {
public:
    ParquetStatisticsTest() = default;
};

TEST_F(ParquetStatisticsTest, test_try_read_old_utf8_stats) {
    // [, bcé]: min is empty, max starts with ASCII
    {
        std::string encoding_min("");
        std::string encoding_max("bcé");
        EXPECT_FALSE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
    }

    //    // [, ébc]: min is empty, max starts with non-ASCII
    {
        std::string encoding_min("");
        std::string encoding_max("ébc");
        EXPECT_FALSE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
    }

    // [aa, bé]: no common prefix, first different are both ASCII, min is all ASCII
    {
        std::string encoding_min("aa");
        std::string encoding_max("bé");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
        EXPECT_EQ(encoding_min, "aa");
        EXPECT_EQ(encoding_max, "c");
    }

    // [abcd, abcdN]: common prefix, not only ASCII, one prefix of the other, last common ASCII
    {
        std::string encoding_min("abcd");
        std::string encoding_max("abcdN");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
        EXPECT_EQ(encoding_min, "abcd");
        EXPECT_EQ(encoding_max, "abce");
    }

    // [abcé, abcéN]: common prefix, not only ASCII, one prefix of the other, last common non ASCII
    {
        std::string encoding_min("abcé");
        std::string encoding_max("abcéN");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
        EXPECT_EQ(encoding_min, "abcé");
        EXPECT_EQ(encoding_max, "abd");
    }

    // [abcéM, abcéN]: common prefix, not only ASCII, first different are both ASCII
    {
        std::string encoding_min("abcéM");
        std::string encoding_max("abcéN");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
        EXPECT_EQ(encoding_min, "abcéM");
        EXPECT_EQ(encoding_max, "abcéO");
    }

    // [abcéMab, abcéNxy]: common prefix, not only ASCII, first different are both ASCII, more characters afterwards
    {
        std::string encoding_min("abcéMab");
        std::string encoding_max("abcéNxy");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
        EXPECT_EQ(encoding_min, "abcéMab");
        EXPECT_EQ(encoding_max, "abcéO");
    }

    // [abcéM, abcé\u00f7]: common prefix, not only ASCII, first different are both ASCII, but need to be chopped off (127)
    {
        std::string encoding_min("abcéM");
        std::string encoding_max("abcé\u00f7");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        EXPECT_EQ(encoding_min, "abcéM");
        EXPECT_EQ(encoding_max, "abd");
    }

    // [abc\u007fé, bcd\u007fé]: no common prefix, first different are both ASCII
    {
        std::string encoding_min("abc\u007fé");
        std::string encoding_max("bcd\u007fé");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
        EXPECT_EQ(encoding_min, "abc\u007f");
        EXPECT_EQ(encoding_max, "c");
    }

    // [é, a]: no common prefix, first different are not both ASCII
    {
        std::string encoding_min("é");
        std::string encoding_max("a");
        EXPECT_FALSE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
    }

    // [é, ê]: no common prefix, first different are both not ASCII
    {
        std::string encoding_min("é");
        std::string encoding_max("ê");
        EXPECT_FALSE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
    }

    // [aé, aé]: min = max (common prefix, first different are both not ASCII)
    {
        std::string encoding_min("aé");
        std::string encoding_max("aé");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
        EXPECT_EQ(encoding_min, "aé");
        EXPECT_EQ(encoding_max, "aé");
    }

    // [aé, bé]: no common prefix, first different are both ASCII
    {
        std::string encoding_min("aé");
        std::string encoding_max("bé");
        EXPECT_TRUE(ParquetPredicate::_try_read_old_utf8_stats(encoding_min, encoding_max));
        ;
        EXPECT_EQ(encoding_min, "a");
        EXPECT_EQ(encoding_max, "c");
    }
}

} // namespace vectorized
} // namespace doris
