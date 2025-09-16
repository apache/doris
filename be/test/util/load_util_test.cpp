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

#include "util/load_util.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"

namespace doris {

class LoadUtilTest : public testing::Test {
public:
    LoadUtilTest() {}
    ~LoadUtilTest() override = default;
};

TEST_F(LoadUtilTest, StreamingTest) {
    {
        EXPECT_TRUE(LoadUtil::is_format_support_streaming(TFileFormatType::FORMAT_CSV_PLAIN));
        EXPECT_TRUE(LoadUtil::is_format_support_streaming(TFileFormatType::FORMAT_CSV_BZ2));
        EXPECT_TRUE(LoadUtil::is_format_support_streaming(TFileFormatType::FORMAT_CSV_DEFLATE));
        EXPECT_TRUE(LoadUtil::is_format_support_streaming(TFileFormatType::FORMAT_CSV_GZ));
        EXPECT_TRUE(LoadUtil::is_format_support_streaming(TFileFormatType::FORMAT_CSV_LZ4FRAME));
        EXPECT_TRUE(LoadUtil::is_format_support_streaming(TFileFormatType::FORMAT_CSV_LZO));
        EXPECT_TRUE(LoadUtil::is_format_support_streaming(TFileFormatType::FORMAT_CSV_LZOP));
        EXPECT_TRUE(LoadUtil::is_format_support_streaming(TFileFormatType::FORMAT_JSON));
        EXPECT_FALSE(LoadUtil::is_format_support_streaming(TFileFormatType::FORMAT_PARQUET));
        EXPECT_FALSE(LoadUtil::is_format_support_streaming(TFileFormatType::FORMAT_ORC));
        EXPECT_FALSE(LoadUtil::is_format_support_streaming(TFileFormatType::FORMAT_PROTO));
        EXPECT_FALSE(LoadUtil::is_format_support_streaming(TFileFormatType::FORMAT_JNI));
        EXPECT_FALSE(LoadUtil::is_format_support_streaming(TFileFormatType::FORMAT_AVRO));
        EXPECT_FALSE(LoadUtil::is_format_support_streaming(TFileFormatType::FORMAT_UNKNOWN));
    }
}

TEST_F(LoadUtilTest, ParseTest) {
    {
        // "", ""
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("", "", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_CSV_PLAIN, format_type);
        EXPECT_EQ(TFileCompressType::PLAIN, compress_type);
    }
    {
        // CSV, GZ
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("CSV", "GZ", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_CSV_GZ, format_type);
        EXPECT_EQ(TFileCompressType::GZ, compress_type);
    }
    {
        // CSV, LZO
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("CSV", "LZO", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_CSV_LZO, format_type);
        EXPECT_EQ(TFileCompressType::LZO, compress_type);
    }
    {
        // CSV, BZ2
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("CSV", "BZ2", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_CSV_BZ2, format_type);
        EXPECT_EQ(TFileCompressType::BZ2, compress_type);
    }
    {
        // CSV, LZ4
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("CSV", "LZ4", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_CSV_LZ4FRAME, format_type);
        EXPECT_EQ(TFileCompressType::LZ4FRAME, compress_type);
    }
    {
        // CSV, LZOP
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("CSV", "LZOP", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_CSV_LZOP, format_type);
        EXPECT_EQ(TFileCompressType::LZO, compress_type);
    }
    {
        // CSV, DEFLATE
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("CSV", "DEFLATE", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_CSV_DEFLATE, format_type);
        EXPECT_EQ(TFileCompressType::DEFLATE, compress_type);
    }
    {
        // JSON, ""
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("JSON", "", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_JSON, format_type);
        EXPECT_EQ(TFileCompressType::PLAIN, compress_type);
    }
    {
        // JSON, GZ
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("JSON", "GZ", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_JSON, format_type);
        EXPECT_EQ(TFileCompressType::GZ, compress_type);
    }
    {
        // JSON, LZO
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("JSON", "LZOP", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_JSON, format_type);
        EXPECT_EQ(TFileCompressType::LZO, compress_type);
    }
    {
        // JSON, BZ2
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("JSON", "BZ2", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_JSON, format_type);
        EXPECT_EQ(TFileCompressType::BZ2, compress_type);
    }
    {
        // JSON, LZ4
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("JSON", "LZ4", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_JSON, format_type);
        EXPECT_EQ(TFileCompressType::LZ4FRAME, compress_type);
    }
    {
        // JSON, LZ4_BLOCK
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("JSON", "LZ4_BLOCK", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_JSON, format_type);
        EXPECT_EQ(TFileCompressType::LZ4BLOCK, compress_type);
    }
    {
        // JSON, LZOP
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("JSON", "LZOP", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_JSON, format_type);
        EXPECT_EQ(TFileCompressType::LZO, compress_type);
    }
    {
        // JSON, SNAPPY_BLOCK
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("JSON", "SNAPPY_BLOCK", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_JSON, format_type);
        EXPECT_EQ(TFileCompressType::SNAPPYBLOCK, compress_type);
    }
    {
        // JSON, DEFLATE
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("JSON", "DEFLATE", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_JSON, format_type);
        EXPECT_EQ(TFileCompressType::DEFLATE, compress_type);
    }
    {
        // JSON, unkonw
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("JSON", "UNKNOWN", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_JSON, format_type);
        EXPECT_EQ(TFileCompressType::PLAIN, compress_type);
    }
    {
        // PARQUET, ""
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("PARQUET", "", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_PARQUET, format_type);
        EXPECT_EQ(TFileCompressType::PLAIN, compress_type);
    }
    {
        // ORC, ""
        TFileFormatType::type format_type;
        TFileCompressType::type compress_type;
        LoadUtil::parse_format("ORC", "", &format_type, &compress_type);
        EXPECT_EQ(TFileFormatType::FORMAT_ORC, format_type);
        EXPECT_EQ(TFileCompressType::PLAIN, compress_type);
    }
}

} // namespace doris
