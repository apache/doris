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

#include <string>
#include <string_view>

#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_varbinary.h"
#include "exprs/function/function_test_util.h"

namespace doris {

using namespace ut_type;

TEST(function_character_encoding_test, encode_supported_charsets) {
    // The UTF-16 byte pairs 0x4E2D and 0x2D4E are "N-" and "-N" as raw bytes.
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{std::string("A"), std::string("US-ASCII")}, VARBINARY("A")},
            {{std::string("é"), std::string("ISO-8859-1")}, VARBINARY("\xE9")},
            {{std::string("中"), std::string("UTF-8")}, VARBINARY("\xE4\xB8\xAD")},
            {{std::string("中"), std::string("UTF-16BE")}, VARBINARY("N-")},
            {{std::string("中"), std::string("UTF-16LE")}, VARBINARY("-N")},
            {{std::string("中"), std::string("UTF-16")}, VARBINARY("\xFE\xFF\x4E\x2D")},
            {{std::string("😀"), std::string("utf-16be")},
             VARBINARY(std::string_view("\xD8\x3D\xDE\x00", 4))},
            {{std::string("A\0中", 5), std::string("UTF-8")},
             VARBINARY(std::string_view("A\0\xE4\xB8\xAD", 5))},
            {{std::string(""), std::string("UTF-16")}, VARBINARY("")},
            {{Null(), std::string("UTF-8")}, Null()},
            {{std::string("text"), Null()}, Null()},
    };

    check_function_all_arg_comb<DataTypeVarbinary, true>("encode", input_types, data_set);
}

TEST(function_character_encoding_test, decode_supported_charsets) {
    // The UTF-16 byte pairs 0x4E2D and 0x2D4E are "N-" and "-N" as raw bytes.
    InputTypeSet input_types = {PrimitiveType::TYPE_VARBINARY, PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{VARBINARY("A"), std::string("US-ASCII")}, std::string("A")},
            {{VARBINARY("\xE9"), std::string("ISO-8859-1")}, std::string("é")},
            {{VARBINARY("\xE4\xB8\xAD"), std::string("UTF-8")}, std::string("中")},
            {{VARBINARY("N-"), std::string("UTF-16BE")}, std::string("中")},
            {{VARBINARY("-N"), std::string("UTF-16LE")}, std::string("中")},
            {{VARBINARY("\xFE\xFF\x4E\x2D"), std::string("UTF-16")}, std::string("中")},
            {{VARBINARY("\xFF\xFE\x2D\x4E"), std::string("utf-16")}, std::string("中")},
            {{VARBINARY("N-"), std::string("UTF-16")}, std::string("中")},
            {{VARBINARY("\xFE\xFF"), std::string("UTF-16")}, std::string("")},
            {{VARBINARY(std::string_view("\xD8\x3D\xDE\x00", 4)), std::string("UTF-16BE")},
             std::string("😀")},
            {{VARBINARY(std::string_view("A\0\xE4\xB8\xAD", 5)), std::string("UTF-8")},
             std::string("A\0中", 5)},
            {{VARBINARY(""), std::string("UTF-16")}, std::string("")},
            {{Null(), std::string("UTF-8")}, Null()},
            {{VARBINARY("text"), Null()}, Null()},
    };

    check_function_all_arg_comb<DataTypeString, true>("decode", input_types, data_set);
}

TEST(function_character_encoding_test, rejects_invalid_conversions) {
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
        DataSet data_set = {
                {{std::string("text"), std::string("GBK")}, VARBINARY("")},
        };

        Status status = check_function<DataTypeVarbinary, true>("encode", input_types, data_set, -1,
                                                                -1, true);
        ASSERT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
        EXPECT_NE(status.to_string().find("Unsupported character set"), std::string::npos);
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
        DataSet data_set = {
                {{std::string("中"), std::string("US-ASCII")}, VARBINARY("")},
        };

        Status status = check_function<DataTypeVarbinary, true>("encode", input_types, data_set, -1,
                                                                -1, true);
        ASSERT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
        EXPECT_NE(status.to_string().find("Character conversion using 'US-ASCII' failed"),
                  std::string::npos);
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARBINARY, PrimitiveType::TYPE_VARCHAR};
        DataSet data_set = {
                {{VARBINARY("\xE4\xB8"), std::string("UTF-8")}, std::string("")},
        };

        Status status =
                check_function<DataTypeString, true>("decode", input_types, data_set, -1, -1, true);
        ASSERT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
        EXPECT_NE(status.to_string().find("Character conversion using 'UTF-8' failed"),
                  std::string::npos);
    }
}

TEST(function_character_encoding_test, streaming_boundaries_and_row_reuse) {
    // Cross the pivot boundary and force output expansion, including pending surrogate pairs.
    for (size_t length : {1, 15, 1023, 1024, 1025, 65535}) {
        std::string ascii(length, 'A');
        std::string utf16;
        for (size_t i = 0; i < length; ++i) {
            utf16.append("\0A", 2);
        }
        const std::string utf16_bom = std::string("\xFE\xFF", 2) + utf16;
        const std::string supplementary = ascii + "😀";
        const std::string supplementary_utf16 = utf16 + std::string("\xD8\x3D\xDE\0", 4);
        DataSet encoded = {
                {{ascii, std::string("UTF-16BE")}, VARBINARY(utf16)},
                {{supplementary, std::string("UTF-16BE")}, VARBINARY(supplementary_utf16)},
                {{Null(), std::string("UTF-16BE")}, Null()},
                {{std::string(""), std::string("UTF-16BE")}, VARBINARY("")},
                {{ascii, std::string("UTF-16")}, VARBINARY(utf16_bom)},
                {{std::string("A"), std::string("UTF-16BE")},
                 VARBINARY(std::string_view("\0A", 2))},
        };
        check_function_all_arg_comb<DataTypeVarbinary, true>(
                "encode", {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR}, encoded);

        std::string latin1(length, '\xE9');
        std::string expanded;
        for (size_t i = 0; i < length; ++i) {
            expanded += "é";
        }
        DataSet decoded = {
                {{VARBINARY(latin1), std::string("ISO-8859-1")}, expanded},
                {{VARBINARY(supplementary_utf16), std::string("UTF-16BE")}, supplementary},
                {{Null(), std::string("ISO-8859-1")}, Null()},
                {{VARBINARY(""), std::string("ISO-8859-1")}, std::string("")},
                {{VARBINARY(utf16_bom), std::string("UTF-16")}, ascii},
                {{VARBINARY("\xFF\xFE\x2D\x4E"), std::string("UTF-16")}, std::string("中")},
                {{VARBINARY("\xFE\xFF"), std::string("UTF-16")}, std::string("")},
                {{VARBINARY("\xE9"), std::string("ISO-8859-1")}, std::string("é")},
        };
        check_function_all_arg_comb<DataTypeString, true>(
                "decode", {PrimitiveType::TYPE_VARBINARY, PrimitiveType::TYPE_VARCHAR}, decoded);
    }
}

TEST(function_character_encoding_test, rejects_invalid_input_after_streaming) {
    const std::string invalid_utf8 = std::string(4096, 'A') + "\xE4\xB8";
    const std::string unrepresentable = std::string(4096, 'A') + "中";
    for (const auto& input : {invalid_utf8, unrepresentable}) {
        DataSet data_set = {{{input, std::string("US-ASCII")}, VARBINARY("")}};
        Status status = check_function<DataTypeVarbinary, true>(
                "encode", {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR}, data_set, -1,
                -1, true);
        ASSERT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
    }
    const std::string expanding_invalid_utf8 = std::string(50000, 'A') + "\xE4\xB8";
    DataSet invalid_encode = {{{expanding_invalid_utf8, std::string("UTF-16BE")}, VARBINARY("")}};
    Status encode_status = check_function<DataTypeVarbinary, true>(
            "encode", {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR}, invalid_encode,
            -1, -1, true);
    ASSERT_TRUE(encode_status.is<ErrorCode::INVALID_ARGUMENT>()) << encode_status;

    std::string invalid_utf16;
    for (size_t i = 0; i < 25000; ++i) {
        invalid_utf16 += "N-"; // The UTF-16BE byte pair for 中.
    }
    invalid_utf16.append("\xD8\x3D", 2); // An unpaired high surrogate after several pivot fills.
    DataSet data_set = {{{VARBINARY(invalid_utf16), std::string("UTF-16BE")}, std::string("")}};
    Status status = check_function<DataTypeString, true>(
            "decode", {PrimitiveType::TYPE_VARBINARY, PrimitiveType::TYPE_VARCHAR}, data_set, -1,
            -1, true);
    ASSERT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
}

} // namespace doris
