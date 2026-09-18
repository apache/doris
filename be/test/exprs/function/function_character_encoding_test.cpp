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

} // namespace doris
