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

#include "olap/rowset/segment_v2/encoding_info.h"

#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <memory>

#include "common/config.h"
#include "gtest/gtest_pred_impl.h"
#include "olap/olap_common.h"
#include "olap/types.h"
#include "runtime/exec_env.h"

namespace doris {
namespace segment_v2 {

class EncodingInfoTest : public testing::Test {
public:
    EncodingInfoTest() {
        _resolver = std::make_unique<segment_v2::EncodingInfoResolver>();
        ExecEnv::GetInstance()->_encoding_info_resolver = _resolver.get();
    }
    virtual ~EncodingInfoTest() { ExecEnv::GetInstance()->_encoding_info_resolver = nullptr; }

private:
    std::unique_ptr<segment_v2::EncodingInfoResolver> _resolver;
};

TEST_F(EncodingInfoTest, normal) {
    const auto* type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_BIGINT>();
    const EncodingInfo* encoding_info = nullptr;
    auto status = EncodingInfo::get(type_info->type(), PLAIN_ENCODING, &encoding_info);
    EXPECT_TRUE(status.ok());
    EXPECT_NE(nullptr, encoding_info);
}

TEST_F(EncodingInfoTest, no_encoding) {
    const auto* type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_BIGINT>();
    const EncodingInfo* encoding_info = nullptr;
    auto status = EncodingInfo::get(type_info->type(), DICT_ENCODING, &encoding_info);
    EXPECT_FALSE(status.ok());
}

TEST_F(EncodingInfoTest, test_use_plain_binary_v2_config) {
    // Helper lambda to test string/JSON types with DICT_ENCODING as default
    auto test_dict_type_encoding = [](FieldType type, const std::string& type_name) {
        const auto* type_info = get_scalar_type_info(type);

        // Test with use_plain_binary_v2 = false (default)
        // String and JSON types default to DICT_ENCODING
        config::use_plain_binary_v2 = false;
        EncodingTypePB encoding_type = EncodingInfo::get_default_encoding(type_info->type(), false);
        EXPECT_EQ(DICT_ENCODING, encoding_type)
                << "Type " << type_name << " should use DICT_ENCODING when config is false";

        // Test with use_plain_binary_v2 = true
        // Config doesn't affect DICT_ENCODING types
        config::use_plain_binary_v2 = true;
        encoding_type = EncodingInfo::get_default_encoding(type_info->type(), false);
        EXPECT_EQ(DICT_ENCODING, encoding_type)
                << "Type " << type_name << " should still use DICT_ENCODING when config is true";
    };

    // Helper lambda to test aggregate state types with PLAIN_ENCODING as default
    auto test_plain_type_encoding = [](FieldType type, const std::string& type_name) {
        const auto* type_info = get_scalar_type_info(type);

        // Test with use_plain_binary_v2 = false (default)
        config::use_plain_binary_v2 = false;
        EncodingTypePB encoding_type = EncodingInfo::get_default_encoding(type_info->type(), false);
        EXPECT_EQ(PLAIN_ENCODING, encoding_type)
                << "Type " << type_name << " should use PLAIN_ENCODING when config is false";

        // Test with use_plain_binary_v2 = true
        config::use_plain_binary_v2 = true;
        encoding_type = EncodingInfo::get_default_encoding(type_info->type(), false);
        EXPECT_EQ(PLAIN_ENCODING_V2, encoding_type)
                << "Type " << type_name << " should use PLAIN_ENCODING_V2 when config is true";
    };

    // Test string types (default to DICT_ENCODING, not affected by config)
    test_dict_type_encoding(FieldType::OLAP_FIELD_TYPE_VARCHAR, "VARCHAR");
    test_dict_type_encoding(FieldType::OLAP_FIELD_TYPE_STRING, "STRING");
    test_dict_type_encoding(FieldType::OLAP_FIELD_TYPE_CHAR, "CHAR");

    // Test JSON/variant types (default to DICT_ENCODING, not affected by config)
    test_dict_type_encoding(FieldType::OLAP_FIELD_TYPE_JSONB, "JSONB");
    test_dict_type_encoding(FieldType::OLAP_FIELD_TYPE_VARIANT, "VARIANT");

    // Test aggregate state types (default to PLAIN_ENCODING, affected by config)
    test_plain_type_encoding(FieldType::OLAP_FIELD_TYPE_HLL, "HLL");
    test_plain_type_encoding(FieldType::OLAP_FIELD_TYPE_BITMAP, "BITMAP");
    test_plain_type_encoding(FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE, "QUANTILE_STATE");
    test_plain_type_encoding(FieldType::OLAP_FIELD_TYPE_AGG_STATE, "AGG_STATE");

    // Test non-binary type (BIGINT) - should not be affected by the config
    const auto* bigint_type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_BIGINT>();

    config::use_plain_binary_v2 = false;
    EncodingTypePB encoding_type =
            EncodingInfo::get_default_encoding(bigint_type_info->type(), false);
    EXPECT_EQ(BIT_SHUFFLE, encoding_type);

    config::use_plain_binary_v2 = true;
    encoding_type = EncodingInfo::get_default_encoding(bigint_type_info->type(), false);
    EXPECT_EQ(BIT_SHUFFLE, encoding_type); // Should remain BIT_SHUFFLE

    // Reset to default
    config::use_plain_binary_v2 = false;
}

} // namespace segment_v2
} // namespace doris
