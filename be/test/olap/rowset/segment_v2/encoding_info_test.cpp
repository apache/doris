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
#include "olap/rowset/segment_v2/binary_dict_page_pre_decoder.h"
#include "olap/rowset/segment_v2/binary_plain_page_v2_pre_decoder.h"
#include "olap/rowset/segment_v2/bitshuffle_page_pre_decoder.h"
#include "olap/types.h"
#include "runtime/exec_env.h"

namespace doris {
namespace segment_v2 {

class EncodingInfoTest : public testing::Test {
public:
    EncodingInfoTest() {}
    virtual ~EncodingInfoTest() {}
};

TEST_F(EncodingInfoTest, normal) {
    const auto* type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_BIGINT>();
    const EncodingInfo* encoding_info = nullptr;
    EncodingPreference encoding_preference;
    auto status = EncodingInfo::get(type_info->type(), PLAIN_ENCODING, encoding_preference,
                                    &encoding_info);
    EXPECT_TRUE(status.ok());
    EXPECT_NE(nullptr, encoding_info);
}

TEST_F(EncodingInfoTest, no_encoding) {
    const auto* type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_BIGINT>();
    const EncodingInfo* encoding_info = nullptr;
    EncodingPreference encoding_preference;
    auto status = EncodingInfo::get(type_info->type(), DICT_ENCODING, encoding_preference,
                                    &encoding_info);
    EXPECT_FALSE(status.ok());
}

TEST_F(EncodingInfoTest, test_use_plain_binary_v2_config) {
    // Helper lambda to test string/JSON types with DICT_ENCODING as default
    auto test_dict_type_encoding = [](FieldType type, const std::string& type_name) {
        const auto* type_info = get_scalar_type_info(type);

        // Test with BINARY_PLAIN_ENCODING_V1 (default)
        // String and JSON types default to DICT_ENCODING
        EncodingPreference pref_v1;
        pref_v1.binary_plain_encoding_default_impl =
                BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V1;
        EncodingTypePB encoding_type =
                EncodingInfo::get_default_encoding(type_info->type(), pref_v1, false);
        EXPECT_EQ(DICT_ENCODING, encoding_type)
                << "Type " << type_name << " should use DICT_ENCODING with V1 preference";

        // Test with BINARY_PLAIN_ENCODING_V2
        // Preference doesn't affect DICT_ENCODING types
        EncodingPreference pref_v2;
        pref_v2.binary_plain_encoding_default_impl =
                BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V2;
        encoding_type = EncodingInfo::get_default_encoding(type_info->type(), pref_v2, false);
        EXPECT_EQ(DICT_ENCODING, encoding_type)
                << "Type " << type_name << " should still use DICT_ENCODING with V2 preference";
    };

    // Helper lambda to test aggregate state types with PLAIN_ENCODING as default
    auto test_plain_type_encoding = [](FieldType type, const std::string& type_name) {
        const auto* type_info = get_scalar_type_info(type);

        // Test with BINARY_PLAIN_ENCODING_V1 (default)
        EncodingPreference pref_v1;
        pref_v1.binary_plain_encoding_default_impl =
                BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V1;
        EncodingTypePB encoding_type =
                EncodingInfo::get_default_encoding(type_info->type(), pref_v1, false);
        EXPECT_EQ(PLAIN_ENCODING, encoding_type)
                << "Type " << type_name << " should use PLAIN_ENCODING with V1 preference";

        // Test with BINARY_PLAIN_ENCODING_V2
        EncodingPreference pref_v2;
        pref_v2.binary_plain_encoding_default_impl =
                BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V2;
        encoding_type = EncodingInfo::get_default_encoding(type_info->type(), pref_v2, false);
        EXPECT_EQ(PLAIN_ENCODING_V2, encoding_type)
                << "Type " << type_name << " should use PLAIN_ENCODING_V2 with V2 preference";
    };

    // Test string types (default to DICT_ENCODING, not affected by preference)
    test_dict_type_encoding(FieldType::OLAP_FIELD_TYPE_VARCHAR, "VARCHAR");
    test_dict_type_encoding(FieldType::OLAP_FIELD_TYPE_STRING, "STRING");
    test_dict_type_encoding(FieldType::OLAP_FIELD_TYPE_CHAR, "CHAR");

    // Test JSON/variant types (default to DICT_ENCODING, not affected by preference)
    test_dict_type_encoding(FieldType::OLAP_FIELD_TYPE_JSONB, "JSONB");
    test_dict_type_encoding(FieldType::OLAP_FIELD_TYPE_VARIANT, "VARIANT");

    // Test aggregate state types (default to PLAIN_ENCODING, affected by preference)
    test_plain_type_encoding(FieldType::OLAP_FIELD_TYPE_HLL, "HLL");
    test_plain_type_encoding(FieldType::OLAP_FIELD_TYPE_BITMAP, "BITMAP");
    test_plain_type_encoding(FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE, "QUANTILE_STATE");
    test_plain_type_encoding(FieldType::OLAP_FIELD_TYPE_AGG_STATE, "AGG_STATE");

    // Test non-binary type (BIGINT) - should not be affected by binary preference
    const auto* bigint_type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_BIGINT>();

    // Test with plain encoding disabled for integers (default)
    EncodingPreference pref_plain_disabled;
    pref_plain_disabled.integer_type_default_use_plain_encoding = false;
    pref_plain_disabled.binary_plain_encoding_default_impl =
            BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V1;
    EncodingTypePB encoding_type = EncodingInfo::get_default_encoding(bigint_type_info->type(),
                                                                      pref_plain_disabled, false);
    EXPECT_EQ(BIT_SHUFFLE, encoding_type);

    // Test with plain encoding enabled for integers
    EncodingPreference pref_plain_enabled;
    pref_plain_enabled.integer_type_default_use_plain_encoding = true;
    pref_plain_enabled.binary_plain_encoding_default_impl =
            BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V1;
    encoding_type =
            EncodingInfo::get_default_encoding(bigint_type_info->type(), pref_plain_enabled, false);
    EXPECT_EQ(PLAIN_ENCODING, encoding_type);

    // Verify binary preference doesn't affect integer types
    pref_plain_enabled.binary_plain_encoding_default_impl =
            BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V2;
    encoding_type =
            EncodingInfo::get_default_encoding(bigint_type_info->type(), pref_plain_enabled, false);
    EXPECT_EQ(PLAIN_ENCODING, encoding_type); // Should still be PLAIN_ENCODING
}

// Comprehensive test for _data_page_pre_decoder for all encoding types
TEST_F(EncodingInfoTest, test_all_pre_decoders) {
    EncodingPreference encoding_preference;

    // Test BIT_SHUFFLE encoding - should have BitShufflePagePreDecoder
    // Test various integer types
    std::vector<FieldType> bitshuffle_types = {
            FieldType::OLAP_FIELD_TYPE_TINYINT,      FieldType::OLAP_FIELD_TYPE_SMALLINT,
            FieldType::OLAP_FIELD_TYPE_INT,          FieldType::OLAP_FIELD_TYPE_BIGINT,
            FieldType::OLAP_FIELD_TYPE_LARGEINT,     FieldType::OLAP_FIELD_TYPE_FLOAT,
            FieldType::OLAP_FIELD_TYPE_DOUBLE,       FieldType::OLAP_FIELD_TYPE_BOOL,
            FieldType::OLAP_FIELD_TYPE_DATE,         FieldType::OLAP_FIELD_TYPE_DATEV2,
            FieldType::OLAP_FIELD_TYPE_DATETIMEV2,   FieldType::OLAP_FIELD_TYPE_DATETIME,
            FieldType::OLAP_FIELD_TYPE_DECIMAL,      FieldType::OLAP_FIELD_TYPE_DECIMAL32,
            FieldType::OLAP_FIELD_TYPE_DECIMAL64,    FieldType::OLAP_FIELD_TYPE_DECIMAL128I,
            FieldType::OLAP_FIELD_TYPE_DECIMAL256,   FieldType::OLAP_FIELD_TYPE_IPV4,
            FieldType::OLAP_FIELD_TYPE_IPV6,         FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT,
            FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT,
    };

    for (auto type : bitshuffle_types) {
        const EncodingInfo* encoding_info = nullptr;
        auto status = EncodingInfo::get(type, BIT_SHUFFLE, encoding_preference, &encoding_info);
        if (status.ok()) {
            ASSERT_NE(nullptr, encoding_info);
            auto* pre_decoder = encoding_info->get_data_page_pre_decoder();
            ASSERT_NE(nullptr, pre_decoder) << "Type " << static_cast<int>(type)
                                            << " with BIT_SHUFFLE should have pre_decoder";
            auto* bitshuffle_decoder = dynamic_cast<BitShufflePagePreDecoder*>(pre_decoder);
            EXPECT_NE(nullptr, bitshuffle_decoder)
                    << "Type " << static_cast<int>(type)
                    << " with BIT_SHUFFLE should have BitShufflePagePreDecoder";
        }
    }

    // Test DICT_ENCODING - should have BinaryDictPagePreDecoder
    std::vector<FieldType> dict_types = {
            FieldType::OLAP_FIELD_TYPE_CHAR,    FieldType::OLAP_FIELD_TYPE_VARCHAR,
            FieldType::OLAP_FIELD_TYPE_STRING,  FieldType::OLAP_FIELD_TYPE_JSONB,
            FieldType::OLAP_FIELD_TYPE_VARIANT,
    };

    for (auto type : dict_types) {
        const EncodingInfo* encoding_info = nullptr;
        auto status = EncodingInfo::get(type, DICT_ENCODING, encoding_preference, &encoding_info);
        ASSERT_TRUE(status.ok()) << "Type " << static_cast<int>(type)
                                 << " should support DICT_ENCODING";
        ASSERT_NE(nullptr, encoding_info);
        auto* pre_decoder = encoding_info->get_data_page_pre_decoder();
        ASSERT_NE(nullptr, pre_decoder) << "Type " << static_cast<int>(type)
                                        << " with DICT_ENCODING should have pre_decoder";
        auto* dict_decoder = dynamic_cast<BinaryDictPagePreDecoder*>(pre_decoder);
        EXPECT_NE(nullptr, dict_decoder)
                << "Type " << static_cast<int>(type)
                << " with DICT_ENCODING should have BinaryDictPagePreDecoder";
    }

    // Test PLAIN_ENCODING_V2 with Slice types - should have BinaryPlainPageV2PreDecoder
    std::vector<FieldType> plain_v2_types = {
            FieldType::OLAP_FIELD_TYPE_CHAR,      FieldType::OLAP_FIELD_TYPE_VARCHAR,
            FieldType::OLAP_FIELD_TYPE_STRING,    FieldType::OLAP_FIELD_TYPE_JSONB,
            FieldType::OLAP_FIELD_TYPE_VARIANT,   FieldType::OLAP_FIELD_TYPE_HLL,
            FieldType::OLAP_FIELD_TYPE_BITMAP,    FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE,
            FieldType::OLAP_FIELD_TYPE_AGG_STATE,
    };

    for (auto type : plain_v2_types) {
        const EncodingInfo* encoding_info = nullptr;
        auto status =
                EncodingInfo::get(type, PLAIN_ENCODING_V2, encoding_preference, &encoding_info);
        ASSERT_TRUE(status.ok()) << "Type " << static_cast<int>(type)
                                 << " should support PLAIN_ENCODING_V2";
        ASSERT_NE(nullptr, encoding_info);
        auto* pre_decoder = encoding_info->get_data_page_pre_decoder();
        ASSERT_NE(nullptr, pre_decoder) << "Type " << static_cast<int>(type)
                                        << " with PLAIN_ENCODING_V2 should have pre_decoder";
        auto* v2_decoder = dynamic_cast<BinaryPlainPageV2PreDecoder*>(pre_decoder);
        EXPECT_NE(nullptr, v2_decoder) << "Type " << static_cast<int>(type)
                                       << " with PLAIN_ENCODING_V2 should have "
                                          "BinaryPlainPageV2PreDecoder";
    }

    // Test PLAIN_ENCODING - should NOT have pre_decoder
    std::vector<FieldType> plain_encoding_types = {
            FieldType::OLAP_FIELD_TYPE_TINYINT,
            FieldType::OLAP_FIELD_TYPE_SMALLINT,
            FieldType::OLAP_FIELD_TYPE_INT,
            FieldType::OLAP_FIELD_TYPE_BIGINT,
            FieldType::OLAP_FIELD_TYPE_LARGEINT,
            FieldType::OLAP_FIELD_TYPE_FLOAT,
            FieldType::OLAP_FIELD_TYPE_DOUBLE,
            FieldType::OLAP_FIELD_TYPE_BOOL,
            FieldType::OLAP_FIELD_TYPE_DATE,
            FieldType::OLAP_FIELD_TYPE_DATEV2,
            FieldType::OLAP_FIELD_TYPE_DATETIMEV2,
            FieldType::OLAP_FIELD_TYPE_DATETIME,
            FieldType::OLAP_FIELD_TYPE_DECIMAL,
            FieldType::OLAP_FIELD_TYPE_DECIMAL32,
            FieldType::OLAP_FIELD_TYPE_DECIMAL64,
            FieldType::OLAP_FIELD_TYPE_DECIMAL128I,
            FieldType::OLAP_FIELD_TYPE_DECIMAL256,
            FieldType::OLAP_FIELD_TYPE_IPV4,
            FieldType::OLAP_FIELD_TYPE_IPV6,
            FieldType::OLAP_FIELD_TYPE_CHAR,
            FieldType::OLAP_FIELD_TYPE_VARCHAR,
            FieldType::OLAP_FIELD_TYPE_STRING,
            FieldType::OLAP_FIELD_TYPE_JSONB,
            FieldType::OLAP_FIELD_TYPE_VARIANT,
            FieldType::OLAP_FIELD_TYPE_HLL,
            FieldType::OLAP_FIELD_TYPE_BITMAP,
            FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE,
            FieldType::OLAP_FIELD_TYPE_AGG_STATE,
    };

    for (auto type : plain_encoding_types) {
        const EncodingInfo* encoding_info = nullptr;
        auto status = EncodingInfo::get(type, PLAIN_ENCODING, encoding_preference, &encoding_info);
        if (status.ok() && encoding_info != nullptr) {
            auto* pre_decoder = encoding_info->get_data_page_pre_decoder();
            EXPECT_EQ(nullptr, pre_decoder) << "Type " << static_cast<int>(type)
                                            << " with PLAIN_ENCODING should NOT have pre_decoder";
        }
    }

    // Test FOR_ENCODING - should NOT have pre_decoder
    std::vector<FieldType> for_encoding_types = {
            FieldType::OLAP_FIELD_TYPE_TINYINT,  FieldType::OLAP_FIELD_TYPE_SMALLINT,
            FieldType::OLAP_FIELD_TYPE_INT,      FieldType::OLAP_FIELD_TYPE_BIGINT,
            FieldType::OLAP_FIELD_TYPE_LARGEINT, FieldType::OLAP_FIELD_TYPE_DATE,
            FieldType::OLAP_FIELD_TYPE_DATEV2,   FieldType::OLAP_FIELD_TYPE_DATETIMEV2,
            FieldType::OLAP_FIELD_TYPE_DATETIME,
    };

    for (auto type : for_encoding_types) {
        const EncodingInfo* encoding_info = nullptr;
        auto status = EncodingInfo::get(type, FOR_ENCODING, encoding_preference, &encoding_info);
        if (status.ok() && encoding_info != nullptr) {
            auto* pre_decoder = encoding_info->get_data_page_pre_decoder();
            EXPECT_EQ(nullptr, pre_decoder) << "Type " << static_cast<int>(type)
                                            << " with FOR_ENCODING should NOT have pre_decoder";
        }
    }

    // Test PREFIX_ENCODING - should NOT have pre_decoder
    std::vector<FieldType> prefix_encoding_types = {
            FieldType::OLAP_FIELD_TYPE_CHAR,    FieldType::OLAP_FIELD_TYPE_VARCHAR,
            FieldType::OLAP_FIELD_TYPE_STRING,  FieldType::OLAP_FIELD_TYPE_JSONB,
            FieldType::OLAP_FIELD_TYPE_VARIANT,
    };

    for (auto type : prefix_encoding_types) {
        const EncodingInfo* encoding_info = nullptr;
        auto status = EncodingInfo::get(type, PREFIX_ENCODING, encoding_preference, &encoding_info);
        if (status.ok() && encoding_info != nullptr) {
            auto* pre_decoder = encoding_info->get_data_page_pre_decoder();
            EXPECT_EQ(nullptr, pre_decoder) << "Type " << static_cast<int>(type)
                                            << " with PREFIX_ENCODING should NOT have pre_decoder";
        }
    }

    // Test RLE - should NOT have pre_decoder (only for BOOL)
    {
        const EncodingInfo* encoding_info = nullptr;
        auto status = EncodingInfo::get(FieldType::OLAP_FIELD_TYPE_BOOL, RLE, encoding_preference,
                                        &encoding_info);
        if (status.ok() && encoding_info != nullptr) {
            auto* pre_decoder = encoding_info->get_data_page_pre_decoder();
            EXPECT_EQ(nullptr, pre_decoder) << "BOOL with RLE should NOT have pre_decoder";
        }
    }
}

} // namespace segment_v2
} // namespace doris
