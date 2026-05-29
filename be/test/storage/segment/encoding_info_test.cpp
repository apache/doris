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

#include "storage/segment/encoding_info.h"

#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <memory>

#include "common/config.h"
#include "gtest/gtest_pred_impl.h"
#include "runtime/exec_env.h"
#include "storage/olap_common.h"
#include "storage/segment/binary_dict_page_pre_decoder.h"
#include "storage/segment/binary_plain_page_v2_pre_decoder.h"
#include "storage/segment/bitshuffle_page_pre_decoder.h"
#include "storage/types.h"

namespace doris {
namespace segment_v2 {

class EncodingInfoTest : public testing::Test {
public:
    EncodingInfoTest() {}
    virtual ~EncodingInfoTest() {}

protected:
    // EncodingInfo has friended EncodingInfoTest so this fixture can poke the private
    // get_v2/v3_default_encoding lookup tables. TEST_F bodies subclass this fixture and
    // call these helpers (they don't get the friend permission directly).
    static EncodingTypePB get_v2_default_encoding(FieldType t) {
        return EncodingInfo::get_v2_default_encoding(t);
    }
    static EncodingTypePB get_v3_default_encoding(FieldType t) {
        return EncodingInfo::get_v3_default_encoding(t);
    }
};

TEST_F(EncodingInfoTest, normal) {
    constexpr FieldType type = FieldType::OLAP_FIELD_TYPE_BIGINT;
    const EncodingInfo* encoding_info = nullptr;
    auto status = EncodingInfo::get(type, PLAIN_ENCODING, &encoding_info);
    EXPECT_TRUE(status.ok());
    EXPECT_NE(nullptr, encoding_info);
}

TEST_F(EncodingInfoTest, no_encoding) {
    constexpr FieldType type = FieldType::OLAP_FIELD_TYPE_BIGINT;
    const EncodingInfo* encoding_info = nullptr;
    auto status = EncodingInfo::get(type, DICT_ENCODING, &encoding_info);
    EXPECT_FALSE(status.ok());
}

TEST_F(EncodingInfoTest, v2_vs_v3_defaults) {
    // String / JSON / variant: default is DICT_ENCODING for both V2 and V3.
    auto check_same = [](FieldType type, const std::string& name, EncodingTypePB expected) {
        EXPECT_EQ(expected, get_v2_default_encoding(type)) << name << " v2 default";
        EXPECT_EQ(expected, get_v3_default_encoding(type)) << name << " v3 default";
    };
    check_same(FieldType::OLAP_FIELD_TYPE_VARCHAR, "VARCHAR", DICT_ENCODING);
    check_same(FieldType::OLAP_FIELD_TYPE_STRING, "STRING", DICT_ENCODING);
    check_same(FieldType::OLAP_FIELD_TYPE_CHAR, "CHAR", DICT_ENCODING);
    check_same(FieldType::OLAP_FIELD_TYPE_JSONB, "JSONB", DICT_ENCODING);
    check_same(FieldType::OLAP_FIELD_TYPE_VARIANT, "VARIANT", DICT_ENCODING);

    // Aggregate/binary-flavored types: V2=PLAIN, V3=PLAIN_V2.
    auto check_split = [](FieldType type, const std::string& name) {
        EXPECT_EQ(PLAIN_ENCODING, get_v2_default_encoding(type)) << name << " v2 default";
        EXPECT_EQ(PLAIN_ENCODING_V2, get_v3_default_encoding(type)) << name << " v3 default";
    };
    check_split(FieldType::OLAP_FIELD_TYPE_HLL, "HLL");
    check_split(FieldType::OLAP_FIELD_TYPE_BITMAP, "BITMAP");
    check_split(FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE, "QUANTILE_STATE");
    check_split(FieldType::OLAP_FIELD_TYPE_AGG_STATE, "AGG_STATE");

    // Signed integers: V2=BIT_SHUFFLE, V3=PLAIN.
    constexpr FieldType bigint_type = FieldType::OLAP_FIELD_TYPE_BIGINT;
    EXPECT_EQ(BIT_SHUFFLE, get_v2_default_encoding(bigint_type));
    EXPECT_EQ(PLAIN_ENCODING, get_v3_default_encoding(bigint_type));

    // Value-seek default is only registered for VARCHAR (the only production caller).
    EXPECT_EQ(PREFIX_ENCODING,
              EncodingInfo::get_index_column_encoding(FieldType::OLAP_FIELD_TYPE_VARCHAR));
    EXPECT_EQ(UNKNOWN_ENCODING, EncodingInfo::get_index_column_encoding(bigint_type));
}

// Comprehensive test for _data_page_pre_decoder for all encoding types
TEST_F(EncodingInfoTest, test_all_pre_decoders) {
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
        auto status = EncodingInfo::get(type, BIT_SHUFFLE, &encoding_info);
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
        auto status = EncodingInfo::get(type, DICT_ENCODING, &encoding_info);
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
        auto status = EncodingInfo::get(type, PLAIN_ENCODING_V2, &encoding_info);
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
        auto status = EncodingInfo::get(type, PLAIN_ENCODING, &encoding_info);
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
        auto status = EncodingInfo::get(type, FOR_ENCODING, &encoding_info);
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
        auto status = EncodingInfo::get(type, PREFIX_ENCODING, &encoding_info);
        if (status.ok() && encoding_info != nullptr) {
            auto* pre_decoder = encoding_info->get_data_page_pre_decoder();
            EXPECT_EQ(nullptr, pre_decoder) << "Type " << static_cast<int>(type)
                                            << " with PREFIX_ENCODING should NOT have pre_decoder";
        }
    }

    // Test RLE - should NOT have pre_decoder (only for BOOL)
    {
        const EncodingInfo* encoding_info = nullptr;
        auto status = EncodingInfo::get(FieldType::OLAP_FIELD_TYPE_BOOL, RLE, &encoding_info);
        if (status.ok() && encoding_info != nullptr) {
            auto* pre_decoder = encoding_info->get_data_page_pre_decoder();
            EXPECT_EQ(nullptr, pre_decoder) << "BOOL with RLE should NOT have pre_decoder";
        }
    }
}

// ============================================================================
// Behavior-locking tests. The expectation tables below are the authoritative
// contract for what each get_*_encoding API returns per FieldType. Changing
// a value here is a deliberate behavior change and must be justified.
// ============================================================================

namespace {

// (type, expected_encoding) row for a default-encoding test.
struct DefaultExpectation {
    FieldType type;
    EncodingTypePB expected;
    const char* name;
};

// (type, encoding, should_exist) row for the global encoding_map completeness test.
struct EncodingMapEntry {
    FieldType type;
    EncodingTypePB encoding;
    bool should_exist;
    const char* name;
};

// Expected V3 default per type. Differs from V2 default in two type families:
//   - binary blobs (HLL/BITMAP/QUANTILE_STATE/AGG_STATE): PLAIN_ENCODING_V2 (vs V2's PLAIN)
//   - signed integers (TINYINT..LARGEINT): PLAIN_ENCODING (vs V2's BIT_SHUFFLE)
const std::vector<DefaultExpectation> kV3DefaultExpect = {
        {FieldType::OLAP_FIELD_TYPE_TINYINT, PLAIN_ENCODING, "TINYINT"},
        {FieldType::OLAP_FIELD_TYPE_SMALLINT, PLAIN_ENCODING, "SMALLINT"},
        {FieldType::OLAP_FIELD_TYPE_INT, PLAIN_ENCODING, "INT"},
        {FieldType::OLAP_FIELD_TYPE_BIGINT, PLAIN_ENCODING, "BIGINT"},
        {FieldType::OLAP_FIELD_TYPE_LARGEINT, PLAIN_ENCODING, "LARGEINT"},
        {FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT, BIT_SHUFFLE, "UNSIGNED_BIGINT"},
        {FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT, BIT_SHUFFLE, "UNSIGNED_INT"},
        {FieldType::OLAP_FIELD_TYPE_FLOAT, BIT_SHUFFLE, "FLOAT"},
        {FieldType::OLAP_FIELD_TYPE_DOUBLE, BIT_SHUFFLE, "DOUBLE"},
        {FieldType::OLAP_FIELD_TYPE_CHAR, DICT_ENCODING, "CHAR"},
        {FieldType::OLAP_FIELD_TYPE_VARCHAR, DICT_ENCODING, "VARCHAR"},
        {FieldType::OLAP_FIELD_TYPE_STRING, DICT_ENCODING, "STRING"},
        {FieldType::OLAP_FIELD_TYPE_JSONB, DICT_ENCODING, "JSONB"},
        {FieldType::OLAP_FIELD_TYPE_VARIANT, DICT_ENCODING, "VARIANT"},
        {FieldType::OLAP_FIELD_TYPE_BOOL, RLE, "BOOL"},
        {FieldType::OLAP_FIELD_TYPE_DATE, BIT_SHUFFLE, "DATE"},
        {FieldType::OLAP_FIELD_TYPE_DATEV2, BIT_SHUFFLE, "DATEV2"},
        {FieldType::OLAP_FIELD_TYPE_DATETIMEV2, BIT_SHUFFLE, "DATETIMEV2"},
        {FieldType::OLAP_FIELD_TYPE_DATETIME, BIT_SHUFFLE, "DATETIME"},
        {FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, BIT_SHUFFLE, "TIMESTAMPTZ"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL, BIT_SHUFFLE, "DECIMAL"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL32, BIT_SHUFFLE, "DECIMAL32"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL64, BIT_SHUFFLE, "DECIMAL64"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL128I, BIT_SHUFFLE, "DECIMAL128I"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL256, BIT_SHUFFLE, "DECIMAL256"},
        {FieldType::OLAP_FIELD_TYPE_IPV4, BIT_SHUFFLE, "IPV4"},
        {FieldType::OLAP_FIELD_TYPE_IPV6, BIT_SHUFFLE, "IPV6"},
        {FieldType::OLAP_FIELD_TYPE_HLL, PLAIN_ENCODING_V2, "HLL"},
        {FieldType::OLAP_FIELD_TYPE_BITMAP, PLAIN_ENCODING_V2, "BITMAP"},
        {FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE, PLAIN_ENCODING_V2, "QUANTILE_STATE"},
        {FieldType::OLAP_FIELD_TYPE_AGG_STATE, PLAIN_ENCODING_V2, "AGG_STATE"},
};

// Expected V2 (non-V3) default per type.
const std::vector<DefaultExpectation> kV2DefaultExpect = {
        {FieldType::OLAP_FIELD_TYPE_TINYINT, BIT_SHUFFLE, "TINYINT"},
        {FieldType::OLAP_FIELD_TYPE_SMALLINT, BIT_SHUFFLE, "SMALLINT"},
        {FieldType::OLAP_FIELD_TYPE_INT, BIT_SHUFFLE, "INT"},
        {FieldType::OLAP_FIELD_TYPE_BIGINT, BIT_SHUFFLE, "BIGINT"},
        {FieldType::OLAP_FIELD_TYPE_LARGEINT, BIT_SHUFFLE, "LARGEINT"},
        {FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT, BIT_SHUFFLE, "UNSIGNED_BIGINT"},
        {FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT, BIT_SHUFFLE, "UNSIGNED_INT"},
        {FieldType::OLAP_FIELD_TYPE_FLOAT, BIT_SHUFFLE, "FLOAT"},
        {FieldType::OLAP_FIELD_TYPE_DOUBLE, BIT_SHUFFLE, "DOUBLE"},
        {FieldType::OLAP_FIELD_TYPE_CHAR, DICT_ENCODING, "CHAR"},
        {FieldType::OLAP_FIELD_TYPE_VARCHAR, DICT_ENCODING, "VARCHAR"},
        {FieldType::OLAP_FIELD_TYPE_STRING, DICT_ENCODING, "STRING"},
        {FieldType::OLAP_FIELD_TYPE_JSONB, DICT_ENCODING, "JSONB"},
        {FieldType::OLAP_FIELD_TYPE_VARIANT, DICT_ENCODING, "VARIANT"},
        {FieldType::OLAP_FIELD_TYPE_BOOL, RLE, "BOOL"},
        {FieldType::OLAP_FIELD_TYPE_DATE, BIT_SHUFFLE, "DATE"},
        {FieldType::OLAP_FIELD_TYPE_DATEV2, BIT_SHUFFLE, "DATEV2"},
        {FieldType::OLAP_FIELD_TYPE_DATETIMEV2, BIT_SHUFFLE, "DATETIMEV2"},
        {FieldType::OLAP_FIELD_TYPE_DATETIME, BIT_SHUFFLE, "DATETIME"},
        {FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, BIT_SHUFFLE, "TIMESTAMPTZ"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL, BIT_SHUFFLE, "DECIMAL"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL32, BIT_SHUFFLE, "DECIMAL32"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL64, BIT_SHUFFLE, "DECIMAL64"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL128I, BIT_SHUFFLE, "DECIMAL128I"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL256, BIT_SHUFFLE, "DECIMAL256"},
        {FieldType::OLAP_FIELD_TYPE_IPV4, BIT_SHUFFLE, "IPV4"},
        {FieldType::OLAP_FIELD_TYPE_IPV6, BIT_SHUFFLE, "IPV6"},
        {FieldType::OLAP_FIELD_TYPE_HLL, PLAIN_ENCODING, "HLL"},
        {FieldType::OLAP_FIELD_TYPE_BITMAP, PLAIN_ENCODING, "BITMAP"},
        {FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE, PLAIN_ENCODING, "QUANTILE_STATE"},
        {FieldType::OLAP_FIELD_TYPE_AGG_STATE, PLAIN_ENCODING, "AGG_STATE"},
};

// Expected index_column_encoding per type. Only VARCHAR is consulted in production
// (PrimaryKeyIndexBuilder::init hardcodes VARCHAR); all other entries return
// UNKNOWN_ENCODING because they were never queried and have been removed.
const std::vector<DefaultExpectation> kIndexColumnEncodingExpect = {
        {FieldType::OLAP_FIELD_TYPE_TINYINT, UNKNOWN_ENCODING, "TINYINT"},
        {FieldType::OLAP_FIELD_TYPE_SMALLINT, UNKNOWN_ENCODING, "SMALLINT"},
        {FieldType::OLAP_FIELD_TYPE_INT, UNKNOWN_ENCODING, "INT"},
        {FieldType::OLAP_FIELD_TYPE_BIGINT, UNKNOWN_ENCODING, "BIGINT"},
        {FieldType::OLAP_FIELD_TYPE_LARGEINT, UNKNOWN_ENCODING, "LARGEINT"},
        {FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT, UNKNOWN_ENCODING, "UNSIGNED_BIGINT"},
        {FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT, UNKNOWN_ENCODING, "UNSIGNED_INT"},
        {FieldType::OLAP_FIELD_TYPE_FLOAT, UNKNOWN_ENCODING, "FLOAT"},
        {FieldType::OLAP_FIELD_TYPE_DOUBLE, UNKNOWN_ENCODING, "DOUBLE"},
        {FieldType::OLAP_FIELD_TYPE_CHAR, UNKNOWN_ENCODING, "CHAR"},
        {FieldType::OLAP_FIELD_TYPE_VARCHAR, PREFIX_ENCODING, "VARCHAR"},
        {FieldType::OLAP_FIELD_TYPE_STRING, UNKNOWN_ENCODING, "STRING"},
        {FieldType::OLAP_FIELD_TYPE_JSONB, UNKNOWN_ENCODING, "JSONB"},
        {FieldType::OLAP_FIELD_TYPE_VARIANT, UNKNOWN_ENCODING, "VARIANT"},
        {FieldType::OLAP_FIELD_TYPE_BOOL, UNKNOWN_ENCODING, "BOOL"},
        {FieldType::OLAP_FIELD_TYPE_DATE, UNKNOWN_ENCODING, "DATE"},
        {FieldType::OLAP_FIELD_TYPE_DATEV2, UNKNOWN_ENCODING, "DATEV2"},
        {FieldType::OLAP_FIELD_TYPE_DATETIMEV2, UNKNOWN_ENCODING, "DATETIMEV2"},
        {FieldType::OLAP_FIELD_TYPE_DATETIME, UNKNOWN_ENCODING, "DATETIME"},
        {FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, UNKNOWN_ENCODING, "TIMESTAMPTZ"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL, UNKNOWN_ENCODING, "DECIMAL"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL32, UNKNOWN_ENCODING, "DECIMAL32"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL64, UNKNOWN_ENCODING, "DECIMAL64"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL128I, UNKNOWN_ENCODING, "DECIMAL128I"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL256, UNKNOWN_ENCODING, "DECIMAL256"},
        {FieldType::OLAP_FIELD_TYPE_IPV4, UNKNOWN_ENCODING, "IPV4"},
        {FieldType::OLAP_FIELD_TYPE_IPV6, UNKNOWN_ENCODING, "IPV6"},
        {FieldType::OLAP_FIELD_TYPE_HLL, UNKNOWN_ENCODING, "HLL"},
        {FieldType::OLAP_FIELD_TYPE_BITMAP, UNKNOWN_ENCODING, "BITMAP"},
        {FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE, UNKNOWN_ENCODING, "QUANTILE_STATE"},
        {FieldType::OLAP_FIELD_TYPE_AGG_STATE, UNKNOWN_ENCODING, "AGG_STATE"},
};

// Full enumeration of (type, encoding) combinations that EncodingInfo::get should
// find or reject. should_exist=true means EncodingInfo::get must succeed.
const std::vector<EncodingMapEntry> kEncodingMapEntries = {
        // signed integers: BIT_SHUFFLE, PLAIN_ENCODING, FOR_ENCODING
        {FieldType::OLAP_FIELD_TYPE_TINYINT, BIT_SHUFFLE, true, "TINYINT+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_TINYINT, PLAIN_ENCODING, true, "TINYINT+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_TINYINT, FOR_ENCODING, true, "TINYINT+FOR"},
        {FieldType::OLAP_FIELD_TYPE_SMALLINT, BIT_SHUFFLE, true, "SMALLINT+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_SMALLINT, PLAIN_ENCODING, true, "SMALLINT+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_SMALLINT, FOR_ENCODING, true, "SMALLINT+FOR"},
        {FieldType::OLAP_FIELD_TYPE_INT, BIT_SHUFFLE, true, "INT+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_INT, PLAIN_ENCODING, true, "INT+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_INT, FOR_ENCODING, true, "INT+FOR"},
        {FieldType::OLAP_FIELD_TYPE_BIGINT, BIT_SHUFFLE, true, "BIGINT+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_BIGINT, PLAIN_ENCODING, true, "BIGINT+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_BIGINT, FOR_ENCODING, true, "BIGINT+FOR"},
        {FieldType::OLAP_FIELD_TYPE_LARGEINT, BIT_SHUFFLE, true, "LARGEINT+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_LARGEINT, PLAIN_ENCODING, true, "LARGEINT+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_LARGEINT, FOR_ENCODING, true, "LARGEINT+FOR"},
        // unsigned integers: only BIT_SHUFFLE
        {FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT, BIT_SHUFFLE, true,
         "UNSIGNED_BIGINT+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT, BIT_SHUFFLE, true, "UNSIGNED_INT+BIT_SHUFFLE"},
        // FLOAT/DOUBLE: BIT_SHUFFLE, PLAIN_ENCODING
        {FieldType::OLAP_FIELD_TYPE_FLOAT, BIT_SHUFFLE, true, "FLOAT+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_FLOAT, PLAIN_ENCODING, true, "FLOAT+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_DOUBLE, BIT_SHUFFLE, true, "DOUBLE+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_DOUBLE, PLAIN_ENCODING, true, "DOUBLE+PLAIN"},
        // binary types: DICT, PLAIN, PLAIN_V2, PREFIX
        {FieldType::OLAP_FIELD_TYPE_CHAR, DICT_ENCODING, true, "CHAR+DICT"},
        {FieldType::OLAP_FIELD_TYPE_CHAR, PLAIN_ENCODING, true, "CHAR+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_CHAR, PLAIN_ENCODING_V2, true, "CHAR+PLAIN_V2"},
        {FieldType::OLAP_FIELD_TYPE_CHAR, PREFIX_ENCODING, true, "CHAR+PREFIX"},
        {FieldType::OLAP_FIELD_TYPE_VARCHAR, DICT_ENCODING, true, "VARCHAR+DICT"},
        {FieldType::OLAP_FIELD_TYPE_VARCHAR, PLAIN_ENCODING, true, "VARCHAR+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_VARCHAR, PLAIN_ENCODING_V2, true, "VARCHAR+PLAIN_V2"},
        {FieldType::OLAP_FIELD_TYPE_VARCHAR, PREFIX_ENCODING, true, "VARCHAR+PREFIX"},
        {FieldType::OLAP_FIELD_TYPE_STRING, DICT_ENCODING, true, "STRING+DICT"},
        {FieldType::OLAP_FIELD_TYPE_STRING, PLAIN_ENCODING, true, "STRING+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_STRING, PLAIN_ENCODING_V2, true, "STRING+PLAIN_V2"},
        {FieldType::OLAP_FIELD_TYPE_STRING, PREFIX_ENCODING, true, "STRING+PREFIX"},
        {FieldType::OLAP_FIELD_TYPE_JSONB, DICT_ENCODING, true, "JSONB+DICT"},
        {FieldType::OLAP_FIELD_TYPE_JSONB, PLAIN_ENCODING, true, "JSONB+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_JSONB, PLAIN_ENCODING_V2, true, "JSONB+PLAIN_V2"},
        {FieldType::OLAP_FIELD_TYPE_JSONB, PREFIX_ENCODING, true, "JSONB+PREFIX"},
        {FieldType::OLAP_FIELD_TYPE_VARIANT, DICT_ENCODING, true, "VARIANT+DICT"},
        {FieldType::OLAP_FIELD_TYPE_VARIANT, PLAIN_ENCODING, true, "VARIANT+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_VARIANT, PLAIN_ENCODING_V2, true, "VARIANT+PLAIN_V2"},
        {FieldType::OLAP_FIELD_TYPE_VARIANT, PREFIX_ENCODING, true, "VARIANT+PREFIX"},
        // BOOL: RLE, BIT_SHUFFLE, PLAIN
        {FieldType::OLAP_FIELD_TYPE_BOOL, RLE, true, "BOOL+RLE"},
        {FieldType::OLAP_FIELD_TYPE_BOOL, BIT_SHUFFLE, true, "BOOL+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_BOOL, PLAIN_ENCODING, true, "BOOL+PLAIN"},
        // date / datetime: BIT_SHUFFLE, PLAIN, FOR
        {FieldType::OLAP_FIELD_TYPE_DATE, BIT_SHUFFLE, true, "DATE+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_DATE, PLAIN_ENCODING, true, "DATE+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_DATE, FOR_ENCODING, true, "DATE+FOR"},
        {FieldType::OLAP_FIELD_TYPE_DATEV2, BIT_SHUFFLE, true, "DATEV2+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_DATEV2, PLAIN_ENCODING, true, "DATEV2+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_DATEV2, FOR_ENCODING, true, "DATEV2+FOR"},
        {FieldType::OLAP_FIELD_TYPE_DATETIMEV2, BIT_SHUFFLE, true, "DATETIMEV2+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_DATETIMEV2, PLAIN_ENCODING, true, "DATETIMEV2+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_DATETIMEV2, FOR_ENCODING, true, "DATETIMEV2+FOR"},
        {FieldType::OLAP_FIELD_TYPE_DATETIME, BIT_SHUFFLE, true, "DATETIME+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_DATETIME, PLAIN_ENCODING, true, "DATETIME+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_DATETIME, FOR_ENCODING, true, "DATETIME+FOR"},
        {FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, BIT_SHUFFLE, true, "TIMESTAMPTZ+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, PLAIN_ENCODING, true, "TIMESTAMPTZ+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, FOR_ENCODING, true, "TIMESTAMPTZ+FOR"},
        // decimal: BIT_SHUFFLE, PLAIN
        {FieldType::OLAP_FIELD_TYPE_DECIMAL, BIT_SHUFFLE, true, "DECIMAL+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL, PLAIN_ENCODING, true, "DECIMAL+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL32, BIT_SHUFFLE, true, "DECIMAL32+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL32, PLAIN_ENCODING, true, "DECIMAL32+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL64, BIT_SHUFFLE, true, "DECIMAL64+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL64, PLAIN_ENCODING, true, "DECIMAL64+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL128I, BIT_SHUFFLE, true, "DECIMAL128I+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL128I, PLAIN_ENCODING, true, "DECIMAL128I+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL256, BIT_SHUFFLE, true, "DECIMAL256+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL256, PLAIN_ENCODING, true, "DECIMAL256+PLAIN"},
        // ip
        {FieldType::OLAP_FIELD_TYPE_IPV4, BIT_SHUFFLE, true, "IPV4+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_IPV4, PLAIN_ENCODING, true, "IPV4+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_IPV6, BIT_SHUFFLE, true, "IPV6+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_IPV6, PLAIN_ENCODING, true, "IPV6+PLAIN"},
        // aggregate-flavored binary
        {FieldType::OLAP_FIELD_TYPE_HLL, PLAIN_ENCODING, true, "HLL+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_HLL, PLAIN_ENCODING_V2, true, "HLL+PLAIN_V2"},
        {FieldType::OLAP_FIELD_TYPE_BITMAP, PLAIN_ENCODING, true, "BITMAP+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_BITMAP, PLAIN_ENCODING_V2, true, "BITMAP+PLAIN_V2"},
        {FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE, PLAIN_ENCODING, true, "QUANTILE_STATE+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE, PLAIN_ENCODING_V2, true,
         "QUANTILE_STATE+PLAIN_V2"},
        {FieldType::OLAP_FIELD_TYPE_AGG_STATE, PLAIN_ENCODING, true, "AGG_STATE+PLAIN"},
        {FieldType::OLAP_FIELD_TYPE_AGG_STATE, PLAIN_ENCODING_V2, true, "AGG_STATE+PLAIN_V2"},
        // Negative samples: combinations that should NOT be registered.
        {FieldType::OLAP_FIELD_TYPE_INT, DICT_ENCODING, false, "INT+DICT"},
        {FieldType::OLAP_FIELD_TYPE_INT, RLE, false, "INT+RLE"},
        {FieldType::OLAP_FIELD_TYPE_INT, PREFIX_ENCODING, false, "INT+PREFIX"},
        {FieldType::OLAP_FIELD_TYPE_INT, PLAIN_ENCODING_V2, false, "INT+PLAIN_V2"},
        {FieldType::OLAP_FIELD_TYPE_BIGINT, RLE, false, "BIGINT+RLE"},
        {FieldType::OLAP_FIELD_TYPE_FLOAT, DICT_ENCODING, false, "FLOAT+DICT"},
        {FieldType::OLAP_FIELD_TYPE_FLOAT, FOR_ENCODING, false, "FLOAT+FOR"},
        {FieldType::OLAP_FIELD_TYPE_VARCHAR, BIT_SHUFFLE, false, "VARCHAR+BIT_SHUFFLE"},
        {FieldType::OLAP_FIELD_TYPE_VARCHAR, RLE, false, "VARCHAR+RLE"},
        {FieldType::OLAP_FIELD_TYPE_BOOL, DICT_ENCODING, false, "BOOL+DICT"},
        {FieldType::OLAP_FIELD_TYPE_BOOL, FOR_ENCODING, false, "BOOL+FOR"},
        {FieldType::OLAP_FIELD_TYPE_DATE, DICT_ENCODING, false, "DATE+DICT"},
        {FieldType::OLAP_FIELD_TYPE_DECIMAL, DICT_ENCODING, false, "DECIMAL+DICT"},
        {FieldType::OLAP_FIELD_TYPE_HLL, DICT_ENCODING, false, "HLL+DICT"},
        {FieldType::OLAP_FIELD_TYPE_HLL, BIT_SHUFFLE, false, "HLL+BIT_SHUFFLE"},
};

} // namespace

// case 1: V3 default encoding for every type.
TEST_F(EncodingInfoTest, locked_v3_default_per_type) {
    for (const auto& row : kV3DefaultExpect) {
        auto got = get_v3_default_encoding(row.type);
        EXPECT_EQ(row.expected, got)
                << "V3 default mismatch for " << row.name << " (type=" << int(row.type)
                << "): expected " << row.expected << ", got " << got;
    }
}

// case 2: V2 (non-V3) default encoding for every type.
TEST_F(EncodingInfoTest, locked_v2_default_per_type) {
    for (const auto& row : kV2DefaultExpect) {
        auto got = get_v2_default_encoding(row.type);
        EXPECT_EQ(row.expected, got)
                << "Legacy default mismatch for " << row.name << " (type=" << int(row.type)
                << "): expected " << row.expected << ", got " << got;
    }
}

// case 3: index_column_encoding for every type. UNKNOWN_ENCODING means no
// index_column_encoding was registered for that type.
TEST_F(EncodingInfoTest, locked_index_column_encoding_per_type) {
    for (const auto& row : kIndexColumnEncodingExpect) {
        auto got = EncodingInfo::get_index_column_encoding(row.type);
        EXPECT_EQ(row.expected, got)
                << "Value-seek default mismatch for " << row.name << " (type=" << int(row.type)
                << "): expected " << row.expected << ", got " << got;
    }
}

// case 4: every (type, encoding) combination has the expected presence in the
// global encoding map.
TEST_F(EncodingInfoTest, locked_encoding_map_completeness) {
    for (const auto& row : kEncodingMapEntries) {
        const EncodingInfo* info = nullptr;
        auto status = EncodingInfo::get(row.type, row.encoding, &info);
        if (row.should_exist) {
            EXPECT_TRUE(status.ok())
                    << "Expected entry missing: " << row.name << ": " << status.to_string();
            EXPECT_NE(nullptr, info) << "Entry " << row.name << " is null";
        } else {
            EXPECT_FALSE(status.ok())
                    << "Unexpected entry present: " << row.name << " (type=" << int(row.type)
                    << ", encoding=" << row.encoding << ")";
        }
    }
}

} // namespace segment_v2
} // namespace doris
