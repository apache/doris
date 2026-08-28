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

#include "format_v2/jni/paimon_jni_reader.h"

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <utility>

#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant_v2.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"

namespace doris::format::paimon {
namespace {

TFileRangeDesc make_paimon_jni_range() {
    TFileRangeDesc range;
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("paimon");
    TPaimonFileDesc paimon_params;
    paimon_params.__set_reader_type(TPaimonReaderType::PAIMON_JNI);
    paimon_params.__set_paimon_split("serialized-split");
    table_format_params.__set_paimon_params(std::move(paimon_params));
    range.__set_table_format_params(std::move(table_format_params));
    return range;
}

TFileScanRangeParams make_scan_params() {
    TFileScanRangeParams scan_params;
    scan_params.__set_serialized_table("serialized-table");
    return scan_params;
}

Status init_reader(PaimonJniReader* reader, TFileScanRangeParams* scan_params,
                   RuntimeState* runtime_state = nullptr,
                   std::vector<ColumnDefinition> projected_columns = {}) {
    return reader->init({
            .projected_columns = std::move(projected_columns),
            .conjuncts = {},
            .format = FileFormat::JNI,
            .scan_params = scan_params,
            .io_ctx = nullptr,
            .runtime_state = runtime_state,
            .scanner_profile = nullptr,
    });
}

Status build_params(PaimonJniReader* reader, const TFileRangeDesc& range,
                    std::map<std::string, std::string>* params) {
    reader->_current_range = range;
    return reader->build_scanner_params(params);
}

TEST(PaimonJniReaderTest, PublishesVariantAccessPathsByProjectedColumnPosition) {
    auto range = make_paimon_jni_range();
    range.table_format_params.paimon_params.__set_paimon_predicate("serialized-predicate");
    auto scan_params = make_scan_params();

    ColumnDefinition id;
    id.name = "id";
    id.type = std::make_shared<DataTypeString>();
    ColumnDefinition payload;
    payload.name = "payload";
    payload.type = std::make_shared<DataTypeVariantV2>();
    payload.variant_access_paths = {{"name"}, {"profile", "city"}};

    PaimonJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params, nullptr, {id, payload}).ok());

    std::map<std::string, std::string> params;
    ASSERT_TRUE(build_params(&reader, range, &params).ok());
    EXPECT_FALSE(params.contains("variant_access_path.0.0"));
    EXPECT_EQ(params.at("variant_access_path.1.0"), "$bmFtZQ==");
    EXPECT_EQ(params.at("variant_access_path.1.1"), "$cHJvZmlsZQ==,$Y2l0eQ==");
}

TEST(PaimonJniReaderTest, UsesScanLevelPredicateBeforeLegacySplitPredicate) {
    auto range = make_paimon_jni_range();
    range.table_format_params.paimon_params.__set_paimon_predicate("legacy-predicate");

    auto scan_params = make_scan_params();
    scan_params.__set_paimon_predicate("scan-predicate");

    PaimonJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());
    ASSERT_TRUE(reader.validate_scan_range(range).ok());

    std::map<std::string, std::string> params;
    ASSERT_TRUE(build_params(&reader, range, &params).ok());
    EXPECT_EQ(params["paimon_predicate"], "scan-predicate");
}

TEST(PaimonJniReaderTest, ForwardsSerializedTableCacheKey) {
    auto range = make_paimon_jni_range();
    range.table_format_params.paimon_params.__set_paimon_predicate("serialized-predicate");

    auto scan_params = make_scan_params();
    scan_params.__set_serialized_table_cache_key("table-cache-key");

    PaimonJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());

    std::map<std::string, std::string> params;
    ASSERT_TRUE(build_params(&reader, range, &params).ok());
    EXPECT_EQ(params["serialized_table_cache_key"], "table-cache-key");
}

TEST(PaimonJniReaderTest, GeneratesMissingOrEmptySerializedTableCacheKey) {
    auto range = make_paimon_jni_range();
    range.table_format_params.paimon_params.__set_paimon_predicate("serialized-predicate");
    auto scan_params = make_scan_params();

    PaimonJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());

    std::map<std::string, std::string> params;
    ASSERT_TRUE(build_params(&reader, range, &params).ok());
    EXPECT_EQ(params["serialized_table"], "serialized-table");
    const std::string missing_key = params["serialized_table_cache_key"];
    EXPECT_FALSE(missing_key.empty());

    scan_params.__set_serialized_table_cache_key("");
    ASSERT_TRUE(build_params(&reader, range, &params).ok());
    EXPECT_EQ(params["serialized_table"], "serialized-table");
    const std::string empty_key = params["serialized_table_cache_key"];
    EXPECT_FALSE(empty_key.empty());
    EXPECT_NE(missing_key, empty_key);
}

TEST(PaimonJniReaderTest, FallsBackToLegacySplitPredicateWhenScanPredicateIsMissing) {
    auto range = make_paimon_jni_range();
    range.table_format_params.paimon_params.__set_paimon_predicate("legacy-predicate");

    auto scan_params = make_scan_params();

    PaimonJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());
    ASSERT_TRUE(reader.validate_scan_range(range).ok());

    std::map<std::string, std::string> params;
    ASSERT_TRUE(build_params(&reader, range, &params).ok());
    EXPECT_EQ(params["paimon_predicate"], "legacy-predicate");
}

TEST(PaimonJniReaderTest, FallsBackToLegacySplitPredicateWhenScanPredicateIsEmpty) {
    auto range = make_paimon_jni_range();
    range.table_format_params.paimon_params.__set_paimon_predicate("legacy-predicate");

    auto scan_params = make_scan_params();
    scan_params.__set_paimon_predicate("");

    PaimonJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());
    ASSERT_TRUE(reader.validate_scan_range(range).ok());

    std::map<std::string, std::string> params;
    ASSERT_TRUE(build_params(&reader, range, &params).ok());
    EXPECT_EQ(params["paimon_predicate"], "legacy-predicate");
}

TEST(PaimonJniReaderTest, RejectsMissingPredicateFromBothProtocolLocations) {
    const auto range = make_paimon_jni_range();
    auto scan_params = make_scan_params();

    PaimonJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());
    const auto status = reader.validate_scan_range(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing paimon_predicate"), std::string::npos);
}

TEST(PaimonJniReaderTest, FallsBackToLegacySplitOptionsAndHadoopConf) {
    auto range = make_paimon_jni_range();
    auto& paimon_params = range.table_format_params.paimon_params;
    paimon_params.__set_paimon_predicate("legacy-predicate");
    paimon_params.__set_paimon_options({{"legacy-option", "legacy-value"}});
    paimon_params.__set_hadoop_conf({{"fs.defaultFS", "hdfs://legacy"}});

    auto scan_params = make_scan_params();
    PaimonJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());

    std::map<std::string, std::string> params;
    ASSERT_TRUE(build_params(&reader, range, &params).ok());
    EXPECT_EQ(params["paimon.legacy-option"], "legacy-value");
    EXPECT_EQ(params["hadoop.fs.defaultFS"], "hdfs://legacy");
}

TEST(PaimonJniReaderTest, ScanLevelOptionsOverrideLegacySplitFallbacks) {
    auto range = make_paimon_jni_range();
    auto& paimon_params = range.table_format_params.paimon_params;
    paimon_params.__set_paimon_predicate("legacy-predicate");
    paimon_params.__set_paimon_options({{"source", "legacy"}});
    paimon_params.__set_hadoop_conf({{"source", "legacy"}});

    auto scan_params = make_scan_params();
    scan_params.__set_paimon_options({{"source", "scan"}});
    scan_params.__set_properties({{"source", "scan"}});
    PaimonJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());

    std::map<std::string, std::string> params;
    ASSERT_TRUE(build_params(&reader, range, &params).ok());
    EXPECT_EQ(params["paimon.source"], "scan");
    EXPECT_EQ(params["hadoop.source"], "scan");
}

TEST(PaimonJniReaderTest, PublishesEncodedSchemaForQuotedIdentifiers) {
    PaimonJniReader reader;
    reader._jni_columns = {JniTableReader::JniColumn {
            .java_name = "region,code",
            // Keep the aggregate complete because BE UT treats omitted JNI column fields as errors.
            .output_type = std::make_shared<DataTypeString>(),
            .transfer_type = std::make_shared<DataTypeString>(),
    }};

    reader._prepare_jni_scanner_schema();

    EXPECT_EQ(reader._scanner_params.at("required_fields_base64"), "$cmVnaW9uLGNvZGU=");
    EXPECT_TRUE(reader._scanner_params.contains("columns_types_base64"));
}

TEST(PaimonJniReaderTest, UsesStableRuntimeBatchSizeBeforeAndAfterOpen) {
    TQueryOptions query_options;
    query_options.__set_batch_size(8160);
    RuntimeState state {query_options, TQueryGlobals()};
    auto scan_params = make_scan_params();
    PaimonJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params, &state).ok());

    reader.set_batch_size(32);
    EXPECT_EQ(reader.TEST_batch_size(), 8160);

    reader.TEST_set_split_state(true, false);
    reader.set_batch_size(1);
    EXPECT_EQ(reader.TEST_batch_size(), 8160);
}

} // namespace
} // namespace doris::format::paimon
