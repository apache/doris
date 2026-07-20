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
#include <memory>
#include <string>
#include <utility>

#include "format_v2/table_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/io_common.h"

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

Status init_reader(PaimonJniReader* reader, TFileScanRangeParams* scan_params) {
    return reader->init({
            .projected_columns = {},
            .conjuncts = {},
            .format = FileFormat::JNI,
            .scan_params = scan_params,
            .io_ctx = nullptr,
            .runtime_state = nullptr,
            .scanner_profile = nullptr,
    });
}

Status build_params(PaimonJniReader* reader, const TFileRangeDesc& range,
                    std::map<std::string, std::string>* params) {
    reader->_current_range = range;
    return reader->build_scanner_params(params);
}

class LifecyclePaimonJniReader final : public PaimonJniReader {
public:
    int global_scanner_close_calls = 0;
    int reset_current_split_calls = 0;
    bool fail_next_reset = false;

protected:
    Status _close_jni_scanner() override {
        if (!TEST_scanner_opened()) {
            return Status::OK();
        }
        ++global_scanner_close_calls;
        TEST_set_split_state(false, TEST_eof());
        _on_jni_scanner_discarded();
        return Status::OK();
    }

    Status _reset_current_split() override {
        if (!TEST_current_split_prepared()) {
            return Status::OK();
        }
        ++reset_current_split_calls;
        if (fail_next_reset) {
            fail_next_reset = false;
            return Status::InternalError("injected reset failure");
        }
        TEST_set_current_split_prepared(false);
        return Status::OK();
    }
};

Status init_lifecycle_reader(LifecyclePaimonJniReader* reader,
                             const std::shared_ptr<io::IOContext>& io_ctx) {
    return reader->init({
            .projected_columns = {},
            .conjuncts = {},
            .format = FileFormat::JNI,
            .scan_params = nullptr,
            .io_ctx = io_ctx,
            .runtime_state = nullptr,
            .scanner_profile = nullptr,
    });
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

TEST(PaimonJniReaderTest, KeepsInitialPhysicalBatchSizeAfterOpen) {
    PaimonJniReader reader;
    reader.set_batch_size(32);
    EXPECT_EQ(reader.TEST_batch_size(), 32);

    // Paimon copies the constructor size into the RecordReader during Java open. A later predictor
    // result cannot resize that physical reader, so keep the initial probe size for the split.
    reader.TEST_set_split_state(true, false);
    reader.set_batch_size(1);
    EXPECT_EQ(reader.TEST_batch_size(), 32);
}

TEST(PaimonJniReaderTest, StopDefersPreparedSplitCleanupUntilClose) {
    auto io_ctx = std::make_shared<io::IOContext>();
    LifecyclePaimonJniReader reader;
    ASSERT_TRUE(init_lifecycle_reader(&reader, io_ctx).ok());
    reader.TEST_set_split_state(true, false);
    reader.TEST_set_current_split_prepared(true);
    io_ctx->should_stop = true;

    Block block;
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(reader.global_scanner_close_calls, 0);
    EXPECT_TRUE(reader.TEST_current_split_prepared());

    ASSERT_TRUE(reader.close().ok());
    EXPECT_EQ(reader.reset_current_split_calls, 1);
    EXPECT_EQ(reader.global_scanner_close_calls, 1);
    EXPECT_FALSE(reader.TEST_current_split_prepared());
}

TEST(PaimonJniReaderTest, FailedSplitResetDoesNotSurviveSuccessfulGlobalClose) {
    LifecyclePaimonJniReader reader;
    ASSERT_TRUE(init_lifecycle_reader(&reader, nullptr).ok());
    reader.TEST_set_split_state(true, false);
    reader.TEST_set_current_split_prepared(true);
    reader.fail_next_reset = true;

    EXPECT_FALSE(reader.close().ok());
    EXPECT_EQ(reader.reset_current_split_calls, 1);
    EXPECT_EQ(reader.global_scanner_close_calls, 1);
    EXPECT_FALSE(reader.TEST_current_split_prepared());

    EXPECT_TRUE(reader.close().ok());
    EXPECT_EQ(reader.reset_current_split_calls, 1);
}

} // namespace
} // namespace doris::format::paimon
