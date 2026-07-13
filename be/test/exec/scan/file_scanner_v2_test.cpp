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

#include "exec/scan/file_scanner_v2.h"

#include <gen_cpp/PlanNodes_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/consts.h"
#include "core/assert_cast.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/operator/file_scan_operator.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/vdirect_in_predicate.h"
#include "exprs/vslot_ref.h"
#include "format_v2/expr/cast.h"

namespace doris {
namespace {

TFileRangeDesc range_with_format(std::string table_format, TFileFormatType::type format_type) {
    TFileRangeDesc range;
    range.__set_format_type(format_type);
    if (!table_format.empty()) {
        TTableFormatFileDesc table_desc;
        table_desc.__set_table_format_type(std::move(table_format));
        range.__set_table_format_params(std::move(table_desc));
    }
    return range;
}

TFileRangeDesc hudi_range_with_delta_logs() {
    auto range = range_with_format("hudi", TFileFormatType::FORMAT_PARQUET);
    THudiFileDesc hudi_params;
    hudi_params.__set_delta_logs({"delta.log"});
    range.table_format_params.__set_hudi_params(std::move(hudi_params));
    return range;
}

TFileRangeDesc paimon_cpp_jni_range() {
    auto range = range_with_format("paimon", TFileFormatType::FORMAT_JNI);
    TPaimonFileDesc paimon_params;
    paimon_params.__set_reader_type(TPaimonReaderType::PAIMON_CPP);
    range.table_format_params.__set_paimon_params(std::move(paimon_params));
    return range;
}

TFileRangeDesc legacy_paimon_jni_range_without_reader_type() {
    auto range = range_with_format("paimon", TFileFormatType::FORMAT_JNI);
    TPaimonFileDesc paimon_params;
    paimon_params.__set_paimon_split("legacy-split");
    paimon_params.__set_paimon_predicate("legacy-predicate");
    range.table_format_params.__set_paimon_params(std::move(paimon_params));
    return range;
}

struct RetryableCloseState {
    int close_calls = 0;
};

class RetryableCloseTableReader final : public format::TableReader {
public:
    explicit RetryableCloseTableReader(std::shared_ptr<RetryableCloseState> state)
            : _state(std::move(state)) {}

    Status close() override {
        ++_state->close_calls;
        if (_state->close_calls == 1) {
            return Status::InternalError("injected table reader close failure");
        }
        return Status::OK();
    }

private:
    std::shared_ptr<RetryableCloseState> _state;
};

VExprSPtr slot_ref(int slot_id, int column_id, DataTypePtr type, const std::string& name) {
    return VSlotRef::create_shared(slot_id, column_id, -1, std::move(type), name);
}

TExprNode bool_in_pred_node() {
    TTypeDesc bool_type;
    TTypeNode bool_node;
    TScalarType bool_scalar_type;
    bool_scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    bool_node.__set_type(TTypeNodeType::SCALAR);
    bool_node.__set_scalar_type(bool_scalar_type);
    bool_type.types.push_back(bool_node);

    TExprNode node;
    node.__set_type(bool_type);
    node.__set_node_type(TExprNodeType::IN_PRED);
    node.in_predicate.__set_is_not_in(false);
    node.__set_opcode(TExprOpcode::FILTER_IN);
    node.__set_is_nullable(false);
    return node;
}

} // namespace

// Scenario: FileScannerV2::is_supported should honor table format, scan params format, and the
// optional per-range file format override as a single matrix.
TEST(FileScannerV2Test, SupportedFormatMatrix) {
    struct Case {
        std::string table_format;
        TFileFormatType::type params_format;
        std::optional<TFileFormatType::type> range_format;
        bool expected;
    };

    const std::vector<Case> cases {
            {"", TFileFormatType::FORMAT_PARQUET, std::nullopt, true},
            {"tvf", TFileFormatType::FORMAT_PARQUET, std::nullopt, true},
            {"hive", TFileFormatType::FORMAT_PARQUET, std::nullopt, true},
            {"iceberg", TFileFormatType::FORMAT_PARQUET, std::nullopt, true},
            {"paimon", TFileFormatType::FORMAT_PARQUET, std::nullopt, true},
            {"hudi", TFileFormatType::FORMAT_PARQUET, std::nullopt, true},
            {"jdbc", TFileFormatType::FORMAT_PARQUET, std::nullopt, false},
            {"", TFileFormatType::FORMAT_JNI, std::nullopt, false},
            {"hive", TFileFormatType::FORMAT_ORC, std::nullopt, true},
            {"transactional_hive", TFileFormatType::FORMAT_ORC, std::nullopt, false},
            {"jdbc", TFileFormatType::FORMAT_JNI, std::nullopt, true},
            {"hive", TFileFormatType::FORMAT_JNI, std::nullopt, false},
            {"", TFileFormatType::FORMAT_CSV_PLAIN, std::nullopt, true},
            {"tvf", TFileFormatType::FORMAT_CSV_GZ, std::nullopt, true},
            {"hive", TFileFormatType::FORMAT_CSV_BZ2, std::nullopt, true},
            {"hive", TFileFormatType::FORMAT_CSV_LZ4FRAME, std::nullopt, true},
            {"hive", TFileFormatType::FORMAT_CSV_LZ4BLOCK, std::nullopt, true},
            {"hive", TFileFormatType::FORMAT_CSV_LZOP, std::nullopt, true},
            {"hive", TFileFormatType::FORMAT_CSV_DEFLATE, std::nullopt, true},
            {"hive", TFileFormatType::FORMAT_CSV_SNAPPYBLOCK, std::nullopt, true},
            {"hive", TFileFormatType::FORMAT_PROTO, std::nullopt, true},
            {"hive", TFileFormatType::FORMAT_TEXT, std::nullopt, true},
            {"hive", TFileFormatType::FORMAT_JSON, std::nullopt, true},
            {"hive", TFileFormatType::FORMAT_PARQUET, TFileFormatType::FORMAT_ORC, true},
            {"hive", TFileFormatType::FORMAT_ORC, TFileFormatType::FORMAT_PARQUET, true},
            {"hive", TFileFormatType::FORMAT_PARQUET, TFileFormatType::FORMAT_CSV_PLAIN, true},
            {"hive", TFileFormatType::FORMAT_PARQUET, TFileFormatType::FORMAT_TEXT, true},
            {"hive", TFileFormatType::FORMAT_PARQUET, TFileFormatType::FORMAT_JSON, true},
            {"tvf", TFileFormatType::FORMAT_PARQUET, TFileFormatType::FORMAT_NATIVE, true},
            {"remote_doris", TFileFormatType::FORMAT_ARROW, std::nullopt, true},
            {"hive", TFileFormatType::FORMAT_ARROW, std::nullopt, false},
            {"", TFileFormatType::FORMAT_ARROW, std::nullopt, false},
            {"", TFileFormatType::FORMAT_WAL, std::nullopt, false},
            {"", TFileFormatType::FORMAT_ES_HTTP, std::nullopt, false},
            {"", TFileFormatType::FORMAT_LANCE, std::nullopt, false},
    };

    for (const auto& test_case : cases) {
        TFileScanRangeParams params;
        params.__set_format_type(test_case.params_format);
        auto range = range_with_format(test_case.table_format,
                                       test_case.range_format.value_or(test_case.params_format));
        if (!test_case.range_format.has_value()) {
            range.__isset.format_type = false;
        }
        EXPECT_EQ(FileScannerV2::is_supported(params, range), test_case.expected)
                << "table_format=" << test_case.table_format
                << ", params_format=" << static_cast<int>(test_case.params_format)
                << ", range_has_format=" << test_case.range_format.has_value();
    }

    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    EXPECT_FALSE(FileScannerV2::is_supported(params, hudi_range_with_delta_logs()));
}

TEST(FileScannerV2Test, FileScanLocalStateSelectsV2ForSupportedQueriesOnly) {
    TQueryOptions query_options;
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_PARQUET);

    EXPECT_FALSE(FileScanLocalState::TEST_should_use_file_scanner_v2(query_options, false, params));

    query_options.__set_enable_file_scanner_v2(true);
    EXPECT_TRUE(FileScanLocalState::TEST_should_use_file_scanner_v2(query_options, false, params));
    EXPECT_FALSE(FileScanLocalState::TEST_should_use_file_scanner_v2(query_options, true, params));

    const std::vector<TFileFormatType::type> unsupported_formats {
            TFileFormatType::FORMAT_WAL,
            TFileFormatType::FORMAT_ES_HTTP,
            TFileFormatType::FORMAT_LANCE,
    };
    for (const auto format : unsupported_formats) {
        params.__set_format_type(format);
        EXPECT_FALSE(
                FileScanLocalState::TEST_should_use_file_scanner_v2(query_options, false, params));
    }

    params.__set_format_type(TFileFormatType::FORMAT_ORC);
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("transactional_hive");
    params.__set_table_format_params(table_format_params);
    EXPECT_FALSE(FileScanLocalState::TEST_should_use_file_scanner_v2(query_options, false, params));

    params.table_format_params.__set_table_format_type("hive");
    EXPECT_TRUE(FileScanLocalState::TEST_should_use_file_scanner_v2(query_options, false, params));

    query_options.__set_enable_file_scanner_v2(false);
    EXPECT_FALSE(FileScanLocalState::TEST_should_use_file_scanner_v2(query_options, false, params));
}

TEST(FileScannerV2Test, JniCompatibilityShapesForceLegacyScanner) {
    TQueryOptions query_options;
    query_options.__set_enable_file_scanner_v2(true);
    query_options.__set_enable_paimon_cpp_reader(true);

    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_JNI);
    // Rolling upgrades may carry the only Paimon marker and reader type on each split. Since the
    // scan-level selector cannot inspect that split yet, JNI scans conservatively stay on V1.
    EXPECT_FALSE(FileScanLocalState::TEST_should_use_file_scanner_v2(query_options, false, params));
    EXPECT_FALSE(FileScannerV2::is_supported(params, paimon_cpp_jni_range()));

    // Older FEs can omit reader_type. The legacy scanner interprets this as Paimon JNI when the C++
    // reader is disabled, so the scan-level choice must still stay on V1.
    query_options.__set_enable_paimon_cpp_reader(false);
    EXPECT_FALSE(FileScanLocalState::TEST_should_use_file_scanner_v2(query_options, false, params));
    EXPECT_FALSE(
            FileScannerV2::is_supported(params, legacy_paimon_jni_range_without_reader_type()));
}

TEST(FileScannerV2Test, FailedTableReaderCloseCanBeRetriedThroughScanner) {
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("file_scanner_v2_close_retry");
    auto close_state = std::make_shared<RetryableCloseState>();
    FileScannerV2 scanner(&state, &profile,
                          std::make_unique<RetryableCloseTableReader>(close_state));

    EXPECT_FALSE(scanner.close(&state).ok());
    EXPECT_EQ(close_state->close_calls, 1);
    EXPECT_TRUE(scanner.close(&state).ok());
    EXPECT_EQ(close_state->close_calls, 2);
    EXPECT_TRUE(scanner.close(&state).ok());
    EXPECT_EQ(close_state->close_calls, 2);
}

// Scenario: Once FileScannerV2 is selected, an unsupported range must fail instead of falling back
// to FileScanner.
TEST(FileScannerV2Test, ValidateScanRangeRejectsUnsupportedRange) {
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_PARQUET);

    const auto supported = range_with_format("hive", TFileFormatType::FORMAT_PARQUET);
    EXPECT_TRUE(FileScannerV2::TEST_validate_scan_range(params, supported).ok());

    const auto unsupported = range_with_format("lakesoul", TFileFormatType::FORMAT_PARQUET);
    const auto status = FileScannerV2::TEST_validate_scan_range(params, unsupported);
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
    EXPECT_NE(status.to_string().find("lakesoul"), std::string::npos);
}

// Scenario: FileScannerV2 converts only the file formats implemented by format_v2 readers and
// rejects everything else before TableReader::init sees an unsupported FileFormat.
TEST(FileScannerV2Test, FileFormatConversionMatrix) {
    struct Case {
        TFileFormatType::type input;
        std::optional<format::FileFormat> expected;
    };
    const std::vector<Case> cases {
            {TFileFormatType::FORMAT_PARQUET, format::FileFormat::PARQUET},
            {TFileFormatType::FORMAT_JNI, format::FileFormat::JNI},
            {TFileFormatType::FORMAT_CSV_PLAIN, format::FileFormat::CSV},
            {TFileFormatType::FORMAT_CSV_GZ, format::FileFormat::CSV},
            {TFileFormatType::FORMAT_CSV_BZ2, format::FileFormat::CSV},
            {TFileFormatType::FORMAT_CSV_LZ4FRAME, format::FileFormat::CSV},
            {TFileFormatType::FORMAT_CSV_LZ4BLOCK, format::FileFormat::CSV},
            {TFileFormatType::FORMAT_CSV_LZOP, format::FileFormat::CSV},
            {TFileFormatType::FORMAT_CSV_DEFLATE, format::FileFormat::CSV},
            {TFileFormatType::FORMAT_CSV_SNAPPYBLOCK, format::FileFormat::CSV},
            {TFileFormatType::FORMAT_PROTO, format::FileFormat::CSV},
            {TFileFormatType::FORMAT_TEXT, format::FileFormat::TEXT},
            {TFileFormatType::FORMAT_JSON, format::FileFormat::JSON},
            {TFileFormatType::FORMAT_NATIVE, format::FileFormat::NATIVE},
            {TFileFormatType::FORMAT_ARROW, format::FileFormat::ARROW},
            {TFileFormatType::FORMAT_ORC, format::FileFormat::ORC},
    };

    for (const auto& test_case : cases) {
        format::FileFormat file_format = format::FileFormat::PARQUET;
        const auto status = FileScannerV2::TEST_to_file_format(test_case.input, &file_format);
        if (test_case.expected.has_value()) {
            ASSERT_TRUE(status.ok()) << status;
            EXPECT_EQ(file_format, *test_case.expected);
        } else {
            EXPECT_FALSE(status.ok());
        }
    }
}

TEST(FileScannerV2Test, RealtimeCounterDeltasUseReaderBytesAsRemoteWithoutCacheStats) {
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_statistics;
    int64_t last_read_bytes = 0;
    int64_t last_read_rows = 0;
    int64_t last_bytes_read_from_local = 0;
    int64_t last_bytes_read_from_remote = 0;

    file_reader_stats.read_bytes = 100;
    file_reader_stats.read_rows = 7;
    auto deltas = FileScannerV2::TEST_collect_realtime_counter_deltas(
            file_reader_stats, file_cache_statistics,
            FileScannerV2::UncachedReaderBytesStorage::REMOTE, &last_read_bytes, &last_read_rows,
            &last_bytes_read_from_local, &last_bytes_read_from_remote);
    EXPECT_EQ(7, deltas.scan_rows);
    EXPECT_EQ(100, deltas.scan_bytes);
    EXPECT_EQ(0, deltas.scan_bytes_from_local_storage);
    EXPECT_EQ(100, deltas.scan_bytes_from_remote_storage);

    deltas = FileScannerV2::TEST_collect_realtime_counter_deltas(
            file_reader_stats, file_cache_statistics,
            FileScannerV2::UncachedReaderBytesStorage::REMOTE, &last_read_bytes, &last_read_rows,
            &last_bytes_read_from_local, &last_bytes_read_from_remote);
    EXPECT_EQ(0, deltas.scan_rows);
    EXPECT_EQ(0, deltas.scan_bytes);
    EXPECT_EQ(0, deltas.scan_bytes_from_local_storage);
    EXPECT_EQ(0, deltas.scan_bytes_from_remote_storage);

    file_reader_stats.read_bytes = 160;
    file_reader_stats.read_rows = 9;
    deltas = FileScannerV2::TEST_collect_realtime_counter_deltas(
            file_reader_stats, file_cache_statistics,
            FileScannerV2::UncachedReaderBytesStorage::REMOTE, &last_read_bytes, &last_read_rows,
            &last_bytes_read_from_local, &last_bytes_read_from_remote);
    EXPECT_EQ(2, deltas.scan_rows);
    EXPECT_EQ(60, deltas.scan_bytes);
    EXPECT_EQ(0, deltas.scan_bytes_from_local_storage);
    EXPECT_EQ(60, deltas.scan_bytes_from_remote_storage);
}

TEST(FileScannerV2Test, RealtimeCounterDeltasUseFileCacheDeltasWhenAvailable) {
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_statistics;
    int64_t last_read_bytes = 0;
    int64_t last_read_rows = 0;
    int64_t last_bytes_read_from_local = 0;
    int64_t last_bytes_read_from_remote = 0;

    file_reader_stats.read_bytes = 100;
    file_reader_stats.read_rows = 7;
    file_cache_statistics.bytes_read_from_local = 30;
    file_cache_statistics.bytes_read_from_remote = 70;
    auto deltas = FileScannerV2::TEST_collect_realtime_counter_deltas(
            file_reader_stats, file_cache_statistics,
            FileScannerV2::UncachedReaderBytesStorage::REMOTE, &last_read_bytes, &last_read_rows,
            &last_bytes_read_from_local, &last_bytes_read_from_remote);
    EXPECT_EQ(7, deltas.scan_rows);
    EXPECT_EQ(100, deltas.scan_bytes);
    EXPECT_EQ(30, deltas.scan_bytes_from_local_storage);
    EXPECT_EQ(70, deltas.scan_bytes_from_remote_storage);

    file_reader_stats.read_bytes = 125;
    file_reader_stats.read_rows = 10;
    file_cache_statistics.bytes_read_from_local = 35;
    file_cache_statistics.bytes_read_from_remote = 90;
    deltas = FileScannerV2::TEST_collect_realtime_counter_deltas(
            file_reader_stats, file_cache_statistics,
            FileScannerV2::UncachedReaderBytesStorage::REMOTE, &last_read_bytes, &last_read_rows,
            &last_bytes_read_from_local, &last_bytes_read_from_remote);
    EXPECT_EQ(3, deltas.scan_rows);
    EXPECT_EQ(25, deltas.scan_bytes);
    EXPECT_EQ(5, deltas.scan_bytes_from_local_storage);
    EXPECT_EQ(20, deltas.scan_bytes_from_remote_storage);
}

TEST(FileScannerV2Test, RealtimeCounterDeltasDoNotChargePeerCacheAsRemoteStorage) {
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_statistics;
    int64_t last_read_bytes = 0;
    int64_t last_read_rows = 0;
    int64_t last_bytes_read_from_local = 0;
    int64_t last_bytes_read_from_remote = 0;

    file_reader_stats.read_bytes = 100;
    file_reader_stats.read_rows = 7;
    file_cache_statistics.num_peer_io_total = 1;
    file_cache_statistics.bytes_read_from_peer = 100;
    auto deltas = FileScannerV2::TEST_collect_realtime_counter_deltas(
            file_reader_stats, file_cache_statistics,
            FileScannerV2::UncachedReaderBytesStorage::REMOTE, &last_read_bytes, &last_read_rows,
            &last_bytes_read_from_local, &last_bytes_read_from_remote);
    EXPECT_EQ(7, deltas.scan_rows);
    EXPECT_EQ(100, deltas.scan_bytes);
    EXPECT_EQ(0, deltas.scan_bytes_from_local_storage);
    EXPECT_EQ(0, deltas.scan_bytes_from_remote_storage);
}

TEST(FileScannerV2Test, RealtimeCounterDeltasDoNotChargeLocalFileFallbackAsRemoteStorage) {
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_statistics;
    int64_t last_read_bytes = 0;
    int64_t last_read_rows = 0;
    int64_t last_bytes_read_from_local = 0;
    int64_t last_bytes_read_from_remote = 0;

    file_reader_stats.read_bytes = 100;
    file_reader_stats.read_rows = 7;
    auto deltas = FileScannerV2::TEST_collect_realtime_counter_deltas(
            file_reader_stats, file_cache_statistics,
            FileScannerV2::UncachedReaderBytesStorage::LOCAL, &last_read_bytes, &last_read_rows,
            &last_bytes_read_from_local, &last_bytes_read_from_remote);
    EXPECT_EQ(7, deltas.scan_rows);
    EXPECT_EQ(100, deltas.scan_bytes);
    EXPECT_EQ(100, deltas.scan_bytes_from_local_storage);
    EXPECT_EQ(0, deltas.scan_bytes_from_remote_storage);
}

TEST(FileScannerV2Test, FileCacheStatisticsArePublishedToScannerProfile) {
    RuntimeProfile profile("file_scanner_v2");
    io::FileCacheStatistics file_cache_statistics;
    file_cache_statistics.num_local_io_total = 3;
    file_cache_statistics.num_remote_io_total = 5;
    file_cache_statistics.num_peer_io_total = 7;
    file_cache_statistics.bytes_read_from_local = 11;
    file_cache_statistics.bytes_read_from_remote = 13;
    file_cache_statistics.bytes_read_from_peer = 17;
    file_cache_statistics.bytes_write_into_cache = 19;
    file_cache_statistics.peer_hosts = {"peer-a", "peer-b"};

    FileScannerV2::TEST_report_file_cache_profile(&profile, file_cache_statistics);

    ASSERT_NE(profile.get_counter("FileCache"), nullptr);
    EXPECT_EQ(profile.get_counter("NumLocalIOTotal")->value(), 3);
    EXPECT_EQ(profile.get_counter("NumRemoteIOTotal")->value(), 5);
    EXPECT_EQ(profile.get_counter("NumPeerIOTotal")->value(), 7);
    EXPECT_EQ(profile.get_counter("BytesScannedFromCache")->value(), 11);
    EXPECT_EQ(profile.get_counter("BytesScannedFromRemote")->value(), 13);
    EXPECT_EQ(profile.get_counter("BytesScannedFromPeer")->value(), 17);
    EXPECT_EQ(profile.get_counter("BytesWriteIntoCache")->value(), 19);
    ASSERT_NE(profile.get_info_string("PeerCacheNodes"), nullptr);
    EXPECT_EQ(*profile.get_info_string("PeerCacheNodes"), "peer-a, peer-b");
}

TEST(FileScannerV2Test, NotFoundIsSkippedOnlyWhenConfigured) {
    const auto not_found = Status::NotFound("missing external file");
    EXPECT_TRUE(FileScannerV2::TEST_should_skip_not_found(not_found, true));
    EXPECT_FALSE(FileScannerV2::TEST_should_skip_not_found(not_found, false));
    EXPECT_FALSE(
            FileScannerV2::TEST_should_skip_not_found(Status::InternalError("read failed"), true));
    EXPECT_FALSE(FileScannerV2::TEST_should_skip_not_found(Status::OK(), true));
}

// Scenario: partition slots are identified from the explicit FE category when present, otherwise
// from the legacy is_file_slot flag. Scanner-generated rowid columns must never be treated as
// partition columns even if FE marks them as non-file slots.
TEST(FileScannerV2Test, PartitionSlotClassificationMatrix) {
    TFileScanSlotInfo legacy_partition;
    legacy_partition.__set_is_file_slot(false);
    EXPECT_TRUE(FileScannerV2::TEST_is_partition_slot(legacy_partition, "dt"));

    TFileScanSlotInfo legacy_file;
    legacy_file.__set_is_file_slot(true);
    EXPECT_FALSE(FileScannerV2::TEST_is_partition_slot(legacy_file, "value"));

    TFileScanSlotInfo categorized_partition;
    categorized_partition.__set_is_file_slot(true);
    categorized_partition.__set_category(TColumnCategory::PARTITION_KEY);
    EXPECT_TRUE(FileScannerV2::TEST_is_partition_slot(categorized_partition, "p"));

    TFileScanSlotInfo categorized_regular;
    categorized_regular.__set_is_file_slot(false);
    categorized_regular.__set_category(TColumnCategory::REGULAR);
    EXPECT_FALSE(FileScannerV2::TEST_is_partition_slot(categorized_regular, "regular_col"));

    EXPECT_FALSE(
            FileScannerV2::TEST_is_partition_slot(legacy_partition, BeConsts::GLOBAL_ROWID_COL));
    EXPECT_FALSE(
            FileScannerV2::TEST_is_partition_slot(legacy_partition, BeConsts::ICEBERG_ROWID_COL));
}

// Scenario: data-file slots are the complement of partition/default/synthesized columns for
// formats without embedded schema. FE may send either the new category or the old is_file_slot
// flag, and scanner-generated rowid columns must never be passed to a physical file reader.
TEST(FileScannerV2Test, DataFileSlotClassificationMatrix) {
    TFileScanSlotInfo legacy_file;
    legacy_file.__set_is_file_slot(true);
    EXPECT_TRUE(FileScannerV2::TEST_is_data_file_slot(legacy_file, "value"));

    TFileScanSlotInfo legacy_partition;
    legacy_partition.__set_is_file_slot(false);
    EXPECT_FALSE(FileScannerV2::TEST_is_data_file_slot(legacy_partition, "dt"));

    TFileScanSlotInfo categorized_regular;
    categorized_regular.__set_is_file_slot(false);
    categorized_regular.__set_category(TColumnCategory::REGULAR);
    EXPECT_TRUE(FileScannerV2::TEST_is_data_file_slot(categorized_regular, "regular_col"));

    TFileScanSlotInfo categorized_generated;
    categorized_generated.__set_is_file_slot(false);
    categorized_generated.__set_category(TColumnCategory::GENERATED);
    EXPECT_TRUE(FileScannerV2::TEST_is_data_file_slot(categorized_generated, "generated_col"));

    TFileScanSlotInfo categorized_partition;
    categorized_partition.__set_is_file_slot(true);
    categorized_partition.__set_category(TColumnCategory::PARTITION_KEY);
    EXPECT_FALSE(FileScannerV2::TEST_is_data_file_slot(categorized_partition, "p"));

    TFileScanSlotInfo categorized_synthesized;
    categorized_synthesized.__set_is_file_slot(true);
    categorized_synthesized.__set_category(TColumnCategory::SYNTHESIZED);
    EXPECT_FALSE(FileScannerV2::TEST_is_data_file_slot(categorized_synthesized, "virtual_col"));

    EXPECT_FALSE(FileScannerV2::TEST_is_data_file_slot(legacy_file, BeConsts::GLOBAL_ROWID_COL));
    EXPECT_FALSE(FileScannerV2::TEST_is_data_file_slot(legacy_file, BeConsts::ICEBERG_ROWID_COL));
}

// Scenario: table conjuncts are cloned into global-index space before they are handed to
// TableReader. Explicit slot-id mappings use the required_slots order; missing mappings fall back
// to the slot id itself for legacy descriptors.
TEST(FileScannerV2Test, RewriteSlotRefsToGlobalIndexMatrix) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    {
        auto expr = slot_ref(42, 99, int_type, "value");
        const auto status = FileScannerV2::TEST_rewrite_slot_refs_to_global_index(
                &expr, {{42, format::GlobalIndex(3)}});
        ASSERT_TRUE(status.ok()) << status;
        const auto* rewritten = assert_cast<const VSlotRef*>(expr.get());
        EXPECT_EQ(rewritten->slot_id(), 3);
        EXPECT_EQ(rewritten->column_id(), 3);
        EXPECT_EQ(rewritten->column_name(), "value");
    }
    {
        auto expr = slot_ref(7, 99, int_type, "legacy_value");
        const auto status = FileScannerV2::TEST_rewrite_slot_refs_to_global_index(&expr, {});
        ASSERT_TRUE(status.ok()) << status;
        const auto* rewritten = assert_cast<const VSlotRef*>(expr.get());
        EXPECT_EQ(rewritten->slot_id(), 7);
        EXPECT_EQ(rewritten->column_id(), 7);
        EXPECT_EQ(rewritten->column_name(), "legacy_value");
    }
    {
        auto cast_expr = format::Cast::create_shared(int_type);
        cast_expr->add_child(slot_ref(9, 9, int_type, "nested_value"));
        VExprSPtr expr = cast_expr;
        const auto status = FileScannerV2::TEST_rewrite_slot_refs_to_global_index(
                &expr, {{9, format::GlobalIndex(1)}});
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(expr->get_num_children(), 1);
        const auto* rewritten_child = assert_cast<const VSlotRef*>(expr->children()[0].get());
        EXPECT_EQ(rewritten_child->slot_id(), 1);
        EXPECT_EQ(rewritten_child->column_id(), 1);
        EXPECT_EQ(rewritten_child->column_name(), "nested_value");
    }
    {
        const auto node = bool_in_pred_node();
        auto impl = VDirectInPredicate::create_shared(node, nullptr);
        impl->add_child(slot_ref(11, 11, int_type, "rf_value"));
        VExprSPtr expr = RuntimeFilterExpr::create_shared(node, impl, 0.4, false, 7);
        const auto status = FileScannerV2::TEST_rewrite_slot_refs_to_global_index(
                &expr, {{11, format::GlobalIndex(2)}});
        ASSERT_TRUE(status.ok()) << status;

        auto* runtime_filter = assert_cast<RuntimeFilterExpr*>(expr.get());
        auto rewritten_impl = runtime_filter->get_impl();
        ASSERT_NE(rewritten_impl, nullptr);
        ASSERT_EQ(rewritten_impl->get_num_children(), 1);
        const auto* rewritten_child =
                assert_cast<const VSlotRef*>(rewritten_impl->children()[0].get());
        EXPECT_EQ(rewritten_child->slot_id(), 2);
        EXPECT_EQ(rewritten_child->column_id(), 2);
        EXPECT_EQ(rewritten_child->column_name(), "rf_value");
    }
}

} // namespace doris
