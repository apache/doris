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

#include "cloud/config.h"
#include "common/config.h"
#include "common/consts.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/operator/file_scan_operator.h"
#include "exec/runtime_filter/runtime_filter_definitions.h"
#include "exec/scan/file_scan_io_context.h"
#include "exec/scan/file_scanner.h"
#include "exec/scan/split_source_connector.h"
#include "exprs/create_predicate_function.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/vbloom_predicate.h"
#include "exprs/vdirect_in_predicate.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format_v2/expr/cast.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
namespace {

constexpr int kIcebergPositionDeleteContent = 1;
constexpr int kIcebergDeletionVectorContent = 3;

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

TFileRangeDesc iceberg_position_deletes_range(TFileFormatType::type format_type, int content) {
    auto range = range_with_format("iceberg", format_type);
    TIcebergFileDesc iceberg_params;
    iceberg_params.__set_content(content);
    range.table_format_params.__set_iceberg_params(std::move(iceberg_params));
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

TExprNode bool_in_pred_node();

class UnsafePartitionPredicate final : public VExpr {
public:
    UnsafePartitionPredicate() : VExpr(std::make_shared<DataTypeUInt8>(), false) {}

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t count,
                               ColumnPtr& result_column) const override {
        auto result = ColumnUInt8::create();
        result->get_data().resize_fill(count, 1);
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }
    bool is_safe_to_execute_on_selected_rows() const override { return false; }

private:
    const std::string _expr_name = "UnsafePartitionPredicate";
};

class UndigestibleRuntimePredicate final : public VExpr {
public:
    UndigestibleRuntimePredicate() : VExpr(std::make_shared<DataTypeUInt8>(), false) {}

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t count,
                               ColumnPtr& result_column) const override {
        result_column = ColumnUInt8::create(count, 1);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }
    uint64_t get_digest(uint64_t) const override { return 0; }

private:
    const std::string _expr_name = "undigestible_runtime_predicate";
};

VExprContextSPtr runtime_filter_context(VExprSPtr impl, int filter_id) {
    const auto node = bool_in_pred_node();
    return VExprContext::create_shared(
            RuntimeFilterExpr::create_shared(node, std::move(impl), 0.4, false, filter_id));
}

class CloudFileCacheConfigGuard {
public:
    CloudFileCacheConfigGuard()
            : _cloud_unique_id(config::cloud_unique_id),
              _enable_file_cache(config::enable_file_cache) {}

    ~CloudFileCacheConfigGuard() {
        config::cloud_unique_id = _cloud_unique_id;
        config::enable_file_cache = _enable_file_cache;
    }

private:
    std::string _cloud_unique_id;
    bool _enable_file_cache;
};

TUniqueId make_query_id() {
    TUniqueId query_id;
    query_id.hi = 100;
    query_id.lo = 200;
    return query_id;
}

TNetworkAddress make_fe_addr() {
    TNetworkAddress fe_addr;
    fe_addr.hostname = "127.0.0.1";
    fe_addr.port = 9030;
    return fe_addr;
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

VExprContextSPtr int_in_runtime_filter(const std::vector<int32_t>& values, int filter_id) {
    std::shared_ptr<HybridSetBase> filter(create_set(PrimitiveType::TYPE_INT, false));
    for (const auto value : values) {
        filter->insert(&value);
    }
    const auto node = bool_in_pred_node();
    auto impl = VDirectInPredicate::create_shared(node, std::move(filter), true);
    impl->add_child(slot_ref(1, 0, std::make_shared<DataTypeInt32>(), "rf_key"));
    return runtime_filter_context(std::move(impl), filter_id);
}

VExprContextSPtr int_bloom_runtime_filter(const std::vector<int32_t>& values, int filter_id) {
    std::shared_ptr<BloomFilterFuncBase> filter(
            create_bloom_filter(PrimitiveType::TYPE_INT, false));
    RuntimeFilterParams params;
    params.filter_type = RuntimeFilterType::BLOOM_FILTER;
    params.column_return_type = PrimitiveType::TYPE_INT;
    params.bloom_filter_size = 1024;
    filter->init_params(&params);
    EXPECT_TRUE(filter->init_with_fixed_length(1024).ok());
    auto value_column = ColumnInt32::create();
    for (const auto value : values) {
        value_column->insert_value(value);
    }
    ColumnPtr values_column_ptr = std::move(value_column);
    filter->insert_fixed_len(values_column_ptr, 0);

    TExprNode node = bool_in_pred_node();
    node.__set_node_type(TExprNodeType::BLOOM_PRED);
    node.__set_opcode(TExprOpcode::RT_FILTER);
    auto impl = VBloomPredicate::create_shared(node);
    impl->set_filter(std::move(filter));
    impl->add_child(slot_ref(1, 0, std::make_shared<DataTypeInt32>(), "rf_key"));
    return runtime_filter_context(std::move(impl), filter_id);
}

VExprContextSPtr int_minmax_runtime_filter(int32_t upper_bound, int filter_id) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    VExprSPtr impl;
    TExprNode node;
    EXPECT_TRUE(create_vbin_predicate(int_type, TExprOpcode::LE, impl, &node, false).ok());
    impl->add_child(slot_ref(1, 0, int_type, "rf_key"));
    VExprSPtr literal;
    EXPECT_TRUE(create_literal(int_type, &upper_bound, literal).ok());
    impl->add_child(std::move(literal));
    return runtime_filter_context(std::move(impl), filter_id);
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

// Scenario: Iceberg position-delete system table splits use FileScannerV2 for both native delete
// formats and V3 deletion vectors. Avro remains unsupported and is rejected by FE before routing.
TEST(FileScannerV2Test, IcebergPositionDeletesSupportNativeFormats) {
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_PARQUET);

    const auto parquet_position_delete = iceberg_position_deletes_range(
            TFileFormatType::FORMAT_PARQUET, kIcebergPositionDeleteContent);
    const auto parquet_deletion_vector = iceberg_position_deletes_range(
            TFileFormatType::FORMAT_PARQUET, kIcebergDeletionVectorContent);
    const auto orc_position_delete = iceberg_position_deletes_range(TFileFormatType::FORMAT_ORC,
                                                                    kIcebergPositionDeleteContent);
    const auto avro_position_delete = iceberg_position_deletes_range(TFileFormatType::FORMAT_AVRO,
                                                                     kIcebergPositionDeleteContent);

    EXPECT_TRUE(FileScannerV2::is_supported(params, parquet_position_delete));
    EXPECT_TRUE(FileScannerV2::is_supported(params, parquet_deletion_vector));
    EXPECT_TRUE(FileScannerV2::is_supported(params, orc_position_delete));
    EXPECT_FALSE(FileScannerV2::is_supported(params, avro_position_delete));
}

// Ready IN/Bloom/MinMax runtime filters all expose their payload through get_digest(). Rebuilding
// the scanner digest must therefore be stable for the same payload and isolated for a different
// payload. An RF without a complete digest remains the zero-digest safety fallback.
TEST(FileScannerV2Test, ConditionCacheDigestIncludesRuntimeFilterPayload) {
    constexpr uint64_t seed = 12345;
    const auto digest = [](uint64_t initial_seed, const VExprContextSPtr& conjunct) {
        return Scanner::TEST_build_condition_cache_digest(initial_seed, {conjunct});
    };

    EXPECT_EQ(digest(seed, int_in_runtime_filter({7, 9}, 1)),
              digest(seed, int_in_runtime_filter({9, 7}, 1)));
    EXPECT_NE(digest(seed, int_in_runtime_filter({7, 9}, 1)),
              digest(seed, int_in_runtime_filter({8, 10}, 1)));

    EXPECT_EQ(digest(seed, int_bloom_runtime_filter({7, 9}, 2)),
              digest(seed, int_bloom_runtime_filter({7, 9}, 2)));
    EXPECT_NE(digest(seed, int_bloom_runtime_filter({7, 9}, 2)),
              digest(seed, int_bloom_runtime_filter({8, 10}, 2)));

    EXPECT_EQ(digest(seed, int_minmax_runtime_filter(9, 3)),
              digest(seed, int_minmax_runtime_filter(9, 3)));
    EXPECT_NE(digest(seed, int_minmax_runtime_filter(9, 3)),
              digest(seed, int_minmax_runtime_filter(10, 3)));

    EXPECT_EQ(digest(seed,
                     runtime_filter_context(std::make_shared<UndigestibleRuntimePredicate>(), 4)),
              0);
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

// Scenario: external file scan IO contexts are query readers for SELECT so the shared file-cache
// gate can apply the query-level remote scan write limiter.
TEST(FileScannerV2Test, FileScanIoContextPropagatesQueryLimiterForSelect) {
    CloudFileCacheConfigGuard config_guard;
    config::cloud_unique_id = "file_scanner_io_context_ut";
    config::enable_file_cache = true;

    TQueryOptions query_options;
    query_options.__set_query_type(TQueryType::SELECT);
    query_options.__set_file_cache_query_limit_bytes(0);

    const auto query_id = make_query_id();
    const auto fe_addr = make_fe_addr();
    auto query_ctx = MockQueryContext::create(query_id, ExecEnv::GetInstance(), query_options,
                                              fe_addr, true, fe_addr);
    ASSERT_NE(query_ctx->remote_scan_cache_write_limiter(), nullptr);

    MockRuntimeState state;
    state._query_id = query_id;
    state.set_query_options(query_options);
    state._query_ctx_uptr = query_ctx;
    state._query_ctx = query_ctx.get();

    auto io_ctx = create_file_scan_io_context(&state);

    EXPECT_EQ(io_ctx->query_id, &state.query_id());
    EXPECT_EQ(io_ctx->reader_type, ReaderType::READER_QUERY);
    EXPECT_EQ(io_ctx->remote_scan_cache_write_limiter,
              query_ctx->remote_scan_cache_write_limiter());
}

// Scenario: LOAD file scans keep non-query reader semantics even when the byte-limit option exists.
TEST(FileScannerV2Test, FileScanIoContextDoesNotMarkLoadAsQueryReader) {
    TQueryOptions query_options;
    query_options.__set_query_type(TQueryType::LOAD);
    query_options.__set_file_cache_query_limit_bytes(0);

    MockRuntimeState state;
    state._query_id = make_query_id();
    state.set_query_options(query_options);
    state._query_ctx = nullptr;

    auto io_ctx = create_file_scan_io_context(&state);

    EXPECT_EQ(io_ctx->query_id, &state.query_id());
    EXPECT_EQ(io_ctx->reader_type, ReaderType::UNKNOWN);
    EXPECT_EQ(io_ctx->remote_scan_cache_write_limiter, nullptr);
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
// TableReader. Explicit slot-id mappings use the required_slots order; missing mappings are an
// error because a scanner slot id is not a table-global ordinal.
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
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("Can not resolve source slot id 7"), std::string::npos);
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

TEST(FileScannerTest, PartitionPruningStopsAtUnsafePredicate) {
    const auto bool_type = std::make_shared<DataTypeUInt8>();
    auto unsafe_predicate = std::make_shared<UnsafePartitionPredicate>();
    unsafe_predicate->add_child(slot_ref(1, 0, bool_type, "part"));
    VExprContextSPtrs conjuncts {
            runtime_filter_context(slot_ref(1, 0, bool_type, "part"), 1),
            runtime_filter_context(std::move(unsafe_predicate), 2),
            runtime_filter_context(slot_ref(1, 0, bool_type, "part"), 3),
    };

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("file_scanner");
    FileScanner scanner(&state, &profile, nullptr, nullptr, nullptr);
    scanner.TEST_init_runtime_filter_partition_prune_ctxs(conjuncts, {{1, 0}});

    const auto& partition_conjuncts = scanner.TEST_runtime_filter_partition_prune_ctxs();
    ASSERT_EQ(partition_conjuncts.size(), 1);
    EXPECT_EQ(partition_conjuncts[0], conjuncts[0]);
}

} // namespace doris
