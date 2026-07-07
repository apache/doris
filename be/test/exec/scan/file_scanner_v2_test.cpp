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
#include "exec/scan/split_source_connector.h"
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

TScanRangeParams scan_range_param(const TFileRangeDesc& range) {
    TScanRangeParams params;
    params.scan_range.ext_scan_range.file_scan_range.ranges.push_back(range);
    return params;
}

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
            {"hive", TFileFormatType::FORMAT_ORC, std::nullopt, false},
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
            {"hive", TFileFormatType::FORMAT_PARQUET, TFileFormatType::FORMAT_ORC, false},
            {"hive", TFileFormatType::FORMAT_ORC, TFileFormatType::FORMAT_PARQUET, true},
            {"hive", TFileFormatType::FORMAT_PARQUET, TFileFormatType::FORMAT_CSV_PLAIN, true},
            {"hive", TFileFormatType::FORMAT_PARQUET, TFileFormatType::FORMAT_TEXT, true},
            {"hive", TFileFormatType::FORMAT_PARQUET, TFileFormatType::FORMAT_JSON, true},
            {"tvf", TFileFormatType::FORMAT_PARQUET, TFileFormatType::FORMAT_NATIVE, true},
            {"remote_doris", TFileFormatType::FORMAT_ARROW, std::nullopt, true},
            {"hive", TFileFormatType::FORMAT_ARROW, std::nullopt, false},
            {"", TFileFormatType::FORMAT_ARROW, std::nullopt, false},
            {"", TFileFormatType::FORMAT_WAL, std::nullopt, false},
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

// Scenario: SplitSourceConnector should route to FileScannerV2 only when every scan range in the
// source is supported; one unsupported table format or file format must make the match fail.
TEST(FileScannerV2Test, SplitSourceAllScanRangesMatchRequiresEveryRangeSupported) {
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_PARQUET);

    const auto supported = range_with_format("hive", TFileFormatType::FORMAT_PARQUET);
    const auto unsupported_table = range_with_format("lakesoul", TFileFormatType::FORMAT_PARQUET);
    const auto unsupported_format = range_with_format("hive", TFileFormatType::FORMAT_ORC);

    LocalSplitSourceConnector all_supported(
            {scan_range_param(supported),
             scan_range_param(range_with_format("iceberg", TFileFormatType::FORMAT_PARQUET))},
            1);
    EXPECT_TRUE(all_supported.all_scan_ranges_match(params, FileScannerV2::is_supported));

    LocalSplitSourceConnector hudi_supported(
            {scan_range_param(supported),
             scan_range_param(range_with_format("hudi", TFileFormatType::FORMAT_PARQUET))},
            1);
    EXPECT_TRUE(hudi_supported.all_scan_ranges_match(params, FileScannerV2::is_supported));

    LocalSplitSourceConnector table_mismatch(
            {scan_range_param(supported), scan_range_param(unsupported_table)}, 1);
    EXPECT_FALSE(table_mismatch.all_scan_ranges_match(params, FileScannerV2::is_supported));

    LocalSplitSourceConnector format_mismatch(
            {scan_range_param(supported), scan_range_param(unsupported_format)}, 1);
    EXPECT_FALSE(format_mismatch.all_scan_ranges_match(params, FileScannerV2::is_supported));
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
            {TFileFormatType::FORMAT_ORC, std::nullopt},
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
