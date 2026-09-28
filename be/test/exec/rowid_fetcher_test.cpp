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

#include "exec/rowid_fetcher.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "exec/operator/file_scan_operator.h"
#include "exec/scan/file_scanner_v2.h"
#include "format_v2/column_mapper.h"
#include "format_v2/table/hive_reader.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris {

// source_column_key() decides which projected slots collapse onto a single scanned
// column. Two slots that name the same physical column must produce the same key, and
// any two slots that do not must produce different ones -- a false merge drops a column
// the result block still expects, which is the crash this keying was introduced to fix.
class RowIdStorageReaderTest : public testing::Test {
public:
    static std::string key_of(const SlotDescriptor& slot, uint32_t column_idx) {
        return RowIdStorageReader::source_column_key(slot, column_idx);
    }

protected:
    struct SlotSpec {
        std::string col_name = "c";
        int32_t slot_id = 0;
        PrimitiveType type = TYPE_INT;
        int32_t col_unique_id = 1;
        std::vector<std::string> column_paths = {};
        TColumnAccessPaths access_paths = {};
    };

    static SlotDescriptor make_slot(const SlotSpec& spec) {
        TSlotDescriptor tdesc = TSlotDescriptorBuilder()
                                        .type(spec.type)
                                        .nullable(true)
                                        .column_name(spec.col_name)
                                        .column_pos(0)
                                        .build();
        tdesc.__set_id(spec.slot_id);
        tdesc.__set_col_unique_id(spec.col_unique_id);
        tdesc.__set_column_paths(spec.column_paths);
        if (!spec.access_paths.empty()) {
            tdesc.__set_all_access_paths(spec.access_paths);
        }
        return SlotDescriptor(tdesc);
    }

    static TColumnAccessPath data_path(const std::vector<std::string>& path) {
        TColumnAccessPath access_path;
        access_path.type = TAccessPathType::DATA;
        TDataAccessPath data;
        data.__set_path(path);
        access_path.__set_data_access_path(data);
        return access_path;
    }

    static TColumnAccessPath bare_path() {
        TColumnAccessPath access_path;
        access_path.type = TAccessPathType::DATA;
        return access_path;
    }
};

TEST_F(RowIdStorageReaderTest, ExternalScannerSelectionRespectsRolloutOption) {
    for (auto format : {TFileFormatType::FORMAT_PARQUET, TFileFormatType::FORMAT_ORC}) {
        TFileScanRangeParams params;
        params.__set_format_type(format);
        TFileRangeDesc range;
        for (const auto& table_format : {"hive", "iceberg", "tvf"}) {
            TTableFormatFileDesc table;
            table.__set_table_format_type(table_format);
            params.__set_table_format_params(table);
            range.__set_table_format_params(table);
            for (int option = 0; option < 3; ++option) {
                TQueryOptions options;
                if (option != 0) {
                    options.__set_enable_file_scanner_v2(option == 2);
                } else {
                    // An absent Thrift field must not enable V2 even if its value defaults to true.
                    options.enable_file_scanner_v2 = true;
                    options.__isset.enable_file_scanner_v2 = false;
                }
                EXPECT_EQ(RowIdStorageReader::should_use_file_scanner_v2(options, params, range),
                          FileScanLocalState::TEST_should_use_file_scanner_v2(options, false,
                                                                              params));
                EXPECT_EQ(RowIdStorageReader::should_use_file_scanner_v2(options, params, range),
                          option == 2);
            }
        }
    }
}

TEST_F(RowIdStorageReaderTest, ExternalScannerSelectionKeepsUnsupportedFormatsOnV1) {
    TQueryOptions options;
    options.__set_enable_file_scanner_v2(true);
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    TFileRangeDesc range;
    range.__set_format_type(TFileFormatType::FORMAT_JNI);
    EXPECT_FALSE(RowIdStorageReader::should_use_file_scanner_v2(options, params, range));
    range.__set_format_type(TFileFormatType::FORMAT_ORC);
    TTableFormatFileDesc table;
    table.__set_table_format_type("transactional_hive");
    params.__set_table_format_params(table);
    range.__set_table_format_params(table);
    EXPECT_FALSE(RowIdStorageReader::should_use_file_scanner_v2(options, params, range));
}

TEST_F(RowIdStorageReaderTest, ExternalFetchPreservesIcebergFileMetadata) {
    TFileRangeDesc range;
    range.__set_path("normalized/data.parquet");
    TIcebergFileDesc iceberg;
    iceberg.__set_original_file_path("s3://bucket/data.parquet");
    iceberg.__set_format_version(3);
    iceberg.__set_first_row_id(128);
    iceberg.__set_last_updated_sequence_number(7);
    TIcebergDeleteFileDesc deletes;
    deletes.__set_path("s3://bucket/deletes.parquet");
    iceberg.__set_delete_files({deletes});
    range.table_format_params.__set_iceberg_params(iceberg);

    const auto fetch_range = RowIdStorageReader::build_external_fetch_range(range);
    const auto& fetch_iceberg = fetch_range.table_format_params.iceberg_params;
    EXPECT_TRUE(fetch_iceberg.__isset.original_file_path);
    EXPECT_EQ(fetch_iceberg.original_file_path, iceberg.original_file_path);
    EXPECT_EQ(fetch_iceberg.format_version, 3);
    EXPECT_EQ(fetch_iceberg.first_row_id, 128);
    EXPECT_EQ(fetch_iceberg.last_updated_sequence_number, 7);
    EXPECT_TRUE(fetch_iceberg.delete_files.empty());
    EXPECT_EQ(range.table_format_params.iceberg_params, iceberg);
}

TEST_F(RowIdStorageReaderTest, ExternalFetchPreservesPrunedMetadataCategories) {
    TFileScanRangeParams source_params;
    source_params.__set_column_name_to_category(
            {{"_file", TColumnCategory::SYNTHESIZED},
             {"_pos", TColumnCategory::SYNTHESIZED},
             {"generated_col", TColumnCategory::GENERATED},
             {"partition_col", TColumnCategory::PARTITION_KEY}});
    // Phase one projects only the sort key; none of these fetch slots survives in required_slots.
    TFileScanSlotInfo sort_slot;
    sort_slot.__set_slot_id(99);
    sort_slot.__set_category(TColumnCategory::REGULAR);
    source_params.__set_required_slots({sort_slot});
    source_params.__set_column_idxs({0});
    std::vector<SlotDescriptor> slots;
    for (const auto* name : {"_file", "_pos", "generated_col", "partition_col", "value"}) {
        slots.emplace_back(
                make_slot({.col_name = name,
                           .slot_id = static_cast<int32_t>(slots.size()),
                           .type = std::string_view(name) == "_file" ? TYPE_STRING : TYPE_BIGINT}));
    }
    const auto params = RowIdStorageReader::build_external_scan_params(
            source_params, TFileRangeDesc {}, slots, {3, 4, 1, 2, 0});
    EXPECT_EQ(params.column_idxs, (std::vector<int32_t> {1, 0}));
    const std::vector<TColumnCategory::type> categories {
            TColumnCategory::SYNTHESIZED, TColumnCategory::SYNTHESIZED, TColumnCategory::GENERATED,
            TColumnCategory::PARTITION_KEY, TColumnCategory::REGULAR};
    std::vector<format::ColumnDefinition> columns;
    for (size_t i = 0; i < slots.size(); ++i) {
        const auto& info = params.required_slots[i];
        EXPECT_TRUE(info.__isset.category);
        EXPECT_EQ(info.category, categories[i]);
        EXPECT_EQ(info.is_file_slot, i == 2 || i == 4);
        EXPECT_EQ(FileScannerV2::TEST_is_partition_slot(info, slots[i].col_name()), i == 3);
        auto column = FileScannerV2::_build_table_column(&slots[i]);
        column.is_synthesized =
                info.__isset.category && info.category == TColumnCategory::SYNTHESIZED;
        columns.emplace_back(std::move(column));
    }
    format::TableColumnMapper mapper({.mode = format::TableColumnMappingMode::BY_NAME,
                                      .enable_iceberg_metadata_virtual_columns = true});
    ASSERT_TRUE(mapper.create_mapping({columns[0], columns[1]}, {}, {}).ok());
    EXPECT_EQ(mapper.mappings()[0].virtual_column_type,
              format::TableVirtualColumnType::ICEBERG_FILE_PATH);
    EXPECT_EQ(mapper.mappings()[1].virtual_column_type,
              format::TableVirtualColumnType::ICEBERG_ROW_POSITION);

    // An authoritative empty map means ordinary physical columns, even for metadata spellings.
    source_params.__set_column_name_to_category({});
    const auto physical_params = RowIdStorageReader::build_external_scan_params(
            source_params, TFileRangeDesc {}, slots, {3, 4, 1, 2, 0});
    for (const auto& info : physical_params.required_slots) {
        EXPECT_EQ(info.category, TColumnCategory::REGULAR);
        EXPECT_TRUE(info.is_file_slot);
    }
}

// Row-id fetch rebuilds the projection after TopN. Hive's positional mapper must consume
// indexes only for physical columns, including when partition columns precede file columns.
TEST_F(RowIdStorageReaderTest, ExternalFetchPartitionSlotsPreserveHivePositionMapping) {
    for (const auto format : {TFileFormatType::FORMAT_ORC, TFileFormatType::FORMAT_PARQUET}) {
        TQueryOptions options;
        options.__set_hive_orc_use_column_names(false);
        options.__set_hive_parquet_use_column_names(false);
        RuntimeState state(options, TQueryGlobals {});
        TFileScanRangeParams source_params;
        source_params.__set_format_type(format);
        source_params.__set_column_idxs({0, 1, 2});
        TFileScanSlotInfo old_slot;
        old_slot.__set_slot_id(99);
        source_params.__set_required_slots({old_slot});
        source_params.__set_slot_name_to_schema_pos({{"old_column", 0}});
        TFileRangeDesc range;
        range.__set_columns_from_path_keys({"partition_col"});

        for (const auto& names : {std::vector<std::string> {"value", "partition_col"},
                                  std::vector<std::string> {"partition_col", "value", "id"},
                                  std::vector<std::string> {"partition_col"},
                                  std::vector<std::string> {"value", "id"}}) {
            SCOPED_TRACE(fmt::format("format={}, columns={}", static_cast<int>(format),
                                     fmt::join(names, ",")));
            std::vector<SlotDescriptor> slots;
            std::vector<uint32_t> indices;
            std::vector<int32_t> file_indices;
            for (const auto& name : names) {
                slots.emplace_back(make_slot(
                        {.col_name = name, .slot_id = static_cast<int32_t>(slots.size())}));
                const uint32_t index = name == "partition_col" ? 3 : name == "value" ? 2 : 0;
                indices.emplace_back(index);
                if (name != "partition_col") {
                    file_indices.emplace_back(index);
                }
            }
            const auto params = RowIdStorageReader::build_external_scan_params(source_params, range,
                                                                               slots, indices);
            ASSERT_EQ(params.required_slots.size(), slots.size());
            EXPECT_EQ(params.column_idxs, file_indices);
            EXPECT_FALSE(params.slot_name_to_schema_pos.contains("old_column"));
            format::ProjectedColumnBuildContext context {
                    .scan_params = &params, .range = &range, .runtime_state = &state};
            format::hive::HiveReader reader;
            for (size_t i = 0; i < slots.size(); ++i) {
                const auto& slot_info = params.required_slots[i];
                const auto& name = names[i];
                const bool is_partition = name == "partition_col";
                EXPECT_TRUE(slot_info.__isset.slot_id);
                EXPECT_EQ(slot_info.slot_id, slots[i].id());
                EXPECT_TRUE(slot_info.__isset.is_file_slot);
                EXPECT_EQ(FileScannerV2::TEST_is_partition_slot(slot_info, name), is_partition);
                format::ColumnDefinition column;
                column.name = name;
                column.type = slots[i].get_data_type_ptr();
                const auto status = reader.annotate_projected_column(slot_info, &context, &column);
                ASSERT_TRUE(status.ok()) << status;
                if (!is_partition) {
                    EXPECT_EQ(column.get_identifier_position(), indices[i]);
                }
            }
            EXPECT_EQ(context.next_file_column_idx, file_indices.size());
            EXPECT_TRUE(reader.validate_projected_columns(context).ok());
        }
    }
}

TEST_F(RowIdStorageReaderTest, SameSourceColumnSharesKey) {
    // The bug case: one physical column projected twice must dedup onto one scan column.
    const SlotDescriptor first = make_slot({});
    const SlotDescriptor second = make_slot({});
    EXPECT_EQ(key_of(first, 3), key_of(second, 3));
}

TEST_F(RowIdStorageReaderTest, ColumnIndexSeparatesKeys) {
    const SlotDescriptor slot = make_slot({});
    EXPECT_NE(key_of(slot, 3), key_of(slot, 4));
}

TEST_F(RowIdStorageReaderTest, ColumnNameSeparatesKeys) {
    EXPECT_NE(key_of(make_slot({.col_name = "a"}), 0), key_of(make_slot({.col_name = "b"}), 0));
}

TEST_F(RowIdStorageReaderTest, UniqueIdSeparatesKeys) {
    EXPECT_NE(key_of(make_slot({.col_unique_id = 1}), 0),
              key_of(make_slot({.col_unique_id = 2}), 0));
}

TEST_F(RowIdStorageReaderTest, NameAndIndexBoundaryIsNotAmbiguous) {
    // Without length prefixes, "a" + idx 12 and "a1" + idx 2 both flatten to "a12".
    EXPECT_NE(key_of(make_slot({.col_name = "a"}), 12), key_of(make_slot({.col_name = "a1"}), 2));
}

TEST_F(RowIdStorageReaderTest, PathComponentBoundaryIsNotAmbiguous) {
    // The concatenation hazard the length prefix exists for: ["a", "b"] and ["a:b"] are
    // different nested columns but share the naive ':'-joined spelling.
    EXPECT_NE(key_of(make_slot({.column_paths = {"a", "b"}}), 0),
              key_of(make_slot({.column_paths = {"a:b"}}), 0));
}

TEST_F(RowIdStorageReaderTest, EmptyPathIsNotTheSameAsNoPath) {
    EXPECT_NE(key_of(make_slot({.column_paths = {}}), 0),
              key_of(make_slot({.column_paths = {""}}), 0));
}

TEST_F(RowIdStorageReaderTest, PathOrderMatters) {
    EXPECT_NE(key_of(make_slot({.column_paths = {"a", "b"}}), 0),
              key_of(make_slot({.column_paths = {"b", "a"}}), 0));
}

TEST_F(RowIdStorageReaderTest, EqualPathsShareKey) {
    EXPECT_EQ(key_of(make_slot({.column_paths = {"a", "b"}}), 0),
              key_of(make_slot({.column_paths = {"a", "b"}}), 0));
}

TEST_F(RowIdStorageReaderTest, AccessPathSeparatesKeys) {
    EXPECT_NE(key_of(make_slot({.access_paths = {data_path({"a"})}}), 0),
              key_of(make_slot({.access_paths = {data_path({"b"})}}), 0));
}

TEST_F(RowIdStorageReaderTest, AbsentAccessPathIsNotAnEmptyOne) {
    // The presence bit: an unset data_access_path must not collide with one that is set
    // but carries no components.
    EXPECT_NE(key_of(make_slot({.access_paths = {bare_path()}}), 0),
              key_of(make_slot({.access_paths = {data_path({})}}), 0));
}

TEST_F(RowIdStorageReaderTest, AccessPathCountSeparatesKeys) {
    EXPECT_NE(key_of(make_slot({.access_paths = {data_path({"a"})}}), 0),
              key_of(make_slot({.access_paths = {data_path({"a"}), data_path({"b"})}}), 0));
}

// Runs every submitted task on the submitting thread. The point of these cases is which
// status reaches the caller, not the threading, and inline execution keeps them
// deterministic.
class InlineScanScheduler : public ScannerScheduler {
public:
    Status start(int, int, int, int) override { return Status::OK(); }
    void stop() override {}
    Status submit_scan_task(SimplifiedScanTask scan_task) override {
        scan_task.scan_func();
        return Status::OK();
    }
    Status submit_scan_task(SimplifiedScanTask scan_task, const std::string&) override {
        scan_task.scan_func();
        return Status::OK();
    }
    void reset_thread_num(int, int, int) override {}
    int get_queue_size() override { return 0; }
    int get_active_threads() override { return 0; }
    std::vector<int> thread_debug_info() override { return {}; }
    Status schedule_scan_task(std::shared_ptr<ScannerContext>, std::shared_ptr<ScanTask>,
                              std::unique_lock<std::mutex>&) override {
        return Status::OK();
    }
};

// submit_external_scan_tasks() signals completion from a Defer, so a worker that leaves
// without publishing its status would still wake the waiter and the caller would report
// success over a partially filled result block.
class SubmitExternalScanTasksTest : public RowIdStorageReaderTest {
protected:
    static constexpr size_t kTaskCount = 3;

    static Status run_tasks(const std::function<Status(size_t)>& run_task) {
        InlineScanScheduler scheduler;
        std::counting_semaphore<> semaphore {kTaskCount};
        return RowIdStorageReader::submit_external_scan_tasks(
                &scheduler, semaphore, kTaskCount,
                [](size_t idx) { return fmt::format("task-{}", idx); }, run_task);
    }
};

TEST_F(SubmitExternalScanTasksTest, AllTasksSucceedingReturnsOk) {
    size_t ran = 0;
    EXPECT_TRUE(run_tasks([&](size_t) -> Status {
                    ++ran;
                    return Status::OK();
                }).ok());
    EXPECT_EQ(ran, kTaskCount);
}

TEST_F(SubmitExternalScanTasksTest, ReturnedErrorReachesTheCaller) {
    const Status result = run_tasks([](size_t idx) -> Status {
        return idx == kTaskCount - 1 ? Status::InternalError("scanner returned an error")
                                     : Status::OK();
    });
    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.to_string().find("scanner returned an error"), std::string::npos);
}

TEST_F(SubmitExternalScanTasksTest, ThrownExceptionReachesTheCaller) {
    // The last task is the interesting one: it is the completion that releases the
    // waiter, so a status lost here is a status the caller never sees.
    const Status result = run_tasks([](size_t idx) -> Status {
        if (idx == kTaskCount - 1) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "scanner threw");
        }
        return Status::OK();
    });
    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.to_string().find("scanner threw"), std::string::npos);
}

} // namespace doris
