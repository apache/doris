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

#include "format_v2/delimited_text/csv_reader.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <thread>

#include "common/config.h"
#include "common/consts.h"
#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exec/scan/scanner.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "format_v2/column_mapper.h"
#include "io/fs/stream_load_pipe.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "testutil/desc_tbl_builder.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/obj_storage_client_test_stub.h"
#include "testutil/scoped_temp_dir.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/s3_util.h"
#include "util/threadpool.h"

namespace doris::format::csv {
namespace {

class CsvPrefetchS3Client : public io::ObjStorageClientTestStub {
public:
    const std::string data = "id,name,score\n1,alice,10\n2,bob,20\n3,carol,30\n";
    const std::thread::id scanner_thread = std::this_thread::get_id();
    std::atomic<size_t> foreground_reads {0};
    std::atomic<size_t> reads {0};

    io::ObjStorageHeadResult head_object(const io::ObjStoragePath&) override {
        return {.resp = io::ObjStorageResponse::OK(),
                .file_size = static_cast<long long>(data.size())};
    }

    io::ObjStorageResponse get_object(const io::ObjStoragePath&, void* buffer, size_t offset,
                                      size_t bytes_read, size_t* size_return) override {
        ++reads;
        if (std::this_thread::get_id() == scanner_thread) {
            ++foreground_reads;
        }
        EXPECT_LE(offset + bytes_read, data.size());
        *size_return = std::min(bytes_read, data.size() - offset);
        std::memcpy(buffer, data.data() + offset, *size_return);
        return io::ObjStorageResponse::OK();
    }

    // Only HEAD and GET belong to this read test. Explicit overrides avoid generating unused
    // gMock expectation helpers for this anonymous-namespace client.
    io::ObjStorageUploadResult create_multipart_upload(const io::ObjStoragePath&) override {
        return {.resp = unexpected_operation()};
    }

    io::ObjStorageResponse put_object(const io::ObjStoragePath&, std::string_view) override {
        return unexpected_operation();
    }

    io::ObjStorageUploadResult upload_part(const io::ObjStoragePath&, const std::string&,
                                           std::string_view, int) override {
        return {.resp = unexpected_operation()};
    }

    io::ObjStorageResponse complete_multipart_upload(
            const io::ObjStoragePath&, const std::string&,
            const std::vector<io::ObjStorageCompletedPart>&) override {
        return unexpected_operation();
    }

    io::ObjStorageResponse delete_objects(const io::ObjStoragePath&,
                                          std::vector<std::string>) override {
        return unexpected_operation();
    }

    io::ObjStorageResponse delete_object(const io::ObjStoragePath&) override {
        return unexpected_operation();
    }

    std::string generate_presigned_url(const io::ObjStoragePath&, int64_t) override {
        ADD_FAILURE() << "Unexpected signed URL request in CSV prefetch test";
        return {};
    }

    io::ObjStorageListPageResult list_objects_page(const io::ObjStoragePath&,
                                                   std::string_view) override {
        return {.resp = unexpected_operation()};
    }

private:
    static io::ObjStorageResponse unexpected_operation() {
        ADD_FAILURE() << "Unexpected object storage operation in CSV prefetch test";
        return {.status = {TStatusCode::NOT_IMPLEMENTED_ERROR,
                           "CSV prefetch test only supports HEAD and GET"},
                .http_code = 0};
    }
};

TFileScanRangeParams csv_scan_params() {
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
    params.__set_file_type(TFileType::FILE_LOCAL);
    TFileAttributes attributes;
    TFileTextScanRangeParams text_params;
    text_params.__set_column_separator(",");
    text_params.__set_line_delimiter("\n");
    attributes.__set_text_params(std::move(text_params));
    attributes.__set_header_type(BeConsts::CSV_WITH_NAMES);
    params.__set_file_attributes(std::move(attributes));
    params.__set_column_idxs({0, 1, 2});
    return params;
}

std::unique_ptr<io::FileDescription> file_description(const std::string& path,
                                                      int64_t range_start_offset = 0,
                                                      int64_t range_size = -1) {
    auto desc = std::make_unique<io::FileDescription>();
    desc->path = path;
    desc->range_start_offset = range_start_offset;
    desc->range_size = range_size;
    desc->file_size = static_cast<int64_t>(std::filesystem::file_size(path));
    return desc;
}

std::unique_ptr<io::FileDescription> unknown_size_file_description(const std::string& path) {
    auto desc = std::make_unique<io::FileDescription>();
    desc->path = path;
    desc->range_start_offset = 0;
    desc->range_size = -1;
    desc->file_size = -1;
    return desc;
}

std::vector<SlotDescriptor*> build_slots(ObjectPool* pool) {
    DescriptorTblBuilder builder(pool);
    builder.declare_tuple()
            << TupleDescBuilder::SlotType {make_nullable(std::make_shared<DataTypeInt32>()), "id"}
            << TupleDescBuilder::SlotType {make_nullable(std::make_shared<DataTypeString>()),
                                           "name"}
            << TupleDescBuilder::SlotType {make_nullable(std::make_shared<DataTypeInt32>()),
                                           "score"};
    auto* desc_tbl = builder.build();
    return desc_tbl->get_tuple_descriptor(0)->slots();
}

SlotDescriptor* make_test_slot(ObjectPool* pool, int slot_id, int slot_idx, DataTypePtr type,
                               const std::string& name) {
    TSlotDescriptor slot_desc;
    slot_desc.__set_id(slot_id);
    slot_desc.__set_parent(0);
    slot_desc.__set_slotType(type->to_thrift());
    slot_desc.__set_columnPos(slot_idx);
    slot_desc.__set_byteOffset(0);
    slot_desc.__set_nullIndicatorByte(slot_idx / 8);
    slot_desc.__set_nullIndicatorBit(slot_idx % 8);
    slot_desc.__set_slotIdx(slot_idx);
    slot_desc.__set_isMaterialized(true);
    slot_desc.__set_colName(name);
    return pool->add(new SlotDescriptor(slot_desc));
}

std::vector<SlotDescriptor*> build_struct_slots(ObjectPool* pool) {
    const auto nullable_int = make_nullable(std::make_shared<DataTypeInt32>());
    const auto struct_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {nullable_int, nullable_int}, Strings {"a", "b"}));
    return {make_test_slot(pool, 0, 0, make_nullable(std::make_shared<DataTypeInt32>()), "id"),
            make_test_slot(pool, 1, 1, struct_type, "s"),
            make_test_slot(pool, 2, 2, make_nullable(std::make_shared<DataTypeInt32>()), "score")};
}

std::vector<SlotDescriptor*> build_nested_complex_slots(ObjectPool* pool) {
    const auto nullable_int = make_nullable(std::make_shared<DataTypeInt32>());
    const auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    const auto struct_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {nullable_int, nullable_string}, Strings {"a", "b"}));
    const auto array_type = make_nullable(std::make_shared<DataTypeArray>(struct_type));
    const auto map_type =
            make_nullable(std::make_shared<DataTypeMap>(nullable_string, struct_type));
    return {make_test_slot(pool, 0, 0, make_nullable(std::make_shared<DataTypeInt32>()), "id"),
            make_test_slot(pool, 1, 1, array_type, "xs"),
            make_test_slot(pool, 2, 2, map_type, "kv")};
}

std::vector<SlotDescriptor*> build_char_varchar_slots(ObjectPool* pool) {
    const auto nullable_char3 =
            make_nullable(std::make_shared<DataTypeString>(3, PrimitiveType::TYPE_CHAR));
    const auto nullable_varchar4 =
            make_nullable(std::make_shared<DataTypeString>(4, PrimitiveType::TYPE_VARCHAR));
    const auto struct_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {nullable_char3, nullable_varchar4}, Strings {"city", "country"}));
    return {make_test_slot(pool, 0, 0, make_nullable(std::make_shared<DataTypeInt32>()), "id"),
            make_test_slot(pool, 1, 1, nullable_char3, "city"),
            make_test_slot(pool, 2, 2, struct_type, "region")};
}

std::unique_ptr<CsvReader> create_reader(
        const std::string& path, TFileScanRangeParams* params,
        const std::vector<SlotDescriptor*>& slots, MockRuntimeState* state, RuntimeProfile* profile,
        int64_t range_start_offset = 0, int64_t range_size = -1,
        TFileCompressType::type range_compress_type = TFileCompressType::UNKNOWN,
        std::shared_ptr<io::IOContext> io_ctx = nullptr) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    system_properties->system_type = TFileType::FILE_LOCAL;
    auto desc = file_description(path, range_start_offset, range_size);
    auto reader = std::make_unique<CsvReader>(system_properties, desc, std::move(io_ctx), profile,
                                              params, slots, range_compress_type);
    EXPECT_TRUE(reader->init(state).ok());
    return reader;
}

std::unique_ptr<CsvReader> create_unknown_size_reader(const std::string& path,
                                                      TFileScanRangeParams* params,
                                                      const std::vector<SlotDescriptor*>& slots,
                                                      MockRuntimeState* state,
                                                      RuntimeProfile* profile) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    system_properties->system_type = TFileType::FILE_LOCAL;
    auto desc = unknown_size_file_description(path);
    auto reader =
            std::make_unique<CsvReader>(system_properties, desc, nullptr, profile, params, slots);
    EXPECT_TRUE(reader->init(state).ok());
    return reader;
}

Block make_block(const std::vector<ColumnDefinition>& schema,
                 const std::vector<int32_t>& local_ids) {
    Block block;
    for (const auto local_id : local_ids) {
        const auto it = std::find_if(schema.begin(), schema.end(), [&](const auto& column) {
            return column.local_id == local_id;
        });
        EXPECT_TRUE(it != schema.end());
        block.insert({it->type->create_column(), it->type, it->name});
    }
    return block;
}

std::string nullable_string_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    const auto& nested = assert_cast<const ColumnString&>(nullable.get_nested_column());
    return nested.get_data_at(row).to_string();
}

bool is_null_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    return nullable.is_null_at(row);
}

int32_t nullable_int_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    const auto& nested = assert_cast<const ColumnInt32&>(nullable.get_nested_column());
    return nested.get_data()[row];
}

int32_t nullable_struct_int_child_at(const IColumn& column, size_t child_index, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    const auto& struct_column = assert_cast<const ColumnStruct&>(nullable.get_nested_column());
    const auto& child_nullable =
            assert_cast<const ColumnNullable&>(struct_column.get_column(child_index));
    const auto& nested = assert_cast<const ColumnInt32&>(child_nullable.get_nested_column());
    return nested.get_data()[row];
}

int64_t counter_value(RuntimeProfile* profile, const std::string& name) {
    auto* counter = profile->get_counter(name);
    EXPECT_NE(counter, nullptr) << name;
    return counter == nullptr ? 0 : counter->value();
}

class NullableIntGreaterThanExpr final : public VExpr {
public:
    NullableIntGreaterThanExpr(size_t block_position, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _block_position(block_position),
              _value(value) {}

    const std::string& expr_name() const override { return _name; }

    bool is_constant() const override { return false; }

    Status execute_column_impl(VExprContext*, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        DORIS_CHECK(block != nullptr);
        const auto& nullable =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_block_position).column);
        const auto& data = assert_cast<const ColumnInt32&>(nullable.get_nested_column());

        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const auto source_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] =
                    !nullable.is_null_at(source_row) && data.get_element(source_row) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = std::make_shared<NullableIntGreaterThanExpr>(_block_position, _value);
        return Status::OK();
    }

private:
    size_t _block_position;
    int32_t _value;
    const std::string _name = "NullableIntGreaterThanExpr";
};

class StructIntChildGreaterThanExpr final : public VExpr {
public:
    StructIntChildGreaterThanExpr(size_t block_position, size_t child_index, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _block_position(block_position),
              _child_index(child_index),
              _value(value) {}

    const std::string& expr_name() const override { return _name; }

    bool is_constant() const override { return false; }

    Status execute_column_impl(VExprContext*, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        DORIS_CHECK(block != nullptr);
        const auto& nullable =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_block_position).column);
        const auto& struct_column = assert_cast<const ColumnStruct&>(nullable.get_nested_column());
        const auto& child_nullable =
                assert_cast<const ColumnNullable&>(struct_column.get_column(_child_index));
        const auto& child_data =
                assert_cast<const ColumnInt32&>(child_nullable.get_nested_column());

        auto result = ColumnUInt8::create();
        auto& data = result->get_data();
        data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const auto source_row = selector == nullptr ? row : (*selector)[row];
            data[row] = !nullable.is_null_at(source_row) &&
                        !child_nullable.is_null_at(source_row) &&
                        child_data.get_element(source_row) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = std::make_shared<StructIntChildGreaterThanExpr>(_block_position,
                                                                       _child_index, _value);
        return Status::OK();
    }

private:
    size_t _block_position;
    size_t _child_index;
    int32_t _value;
    const std::string _name = "StructIntChildGreaterThanExpr";
};

VExprContextSPtr prepared_conjunct(RuntimeState* state, const VExprSPtr& expr) {
    auto context = VExprContext::create_shared(expr);
    auto status = context->prepare(state, RowDescriptor());
    EXPECT_TRUE(status.ok()) << status;
    status = context->open(state);
    EXPECT_TRUE(status.ok()) << status;
    return context;
}

class PlainCsvSplitTest : public testing::Test {
protected:
    void read_range(const std::string& content, const std::string& delimiter, int64_t start,
                    int64_t size, bool count_only, std::vector<std::string>* values,
                    size_t* total_rows, int header_mode = 0) {
        const auto path = (_dir.path() / "split.csv").string();
        std::ofstream(path, std::ios::binary) << content;
        auto params = csv_scan_params();
        params.__set_compress_type(TFileCompressType::PLAIN);
        params.__set_column_idxs({0});
        params.file_attributes.__isset.header_type = false;
        params.file_attributes.text_params.__set_line_delimiter(delimiter);
        if (header_mode == 1) {
            params.file_attributes.__set_header_type(BeConsts::CSV_WITH_NAMES);
        } else if (header_mode == 2) {
            params.file_attributes.__set_header_type(BeConsts::CSV_WITH_NAMES_AND_TYPES);
        } else if (header_mode == 3) {
            params.file_attributes.__set_skip_lines(2);
        }
        ObjectPool pool;
        auto type = make_nullable(std::make_shared<DataTypeString>());
        std::vector<SlotDescriptor*> slots {make_test_slot(&pool, 0, 0, type, "id")};
        MockRuntimeState state;
        state._batch_size = 2;
        RuntimeProfile profile("plain_csv_split_test");
        auto read_blocks = [&](auto&& next_block) {
            bool eof = false;
            while (!eof) {
                Block block;
                block.insert({type->create_column(), type, "id"});
                size_t rows = 0;
                auto status = next_block(&block, &rows, &eof);
                ASSERT_TRUE(status.ok()) << status;
                ASSERT_EQ(rows, block.rows());
                *total_rows += rows;
                if (!count_only) {
                    for (size_t row = 0; row < rows; ++row) {
                        ASSERT_FALSE(is_null_at(*block.get_by_position(0).column, row));
                        values->push_back(
                                nullable_string_at(*block.get_by_position(0).column, row));
                    }
                }
            }
        };
        auto reader = create_reader(path, &params, slots, &state, &profile, start, size);
        auto request = std::make_shared<FileScanRequest>();
        request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
        ASSERT_TRUE(reader->open(request).ok());
        if (count_only) {
            FileAggregateRequest aggregate_request;
            aggregate_request.agg_type = TPushAggOp::type::COUNT;
            FileAggregateResult result;
            ASSERT_TRUE(reader->get_aggregate_result(aggregate_request, &result).ok());
            *total_rows += result.count;
        } else {
            read_blocks([&](Block* block, size_t* rows, bool* eof) {
                return reader->get_block(block, rows, eof);
            });
        }
    }

    doris::test::ScopedTempDirectory _dir {"doris_plain_csv_split_test"};
};

// Keep the exhaustive delimiter/split cross-product together to detect lost or duplicated rows.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(PlainCsvSplitTest, EveryByteBoundaryMatchesUnsplitRowsAndCount) {
    for (const std::string delimiter : {"\n", "\r\n", "ABCDE", "||", "aba"}) {
        const std::string extra = delimiter == "||" ? "|" : delimiter == "aba" ? "ba" : "";
        for (bool trailing : {false, true}) {
            const std::string content =
                    "1" + delimiter + extra + "2" + delimiter + "3" + (trailing ? delimiter : "");
            const auto file_size = static_cast<int64_t>(content.size());
            const std::vector<std::string> expected {"1", extra + "2", "3"};
            for (bool count_only : {false, true}) {
                SCOPED_TRACE(testing::Message() << "delimiter=" << delimiter << ", trailing="
                                                << trailing << ", count=" << count_only);
                std::vector<std::string> unsplit;
                size_t unsplit_rows = 0;
                ASSERT_NO_FATAL_FAILURE(read_range(content, delimiter, 0, file_size, count_only,
                                                   &unsplit, &unsplit_rows));
                ASSERT_EQ(unsplit_rows, expected.size());
                if (!count_only) {
                    ASSERT_EQ(unsplit, expected);
                }
                for (int64_t split = 1; split < file_size; ++split) {
                    SCOPED_TRACE(testing::Message() << "split=" << split);
                    std::vector<std::string> values;
                    size_t rows = 0;
                    ASSERT_NO_FATAL_FAILURE(
                            read_range(content, delimiter, 0, split, count_only, &values, &rows));
                    ASSERT_NO_FATAL_FAILURE(read_range(content, delimiter, split, file_size - split,
                                                       count_only, &values, &rows));
                    ASSERT_EQ(rows, expected.size());
                    if (!count_only) {
                        ASSERT_EQ(values, expected);
                    }
                }
                std::vector<std::string> values;
                size_t rows = 0;
                for (int64_t start = 0; start < file_size; ++start) {
                    ASSERT_NO_FATAL_FAILURE(
                            read_range(content, delimiter, start, 1, count_only, &values, &rows));
                }
                EXPECT_EQ(rows, expected.size());
                if (!count_only) {
                    EXPECT_EQ(values, expected);
                }
            }
        }
    }
}

// Keep the exhaustive delimiter/split cross-product together to detect lost or duplicated rows.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(PlainCsvSplitTest, FirstSplitStillHonorsHeadersAndSkipLines) {
    for (int header_mode : {1, 2, 3}) {
        const std::string header = header_mode == 1 ? "id||" : "id||String||";
        const std::string content = header + "1|||2||3";
        const auto split = static_cast<int64_t>(header.size() + 4);
        for (bool count_only : {false, true}) {
            SCOPED_TRACE(testing::Message()
                         << "header_mode=" << header_mode << ", count=" << count_only);
            std::vector<std::string> values;
            size_t rows = 0;
            ASSERT_NO_FATAL_FAILURE(
                    read_range(content, "||", 0, split, count_only, &values, &rows, header_mode));
            ASSERT_NO_FATAL_FAILURE(read_range(content, "||", split, content.size() - split,
                                               count_only, &values, &rows, header_mode));
            EXPECT_EQ(rows, 3);
            if (!count_only) {
                EXPECT_EQ(values, (std::vector<std::string> {"1", "|2", "3"}));
            }
        }
    }
}

class CsvV2ReaderTest : public testing::Test {
public:
    void SetUp() override {
        _test_dir = std::filesystem::temp_directory_path() / "doris_format_v2_csv_reader_test";
        std::filesystem::remove_all(_test_dir);
        std::filesystem::create_directories(_test_dir);
        _file_path = (_test_dir / "reader.csv").string();
        std::ofstream output(_file_path, std::ios::binary);
        output << "id,name,score\n";
        output << "1,alice,10\n";
        output << "2,bob,20\n";
        output.close();
        _slots = build_slots(&_pool);
        _params = csv_scan_params();
    }

    void TearDown() override { std::filesystem::remove_all(_test_dir); }

protected:
    ObjectPool _pool;
    MockRuntimeState _state;
    RuntimeProfile _profile {"csv_v2_reader_test"};
    std::filesystem::path _test_dir;
    std::string _file_path;
    std::vector<SlotDescriptor*> _slots;
    TFileScanRangeParams _params;
};

TEST_F(CsvV2ReaderTest, S3PrefetchPreservesSplitRowsAndReadStatistics) {
    auto* env = ExecEnv::GetInstance();
    auto saved_pool = std::move(env->_buffered_reader_prefetch_thread_pool);
    const auto saved_in_memory_size = config::in_memory_file_size;
    Defer restore([&] {
        env->_buffered_reader_prefetch_thread_pool.reset();
        env->_buffered_reader_prefetch_thread_pool = std::move(saved_pool);
        config::in_memory_file_size = saved_in_memory_size;
        S3ClientFactory::instance().clear_client_creator_for_test();
    });
    ASSERT_TRUE(ThreadPoolBuilder("CsvPrefetchTest")
                        .set_min_threads(1)
                        .set_max_threads(1)
                        .build(&env->_buffered_reader_prefetch_thread_pool)
                        .ok());
    // Keep the small fixture on the same prefetch path as a large S3 CSV file.
    config::in_memory_file_size = 0;
    auto client = std::make_shared<CsvPrefetchS3Client>();
    S3ClientFactory::instance().set_client_creator_for_test(
            [client](const S3ClientConf&) { return client; });
    _params.__set_file_type(TFileType::FILE_S3);
    _state._query_options.__set_enable_file_cache(false);

    // Cover a complete file, a split starting inside Alice's row, and an unknown-size first split.
    for (const int64_t split_start : {0, 17, -1}) {
        SCOPED_TRACE(split_start);
        auto properties = std::make_shared<io::FileSystemProperties>();
        properties->system_type = TFileType::FILE_S3;
        properties->properties = {{"AWS_ENDPOINT", "http://csv-prefetch.invalid"},
                                  {"AWS_REGION", "test-region"}};
        auto desc = std::make_unique<io::FileDescription>();
        desc->path = "s3://csv-prefetch/reader.csv";
        desc->range_start_offset = std::max<int64_t>(0, split_start);
        desc->file_size = split_start < 0 ? -1 : static_cast<int64_t>(client->data.size());
        desc->range_size = desc->file_size - desc->range_start_offset;
        io::FileReaderStats stats;
        auto io_ctx = std::make_shared<io::IOContext>();
        io_ctx->file_reader_stats = &stats;
        auto reader =
                std::make_unique<CsvReader>(properties, desc, io_ctx, &_profile, &_params, _slots);
        ASSERT_TRUE(reader->init(&_state).ok());
        std::vector<ColumnDefinition> schema;
        ASSERT_TRUE(reader->get_schema(&schema).ok());
        auto request = std::make_shared<FileScanRequest>();
        for (int32_t id = 0; id < 3; ++id) {
            request->non_predicate_columns.push_back(
                    LocalColumnIndex::top_level(LocalColumnId(id)));
            request->local_positions.emplace(LocalColumnId(id), LocalIndex(id));
        }
        ASSERT_TRUE(reader->open(request).ok());
        auto block = make_block(schema, {0, 1, 2});
        size_t rows = 0;
        bool eof = false;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        ASSERT_EQ(rows, split_start > 0 ? 2 : 3);
        EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), split_start > 0 ? 2 : 1);
        EXPECT_EQ(nullable_string_at(*block.get_by_position(1).column, rows - 1), "carol");
        EXPECT_GT(stats.read_calls, 0);
        EXPECT_GT(stats.read_bytes, 0);
        const auto previous_prefetch_io = counter_value(&_profile, "PreRequestIO");
        ASSERT_TRUE(reader->close().ok());
        EXPECT_GT(counter_value(&_profile, "PreRequestIO"), previous_prefetch_io);
        EXPECT_GT(counter_value(&_profile, "PreRequestBytes"), 0);
        EXPECT_GT(counter_value(&_profile, "ReadTime"), 0);
        const auto collected_prefetch_io = counter_value(&_profile, "PreRequestIO");
        ASSERT_TRUE(reader->close().ok());
        EXPECT_EQ(counter_value(&_profile, "PreRequestIO"), collected_prefetch_io);
    }
    EXPECT_GT(client->reads.load(), 0);
    EXPECT_EQ(client->foreground_reads.load(), 0);
}

// Scenario: CSV v2 exposes FE-provided file slots as nullable file-local schema using column_idxs
// as CSV field ordinals.
TEST_F(CsvV2ReaderTest, SchemaUsesSlotTypesAndColumnIdxs) {
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);
    EXPECT_EQ(schema[0].name, "id");
    EXPECT_EQ(schema[0].local_id, 0);
    EXPECT_TRUE(schema[0].type->is_nullable());
    EXPECT_EQ(schema[1].name, "name");
    EXPECT_EQ(schema[1].local_id, 1);
    EXPECT_TRUE(schema[1].type->is_nullable());
}

// Scenario: FE slot types for CSV are table target types. CHAR/VARCHAR length is not stored in the
// CSV file, so the file schema must expose bounded strings as unbounded STRING. Otherwise
// TableReader believes the file value already satisfies the table length and skips truncation.
TEST_F(CsvV2ReaderTest, SchemaTreatsCharVarcharSlotsAsUnboundedFileStrings) {
    auto slots = build_char_varchar_slots(&_pool);
    auto reader = create_reader(_file_path, &_params, slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);

    const auto city_type = remove_nullable(schema[1].type);
    EXPECT_EQ(city_type->get_primitive_type(), TYPE_STRING);
    EXPECT_EQ(assert_cast<const DataTypeString*>(city_type.get())->len(), -1);

    const auto region_type = remove_nullable(schema[2].type);
    ASSERT_EQ(region_type->get_primitive_type(), TYPE_STRUCT);
    const auto* region_struct = assert_cast<const DataTypeStruct*>(region_type.get());
    ASSERT_EQ(region_struct->get_elements().size(), 2);
    EXPECT_EQ(remove_nullable(region_struct->get_element(0))->get_primitive_type(), TYPE_STRING);
    EXPECT_EQ(remove_nullable(region_struct->get_element(1))->get_primitive_type(), TYPE_STRING);
    ASSERT_EQ(schema[2].children.size(), 2);
    EXPECT_EQ(remove_nullable(schema[2].children[0].type)->get_primitive_type(), TYPE_STRING);
    EXPECT_EQ(remove_nullable(schema[2].children[1].type)->get_primitive_type(), TYPE_STRING);
}

// Scenario: CSV is row-oriented and cannot lazy-read predicate columns separately. The reader
// declares that capability by choosing MaterializedColumnMapper itself.
TEST_F(CsvV2ReaderTest, CreatesMaterializedColumnMapper) {
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    auto mapper = reader->create_column_mapper({.mode = TableColumnMappingMode::BY_NAME});

    ASSERT_NE(dynamic_cast<MaterializedColumnMapper*>(mapper.get()), nullptr);
}

// Scenario: CSV v2 exposes delimited-text profile counters for read, parse, deserialize, and
// file-local conjunct filtering, so scanner profiles can explain where row-reader time is spent.
TEST_F(CsvV2ReaderTest, ProfileCountersTrackReadParseDeserializeAndFilter) {
    const auto profile_path = (_test_dir / "profile.csv").string();
    std::ofstream output(profile_path, std::ios::binary);
    output << "id,name,score\n";
    output << "\n";
    output << "1,alice,10\n";
    output << "2,bob,20\n";
    output.close();

    _state._query_options.__set_read_csv_empty_line_as_null(true);
    io::FileReaderStats file_reader_stats;
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->file_reader_stats = &file_reader_stats;
    auto reader = create_reader(profile_path, &_params, _slots, &_state, &_profile, 0, -1,
                                TFileCompressType::UNKNOWN, io_ctx);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0)),
                                      LocalColumnIndex::top_level(LocalColumnId(2))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    request->local_positions.emplace(LocalColumnId(2), LocalIndex(1));
    request->conjuncts = {
            prepared_conjunct(&_state, std::make_shared<NullableIntGreaterThanExpr>(1, 15))};
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0, 2});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 2);

    EXPECT_NE(_profile.get_counter("OpenFileTime"), nullptr);
    EXPECT_NE(_profile.get_counter("CreateLineReaderTime"), nullptr);
    EXPECT_NE(_profile.get_counter("SampledReadLineTime"), nullptr);
    EXPECT_NE(_profile.get_counter("SampledSplitLineTime"), nullptr);
    EXPECT_NE(_profile.get_counter("SampledDeserializeTime"), nullptr);
    EXPECT_NE(_profile.get_counter("ConjunctFilterTime"), nullptr);
    EXPECT_NE(_profile.get_counter("DeleteConjunctFilterTime"), nullptr);
    EXPECT_EQ(counter_value(&_profile, "RawLinesRead"), 3);
    EXPECT_EQ(counter_value(&_profile, "RowsReadBeforeFilter"), 3);
    EXPECT_EQ(counter_value(&_profile, "DelimitedRowsFilteredByConjunct"), 2);
    EXPECT_EQ(io_ctx->predicate_filtered_rows, 2);
    EXPECT_EQ(file_reader_stats.read_rows, 3);
    EXPECT_EQ(counter_value(&_profile, "RowsFilteredByDeleteConjunct"), 0);
    EXPECT_EQ(counter_value(&_profile, "RowsReturned"), 1);
    EXPECT_EQ(counter_value(&_profile, "EmptyLinesRead"), 1);
    EXPECT_EQ(counter_value(&_profile, "SkippedLines"), 1);
    EXPECT_EQ(counter_value(&_profile, "CellsDeserialized"), 6);
}

// A row cap can change between calls. It must override the runtime default without dropping
// records, and a zero prediction must still make progress.
TEST_F(CsvV2ReaderTest, HonorsChangingBatchSizeAndZeroPrediction) {
    std::ofstream output(_file_path, std::ios::binary);
    output << "id,name,score\n";
    for (int i = 1; i <= 7; ++i) {
        output << i << ",name,10\n";
    }
    output.close();
    _state._batch_size = 2;
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    reader->set_batch_size(1);
    auto request = std::make_shared<FileScanRequest>();
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(1));
    ASSERT_TRUE(reader->open(request).ok());
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    int next_id = 1;
    const std::vector<size_t> caps {1, 3, 0, 2, 2};
    const std::vector<size_t> expected {1, 3, 1, 2, 0};
    for (size_t i = 0; i < caps.size(); ++i) {
        reader->set_batch_size(caps[i]);
        auto block = make_block(schema, {0, 1});
        size_t rows = 0;
        bool eof = false;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        ASSERT_EQ(rows, expected[i]);
        EXPECT_EQ(eof, rows == 0);
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, row), next_id++);
            EXPECT_EQ(nullable_string_at(*block.get_by_position(1).column, row), "name");
        }
        EXPECT_EQ(counter_value(&_profile, "CellsDeserialized"), (next_id - 1) * 2);
        EXPECT_EQ(counter_value(&_profile, "RawLinesRead"), next_id - 1);
    }
    EXPECT_EQ(next_id, 8);
    EXPECT_EQ(counter_value(&_profile, "SkippedLines"), 1);
}

TEST_F(CsvV2ReaderTest, BatchPredictionStillHonorsByteBudget) {
    const bool saved = config::enable_adaptive_batch_size;
    Defer restore {[&] { config::enable_adaptive_batch_size = saved; }};
    config::enable_adaptive_batch_size = true;
    _state._query_options.__set_preferred_block_size_bytes(1);
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    reader->set_batch_size(100);
    auto request = std::make_shared<FileScanRequest>();
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    for (const std::string expected : {"alice", "bob"}) {
        auto block = make_block(schema, {1});
        size_t rows = 0;
        bool eof = false;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        ASSERT_EQ(rows, 1);
        EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), expected);
    }
}

// Sampling spans batch boundaries; exact work counts are published before every return.
TEST_F(CsvV2ReaderTest, ProfileSamplingSpansBatchesWithExactCounts) {
    std::ofstream output(_file_path, std::ios::binary);
    for (int i = 0; i < 2050; ++i) {
        output << "1,name,10\n";
    }
    output.close();
    _params.file_attributes.__set_header_type("");
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    reader->set_batch_size(300);
    auto request = std::make_shared<FileScanRequest>();
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    size_t total = 0;
    bool eof = false;
    while (!eof) {
        auto block = make_block(schema, {1});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        total += rows;
        EXPECT_EQ(counter_value(&_profile, "RawLinesRead"), total);
        EXPECT_EQ(counter_value(&_profile, "CellsDeserialized"), total);
    }
    EXPECT_EQ(total, 2050);
    EXPECT_EQ(counter_value(&_profile, "SampledReadLineCalls"), 3);
    EXPECT_EQ(counter_value(&_profile, "SampledParsedRows"), 3);
}

// The single-byte split and direct string insertion must preserve NULL, quoted literals,
// escaping, trailing empty fields, missing fields and reordered projections across batches.
TEST_F(CsvV2ReaderTest, StringFastPathPreservesValuesForSingleAndMultiByteSeparators) {
    auto type = make_nullable(std::make_shared<DataTypeString>());
    std::vector<SlotDescriptor*> slots;
    for (int i = 0; i < 3; ++i) {
        slots.push_back(make_test_slot(&_pool, i, i, type, "s" + std::to_string(i)));
    }
    for (const std::string separator : {"|", "||"}) {
        for (bool empty_as_null : {false, true}) {
            SCOPED_TRACE(testing::Message() << separator << ", empty_as_null=" << empty_as_null);
            std::ofstream output(_file_path, std::ios::binary);
            output << "NULL" << separator << "\"NULL\"" << separator << "\n";
            output << "left" << separator << "a\\b" << separator << "tail\n";
            output << "only\n";
            output.close();
            auto params = _params;
            params.file_attributes.__set_header_type("");
            params.file_attributes.__set_trim_double_quotes(true);
            params.file_attributes.text_params.__set_column_separator(separator);
            params.file_attributes.text_params.__set_null_format("NULL");
            params.file_attributes.text_params.__set_empty_field_as_null(empty_as_null);
            params.file_attributes.text_params.__set_escape('\\');
            auto reader = create_reader(_file_path, &params, slots, &_state, &_profile);
            reader->set_batch_size(1);
            auto request = std::make_shared<FileScanRequest>();
            request->local_positions.emplace(LocalColumnId(2), LocalIndex(0));
            request->local_positions.emplace(LocalColumnId(0), LocalIndex(1));
            request->local_positions.emplace(LocalColumnId(1), LocalIndex(2));
            ASSERT_TRUE(reader->open(request).ok());
            std::vector<ColumnDefinition> schema;
            ASSERT_TRUE(reader->get_schema(&schema).ok());
            for (int row = 0; row < 3; ++row) {
                auto block = make_block(schema, {2, 0, 1});
                // Hold shared columns while the reader acquires its mutable batch.
                auto shared = block.get_by_position(0).column;
                size_t rows = 0;
                bool eof = false;
                ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
                ASSERT_EQ(rows, 1);
                EXPECT_EQ(shared->size(), 0);
                const auto& c0 = *block.get_by_position(0).column;
                const auto& c1 = *block.get_by_position(1).column;
                const auto& c2 = *block.get_by_position(2).column;
                if (row == 0) {
                    EXPECT_EQ(is_null_at(c0, 0), empty_as_null);
                    EXPECT_EQ(nullable_string_at(c0, 0), "");
                    EXPECT_TRUE(is_null_at(c1, 0));
                    EXPECT_FALSE(is_null_at(c2, 0));
                    EXPECT_EQ(nullable_string_at(c2, 0), "NULL");
                } else if (row == 1) {
                    EXPECT_EQ(nullable_string_at(c0, 0), "tail");
                    EXPECT_EQ(nullable_string_at(c1, 0), "left");
                    EXPECT_EQ(nullable_string_at(c2, 0), "ab");
                } else {
                    EXPECT_TRUE(is_null_at(c0, 0));
                    EXPECT_EQ(nullable_string_at(c1, 0), "only");
                    EXPECT_TRUE(is_null_at(c2, 0));
                }
            }
        }
    }
}

// Scenario: CSV has no embedded nested schema, but TableColumnMapper still needs semantic children
// for complex table columns. The reader synthesizes ARRAY/MAP/STRUCT children from the slot type
// while keeping the top-level local id as the CSV field ordinal from column_idxs.
TEST_F(CsvV2ReaderTest, SchemaSynthesizesComplexChildrenForColumnMapper) {
    _params.__set_column_idxs({4, 7, 9});
    auto slots = build_nested_complex_slots(&_pool);
    auto reader = create_reader(_file_path, &_params, slots, &_state, &_profile);

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);

    EXPECT_EQ(schema[1].name, "xs");
    EXPECT_EQ(schema[1].local_id, 7);
    ASSERT_EQ(schema[1].children.size(), 1);
    EXPECT_EQ(schema[1].children[0].name, "element");
    EXPECT_EQ(schema[1].children[0].local_id, 0);
    ASSERT_EQ(schema[1].children[0].children.size(), 2);
    EXPECT_EQ(schema[1].children[0].children[0].name, "a");
    EXPECT_EQ(schema[1].children[0].children[0].local_id, 0);
    EXPECT_EQ(schema[1].children[0].children[1].name, "b");
    EXPECT_EQ(schema[1].children[0].children[1].local_id, 1);

    EXPECT_EQ(schema[2].name, "kv");
    EXPECT_EQ(schema[2].local_id, 9);
    ASSERT_EQ(schema[2].children.size(), 2);
    EXPECT_EQ(schema[2].children[0].name, "key");
    EXPECT_EQ(schema[2].children[0].local_id, 0);
    EXPECT_EQ(schema[2].children[1].name, "value");
    EXPECT_EQ(schema[2].children[1].local_id, 1);
    ASSERT_EQ(schema[2].children[1].children.size(), 2);
    EXPECT_EQ(schema[2].children[1].children[0].name, "a");
    EXPECT_EQ(schema[2].children[1].children[1].name, "b");
}

TEST_F(CsvV2ReaderTest, LoadRejectsWrongFieldCountsWithoutShiftingFollowingRows) {
    std::ofstream output(_file_path, std::ios::binary);
    output << "id,name,score\n1,alice,10\n2,bob\n3,carol,30,extra\n4,dave,40\n";
    output.close();
    _params.__set_num_of_columns_from_file(3);
    auto io_ctx = std::make_shared<io::IOContext>();
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile, 0, -1,
                                TFileCompressType::UNKNOWN, io_ctx);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<FileScanRequest>();
    request->reject_invalid_rows = true;
    for (int i = 0; i < 3; ++i) {
        request->local_positions.emplace(LocalColumnId(i), LocalIndex(i));
    }
    ASSERT_TRUE(reader->open(request).ok());
    auto block = make_block(schema, {0, 1, 2});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 2);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 1), 4);
    EXPECT_EQ(io_ctx->load_filtered_rows, 2);
    EXPECT_EQ(counter_value(&_profile, "RawLinesRead"), 4);
    EXPECT_EQ(counter_value(&_profile, "CellsDeserialized"), 6);
}

// Scenario: CSV v2 honors FileScanRequest local positions, so TableReader can request a subset of
// CSV fields in an order different from the physical CSV field order.
TEST_F(CsvV2ReaderTest, ReadsRequestedColumnsInFileLocalBlockOrder) {
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(1)),
                                      LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(1));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {1, 0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 2);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), "alice");
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 1), "bob");
    EXPECT_EQ(nullable_int_at(*block.get_by_position(1).column, 0), 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(1).column, 1), 2);
}

// Scenario: CSV v2 defaults to the same strict UTF-8 validation as the old query reader. Invalid
// bytes should fail fast unless the scan params explicitly disable text UTF-8 validation.
TEST_F(CsvV2ReaderTest, InvalidUtf8FailsWhenValidationEnabled) {
    const auto invalid_path = (_test_dir / "invalid_utf8.csv").string();
    std::ofstream output(invalid_path, std::ios::binary);
    output << "id,name,score\n";
    output << "1,";
    output.write("\xff", 1);
    output << ",10\n";
    output.close();

    auto reader = create_reader(invalid_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(1))};
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {1});
    size_t rows = 0;
    bool eof = false;
    const auto status = reader->get_block(&block, &rows, &eof);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(counter_value(&_profile, "RawLinesRead"), 1);
    EXPECT_EQ(counter_value(&_profile, "SkippedLines"), 1);
    EXPECT_EQ(counter_value(&_profile, "CellsDeserialized"), 0);
    EXPECT_TRUE(status.to_string().find("Only support csv data in utf8 codec") != std::string::npos)
            << status;
}

// Scenario: external CSV scans can opt out of UTF-8 validation through
// `enable_text_validate_utf8=false`. In that mode the reader preserves the original bytes instead
// of rejecting the row.
TEST_F(CsvV2ReaderTest, DisableTextValidateUtf8ReadsRawBytes) {
    const auto invalid_path = (_test_dir / "invalid_utf8_disabled.csv").string();
    std::ofstream output(invalid_path, std::ios::binary);
    output << "id,name,score\n";
    output << "1,";
    output.write("\xff", 1);
    output << ",10\n";
    output.close();

    _params.file_attributes.__set_enable_text_validate_utf8(false);
    auto reader = create_reader(invalid_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(1))};
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {1});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), std::string("\xff", 1));
}

// Scenario: file TVF can keep the logical CSV format as FORMAT_CSV_PLAIN and put the actual gzip
// compression on the scan range. CSV v2 must honor that range-level compression before validating
// UTF-8; otherwise the gzip bytes are misread as CSV text.
TEST_F(CsvV2ReaderTest, RangeCompressTypeGzipDecompressesPlainCsvFormat) {
    const auto gz_path = (_test_dir / "reader.csv.gz").string();
    static constexpr unsigned char gzipped_csv[] = {
            0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xcb, 0x4c,
            0xd1, 0xc9, 0x4b, 0xcc, 0x4d, 0xd5, 0x29, 0x4e, 0xce, 0x2f, 0x4a, 0xe5,
            0x32, 0xd4, 0x49, 0xcc, 0xc9, 0x4c, 0x4e, 0xd5, 0x31, 0x34, 0xe0, 0x02,
            0x00, 0x0b, 0xed, 0x5c, 0xa2, 0x19, 0x00, 0x00, 0x00};
    std::ofstream output(gz_path, std::ios::binary);
    output.write(reinterpret_cast<const char*>(gzipped_csv), sizeof(gzipped_csv));
    output.close();

    _params.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
    _params.__isset.compress_type = false;
    auto reader = create_reader(gz_path, &_params, _slots, &_state, &_profile, 0, -1,
                                TFileCompressType::GZ);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0)),
                                      LocalColumnIndex::top_level(LocalColumnId(1))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(1));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0, 1});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 1);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(1).column, 0), "alice");
}

// Scenario: FE column_idxs define the CSV field ordinal for each physical file slot. The mapping
// can be non-identity when FE reorders projected file slots, so the reader must use the local id
// from FileScanRequest instead of the slot vector position.
TEST_F(CsvV2ReaderTest, ColumnIdxsMapSlotsToCsvOrdinals) {
    const auto remap_path = (_test_dir / "remapped.csv").string();
    std::ofstream output(remap_path, std::ios::binary);
    output << "name,score,id\n";
    output << "alice,10,1\n";
    output.close();

    _params.__set_column_idxs({2, 0, 1});
    auto reader = create_reader(remap_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);
    EXPECT_EQ(schema[0].name, "id");
    EXPECT_EQ(schema[0].local_id, 2);
    EXPECT_EQ(schema[1].name, "name");
    EXPECT_EQ(schema[1].local_id, 0);

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(2)),
                                      LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(2), LocalIndex(0));
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(1));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {2, 0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 1);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(1).column, 0), "alice");
}

// Scenario: CSV stores one complex column as one text field, so v2 must read the whole struct
// field before evaluating a file-local predicate on one child. This covers `SELECT s.a WHERE
// s.b > 10` style scans after CsvReader's MaterializedColumnMapper has requested the full
// top-level `s`.
TEST_F(CsvV2ReaderTest, FullStructColumnSupportsChildConjunctFiltering) {
    const auto complex_path = (_test_dir / "complex.csv").string();
    std::ofstream output(complex_path, std::ios::binary);
    output << "id|s|score\n";
    output << "1|{\"a\": 11, \"b\": 5}|10\n";
    output << "2|{\"a\": 22, \"b\": 20}|20\n";
    output.close();

    _params.file_attributes.text_params.__set_column_separator("|");
    _params.__set_column_idxs({0, 1, 2});
    auto slots = build_struct_slots(&_pool);
    auto reader = create_reader(complex_path, &_params, slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(1))};
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
    request->conjuncts = {prepared_conjunct(
            &_state, std::make_shared<StructIntChildGreaterThanExpr>(
                             /*block_position=*/0, /*child_index=*/1, /*value=*/10))};
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {1});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_struct_int_child_at(*block.get_by_position(0).column, 0, 0), 22);
    EXPECT_EQ(nullable_struct_int_child_at(*block.get_by_position(0).column, 1, 0), 20);
}

// Scenario: a table-level scan can need only partition/default columns, leaving the CSV
// FileScanRequest with no file-local columns. The reader must still report the number of rows read.
TEST_F(CsvV2ReaderTest, EmptyFileLocalProjectionStillReportsRows) {
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    auto request = std::make_shared<FileScanRequest>();
    ASSERT_TRUE(reader->open(request).ok());

    Block block;
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_EQ(rows, 2);
    EXPECT_FALSE(eof);
}

// Scenario: stream-load/http_stream inputs do not have a known split size or file size. A first
// split must still read until EOF instead of rejecting the request before opening the stream.
TEST_F(CsvV2ReaderTest, UnknownFirstSplitSizeReadsUntilEof) {
    auto reader = create_unknown_size_reader(_file_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0)),
                                      LocalColumnIndex::top_level(LocalColumnId(1))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(1));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0, 1});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 2);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 1);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(1).column, 1), "bob");
}

// Scenario: stream load/http_stream CSV input is not backed by a filesystem. If TableReader fails
// to preserve the stream load id, the v2 reader should report that directly instead of calling the
// generic FileFactory path and returning "unsupported file reader type: 2".
TEST_F(CsvV2ReaderTest, StreamInputRequiresLoadIdBeforeOpeningPipe) {
    _params.__set_file_type(TFileType::FILE_STREAM);
    auto reader = create_unknown_size_reader(_file_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    const auto status = reader->open(request);

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("stream reader requires load id"), std::string::npos)
            << status;
}

TEST_F(CsvV2ReaderTest, ClosePreservesSharedStreamPipe) {
    _params.__set_file_type(TFileType::FILE_STREAM);
    auto reader = create_unknown_size_reader(_file_path, &_params, _slots, &_state, &_profile);
    auto pipe = std::make_shared<io::StreamLoadPipe>();
    reader->_file_reader = pipe;
    ASSERT_TRUE(reader->close().ok());
    EXPECT_FALSE(pipe->closed());
    reader.reset();
    EXPECT_FALSE(pipe->closed());
}

// Scenario: CSV has no footer row count, so v2 COUNT pushdown scans the split and returns the
// counted row count through FileAggregateResult.
TEST_F(CsvV2ReaderTest, CountAggregateScansRows) {
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    auto request = std::make_shared<FileScanRequest>();
    ASSERT_TRUE(reader->open(request).ok());

    FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::type::COUNT;
    FileAggregateResult aggregate_result;
    ASSERT_TRUE(reader->get_aggregate_result(aggregate_request, &aggregate_result).ok());
    EXPECT_EQ(aggregate_result.count, 2);
    EXPECT_EQ(counter_value(&_profile, "RawLinesRead"), 2);
    EXPECT_EQ(counter_value(&_profile, "SkippedLines"), 1);
    EXPECT_EQ(counter_value(&_profile, "CellsDeserialized"), 0);
    EXPECT_EQ(counter_value(&_profile, "SampledParsedRows"), 0);
}

TEST_F(CsvV2ReaderTest, CountNullableColumnFallsBackToRowMaterialization) {
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    auto request = std::make_shared<FileScanRequest>();
    ASSERT_TRUE(reader->open(request).ok());

    FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::type::COUNT;
    aggregate_request.columns.push_back(
            {.projection = LocalColumnIndex::top_level(LocalColumnId(1))});
    FileAggregateResult aggregate_result;
    EXPECT_TRUE(reader->get_aggregate_result(aggregate_request, &aggregate_result)
                        .is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
}

// Scenario: CSV v2 parses enclosed fields itself instead of delegating to the old CsvReader. A
// separator inside an enclosed string must stay inside the same CSV field.
TEST_F(CsvV2ReaderTest, EnclosedFieldKeepsSeparatorInsideStringValue) {
    const auto quoted_path = (_test_dir / "quoted.csv").string();
    std::ofstream output(quoted_path, std::ios::binary);
    output << "id,name,score\n";
    output << "1,\"alice,team\",10\n";
    output.close();

    _params.file_attributes.text_params.__set_enclose('"');
    _params.file_attributes.text_params.__set_escape('\\');
    auto reader = create_reader(quoted_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(1))};
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {1});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), "alice,team");
}

// Bare quotes and escapes outside enclosed fields stay in NORMAL state in
// EncloseCsvLineReaderCtx. Field slicing must reuse those exact separator positions.
TEST_F(CsvV2ReaderTest, EncloseFieldSplittingMatchesLineReaderStateMachine) {
    const auto quoted_path = (_test_dir / "enclose_state.csv").string();
    std::ofstream output(quoted_path, std::ios::binary);
    output << "id,name,score\n";
    output << "1,ab\"cd,20\n";
    output << "2,C:\\dir\\,30,40\n";
    output.close();

    _params.file_attributes.text_params.__set_enclose('"');
    _params.file_attributes.text_params.__set_escape('\\');
    auto reader = create_reader(quoted_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(1)),
                                      LocalColumnIndex::top_level(LocalColumnId(2))};
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
    request->local_positions.emplace(LocalColumnId(2), LocalIndex(1));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {1, 2});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 2);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), "ab\"cd");
    EXPECT_EQ(nullable_int_at(*block.get_by_position(1).column, 0), 20);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(1).column, 1), 30);
}

// OpenCSV permits escape and enclose to use the same character. The shared line-reader state
// machine handles doubled quotes without treating every quote as a generic escape.
TEST_F(CsvV2ReaderTest, MatchingEscapeAndEncloseStillSplitQuotedFields) {
    const auto quoted_path = (_test_dir / "matching_escape_enclose.csv").string();
    std::ofstream output(quoted_path, std::ios::binary);
    output << "id,name,score\n";
    output << "\"1\",\"alice\",\"10\"\n";
    output.close();

    _params.file_attributes.text_params.__set_enclose('"');
    _params.file_attributes.text_params.__set_escape('"');
    auto reader = create_reader(quoted_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0)),
                                      LocalColumnIndex::top_level(LocalColumnId(1)),
                                      LocalColumnIndex::top_level(LocalColumnId(2))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(1));
    request->local_positions.emplace(LocalColumnId(2), LocalIndex(2));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0, 1, 2});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 1);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(1).column, 0), "alice");
    EXPECT_EQ(nullable_int_at(*block.get_by_position(2).column, 0), 10);
}

// OpenCSV custom quote characters only remove that configured enclosure. Literal double quotes in
// a field must remain when the configured enclosure is a single quote.
TEST_F(CsvV2ReaderTest, CustomEnclosePreservesLiteralDoubleQuotes) {
    const auto quoted_path = (_test_dir / "custom_enclose.csv").string();
    std::ofstream output(quoted_path, std::ios::binary);
    output << R"("Project Manager")" << '\n';
    output.close();

    auto value_slot = make_test_slot(&_pool, 10, 0,
                                     make_nullable(std::make_shared<DataTypeString>()), "value");
    std::vector<SlotDescriptor*> slots {value_slot};
    _params.__set_column_idxs({0});
    _params.file_attributes.__isset.header_type = false;
    _params.file_attributes.text_params.__set_column_separator("\t");
    _params.file_attributes.text_params.__set_enclose('\'');
    _params.file_attributes.text_params.__set_escape('|');
    auto reader = create_reader(quoted_path, &_params, slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), R"("Project Manager")");
}

// A column separator that overlaps the line delimiter must not be recorded because CsvReader
// splits a Slice that excludes the line delimiter bytes.
TEST_F(CsvV2ReaderTest, ColumnSeparatorOverlappingLineDelimiterStaysOutsideReturnedLine) {
    const auto overlap_path = (_test_dir / "overlapping_delimiters.csv").string();
    std::ofstream output(overlap_path, std::ios::binary);
    output << "value|\n";
    output.close();

    auto value_slot = make_test_slot(&_pool, 10, 0,
                                     make_nullable(std::make_shared<DataTypeString>()), "value");
    std::vector<SlotDescriptor*> slots {value_slot};
    _params.__set_column_idxs({0});
    _params.file_attributes.__isset.header_type = false;
    _params.file_attributes.text_params.__set_column_separator("|\n");
    _params.file_attributes.text_params.__set_enclose('"');
    _params.file_attributes.text_params.__set_escape('\\');
    auto reader = create_reader(overlap_path, &_params, slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), "value|");
}

// When a logical row is refilled, partial-separator backtracking must not cross the end of a
// separator that was already accepted from the previous buffer contents.
TEST_F(CsvV2ReaderTest, MultiCharacterSeparatorAcrossOutputBufferRefill) {
    const auto refill_path = (_test_dir / "separator_refill.csv").string();
    std::ofstream output(refill_path, std::ios::binary);
    output << std::string(28, 'a') << "|||||b\n";
    output.close();

    auto first_slot = make_test_slot(&_pool, 10, 0,
                                     make_nullable(std::make_shared<DataTypeString>()), "first");
    auto second_slot = make_test_slot(&_pool, 11, 1,
                                      make_nullable(std::make_shared<DataTypeString>()), "second");
    std::vector<SlotDescriptor*> slots {first_slot, second_slot};
    _params.__set_column_idxs({0, 1});
    _params.file_attributes.__isset.header_type = false;
    _params.file_attributes.text_params.__set_column_separator("|||");
    _params.file_attributes.text_params.__set_enclose('"');
    _params.file_attributes.text_params.__set_escape('\\');

    const bool enable_debug_points = config::enable_debug_points;
    config::enable_debug_points = true;
    DebugPoints::instance()->add_with_params("NewPlainTextLineReader.shrink_output_buf",
                                             {{"output_buf_size", "32"}});
    Defer restore_debug_point([enable_debug_points]() {
        DebugPoints::instance()->remove("NewPlainTextLineReader.shrink_output_buf");
        config::enable_debug_points = enable_debug_points;
    });

    auto reader = create_reader(refill_path, &_params, slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0)),
                                      LocalColumnIndex::top_level(LocalColumnId(1))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(1));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0, 1});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), std::string(28, 'a'));
    EXPECT_EQ(nullable_string_at(*block.get_by_position(1).column, 0), "||b");
}

// Scenario: when the CSV row has fewer fields than the FE-provided file slot list, v2 fills the
// missing requested field with NULL instead of failing or shifting later columns.
TEST_F(CsvV2ReaderTest, MissingRequestedFieldUsesNullFormat) {
    const auto missing_path = (_test_dir / "missing.csv").string();
    std::ofstream output(missing_path, std::ios::binary);
    output << "id,name,score\n";
    output << "1,alice\n";
    output.close();

    auto reader = create_reader(missing_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(2))};
    request->local_positions.emplace(LocalColumnId(2), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {2});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_TRUE(is_null_at(*block.get_by_position(0).column, 0));
}

// Scenario: the first line may contain UTF-8 BOM and CSV_WITH_NAMES_AND_TYPES has two header
// records. Both must be skipped before materializing the first data row.
TEST_F(CsvV2ReaderTest, HeaderNamesAndTypesSkipsTwoLinesAndBom) {
    const auto header_path = (_test_dir / "header_names_types.csv").string();
    std::ofstream output(header_path, std::ios::binary);
    output.write("\xEF\xBB\xBF", 3);
    output << "id,name,score\n";
    output << "INT,STRING,INT\n";
    output << "7,carol,70\n";
    output.close();

    _params.file_attributes.__set_header_type(BeConsts::CSV_WITH_NAMES_AND_TYPES);
    auto reader = create_reader(header_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 7);
}

// Scenario: when the first returned data line starts with UTF-8 BOM, CSV v2 strips the BOM before
// passing the cell to the serde. This matters for headerless files whose first column is numeric.
TEST_F(CsvV2ReaderTest, BomIsRemovedFromFirstDataLineWithoutHeader) {
    const auto bom_path = (_test_dir / "bom_data.csv").string();
    std::ofstream output(bom_path, std::ios::binary);
    output.write("\xEF\xBB\xBF", 3);
    output << "5,bom,50\n";
    output.close();

    _params.file_attributes.__isset.header_type = false;
    _params.file_attributes.text_params.__set_enclose('"');
    _params.file_attributes.text_params.__set_escape('\\');
    auto reader = create_reader(bom_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 5);
}

// The enclosed line reader sees bytes before DelimitedTextReader removes the BOM. It must skip the
// BOM structurally so the quote immediately after it still starts an enclosed first field and the
// comma inside that field is not recorded as a column separator.
TEST_F(CsvV2ReaderTest, BomBeforeQuotedFirstFieldWithComma) {
    const auto bom_path = (_test_dir / "bom_quoted_comma.csv").string();
    std::ofstream output(bom_path, std::ios::binary);
    output.write("\xEF\xBB\xBF", 3);
    output << "\"alice,team\",20\n";
    output.close();

    auto string_slot = make_test_slot(&_pool, 10, 0,
                                      make_nullable(std::make_shared<DataTypeString>()), "name");
    auto score_slot = make_test_slot(&_pool, 11, 1,
                                     make_nullable(std::make_shared<DataTypeInt32>()), "score");
    std::vector<SlotDescriptor*> slots {string_slot, score_slot};
    _params.__set_column_idxs({0, 1});
    _params.file_attributes.__isset.header_type = false;
    _params.file_attributes.text_params.__set_enclose('"');
    _params.file_attributes.text_params.__set_escape('\\');
    auto reader = create_reader(bom_path, &_params, slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0)),
                                      LocalColumnIndex::top_level(LocalColumnId(1))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(1));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0, 1});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), "alice,team");
    EXPECT_EQ(nullable_int_at(*block.get_by_position(1).column, 0), 20);
}

// The same ordering matters before logical-row detection: a newline inside the first enclosed
// field belongs to the field, even when the opening quote follows a file BOM.
TEST_F(CsvV2ReaderTest, BomBeforeQuotedFirstFieldWithNewline) {
    const auto bom_path = (_test_dir / "bom_quoted_newline.csv").string();
    std::ofstream output(bom_path, std::ios::binary);
    output.write("\xEF\xBB\xBF", 3);
    output << "\"alice\nteam\",20\n";
    output.close();

    auto string_slot = make_test_slot(&_pool, 10, 0,
                                      make_nullable(std::make_shared<DataTypeString>()), "name");
    auto score_slot = make_test_slot(&_pool, 11, 1,
                                     make_nullable(std::make_shared<DataTypeInt32>()), "score");
    std::vector<SlotDescriptor*> slots {string_slot, score_slot};
    _params.__set_column_idxs({0, 1});
    _params.file_attributes.__isset.header_type = false;
    _params.file_attributes.text_params.__set_enclose('"');
    _params.file_attributes.text_params.__set_escape('\\');
    auto reader = create_reader(bom_path, &_params, slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0)),
                                      LocalColumnIndex::top_level(LocalColumnId(1))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(1));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0, 1});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), "alice\nteam");
    EXPECT_EQ(nullable_int_at(*block.get_by_position(1).column, 0), 20);
}

// Scenario: when FE does not set header_type, CSV v2 must honor skip_lines exactly as the old
// reader does.
TEST_F(CsvV2ReaderTest, SkipLinesUsedWhenHeaderTypeUnset) {
    const auto skip_path = (_test_dir / "skip_lines.csv").string();
    std::ofstream output(skip_path, std::ios::binary);
    output << "skip me\n";
    output << "skip me too\n";
    output << "3,dan,30\n";
    output.close();

    _params.file_attributes.__isset.header_type = false;
    _params.file_attributes.__set_skip_lines(2);
    auto reader = create_reader(skip_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 3);
}

// Scenario: empty physical lines are skipped by default, but read_csv_empty_line_as_null turns one
// empty line into one all-null logical row.
TEST_F(CsvV2ReaderTest, EmptyLineAsNullWhenQueryOptionEnabled) {
    const auto empty_line_path = (_test_dir / "empty_line.csv").string();
    std::ofstream output(empty_line_path, std::ios::binary);
    output << "id,name,score\n";
    output << "\n";
    output << "4,erin,40\n";
    output.close();

    _state._query_options.__set_read_csv_empty_line_as_null(true);
    auto reader = create_reader(empty_line_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 2);
    EXPECT_TRUE(is_null_at(*block.get_by_position(0).column, 0));
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 1), 4);
}

// Scenario: FE-provided CSV text parameters define NULL semantics. Explicit null_format and
// empty_field_as_null should both produce nullable values without throwing serde errors.
TEST_F(CsvV2ReaderTest, NullFormatAndEmptyFieldAsNullProduceNullableValues) {
    const auto null_path = (_test_dir / "null_format.csv").string();
    std::ofstream output(null_path, std::ios::binary);
    output << "id,name,score\n";
    output << "1,NULL,\n";
    output.close();

    _params.file_attributes.text_params.__set_null_format("NULL");
    _params.file_attributes.text_params.__set_empty_field_as_null(true);
    auto reader = create_reader(null_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(1)),
                                      LocalColumnIndex::top_level(LocalColumnId(2))};
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
    request->local_positions.emplace(LocalColumnId(2), LocalIndex(1));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {1, 2});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_TRUE(is_null_at(*block.get_by_position(0).column, 0));
    EXPECT_TRUE(is_null_at(*block.get_by_position(1).column, 0));
}

// trim_double_quotes preserves quoted null literals as strings while an unquoted marker remains
// NULL, matching the V1 CSV reader's converted-from-string behavior.
TEST_F(CsvV2ReaderTest, QuotedNullFormatIsAStringLiteral) {
    const auto null_path = (_test_dir / "quoted_null_format.csv").string();
    std::ofstream output(null_path, std::ios::binary);
    output << "id,name,score\n";
    output << "1,\"\\N\",10\n";
    output << "2,\\N,20\n";
    output.close();

    _params.file_attributes.__set_trim_double_quotes(true);
    _params.file_attributes.text_params.__set_null_format("\\N");
    auto reader = create_reader(null_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(1))};
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {1});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 2);
    EXPECT_FALSE(is_null_at(*block.get_by_position(0).column, 0));
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), "\\N");
    EXPECT_TRUE(is_null_at(*block.get_by_position(0).column, 1));
}

// Scenario: OpenCSV keeps an empty field as an empty string when empty_field_as_null is false,
// even if FE passes an empty null_format. This differs from Hive text serde, where an empty
// serialization.null.format is a real NULL marker.
TEST_F(CsvV2ReaderTest, EmptyNullFormatKeepsCsvEmptyFieldAsEmptyString) {
    const auto null_path = (_test_dir / "empty_null_format.csv").string();
    std::ofstream output(null_path, std::ios::binary);
    output << "id,name,score\n";
    output << "1,alice,10\n";
    output << "2,,20\n";
    output << "3,NULL,30\n";
    output.close();

    _params.file_attributes.text_params.__set_null_format("");
    _params.file_attributes.text_params.__set_empty_field_as_null(false);
    auto reader = create_reader(null_path, &_params, _slots, &_state, &_profile);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(1))};
    request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {1});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 3);
    EXPECT_FALSE(is_null_at(*block.get_by_position(0).column, 0));
    EXPECT_FALSE(is_null_at(*block.get_by_position(0).column, 1));
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 1), "");
    EXPECT_FALSE(is_null_at(*block.get_by_position(0).column, 2));
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 2), "NULL");
}

// Scenario: a non-first split starts inside a record. CSV v2 pre-reads enough delimiter bytes and
// skips the partial first line so the split begins at the next complete row.
TEST_F(CsvV2ReaderTest, NonFirstSplitSkipsPartialFirstRecord) {
    const auto split_path = (_test_dir / "split.csv").string();
    std::ofstream output(split_path, std::ios::binary);
    output << "1,skip,10\n";
    output << "2,bob,20\n";
    output.close();

    _params.file_attributes.__isset.header_type = false;
    auto reader = create_reader(split_path, &_params, _slots, &_state, &_profile,
                                /*range_start_offset=*/3);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 2);
}

// Scenario: compressed CSV cannot be split at arbitrary byte offsets because the decompressor needs
// the stream from the beginning. V2 should reject such a split before constructing the line reader.
TEST_F(CsvV2ReaderTest, NonFirstCompressedSplitReturnsError) {
    _params.__set_format_type(TFileFormatType::FORMAT_CSV_GZ);
    _params.file_attributes.__isset.header_type = false;
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile,
                                /*range_start_offset=*/1);

    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
    EXPECT_FALSE(reader->open(request).ok());
}

// Scenario: FileScanRequest is a TableReader-to-FileReader contract. Unknown CSV ordinals,
// out-of-range block positions, and sparse block-position maps must fail during reader open.
TEST_F(CsvV2ReaderTest, InvalidScanRequestReturnsError) {
    {
        auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
        auto request = std::make_shared<FileScanRequest>();
        request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(99))};
        request->local_positions.emplace(LocalColumnId(99), LocalIndex(0));
        EXPECT_FALSE(reader->open(request).ok());
    }
    {
        auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
        auto request = std::make_shared<FileScanRequest>();
        request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
        request->local_positions.emplace(LocalColumnId(0), LocalIndex(2));
        EXPECT_FALSE(reader->open(request).ok());
    }
    {
        auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
        auto request = std::make_shared<FileScanRequest>();
        request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0)),
                                          LocalColumnIndex::top_level(LocalColumnId(1))};
        request->local_positions.emplace(LocalColumnId(0), LocalIndex(0));
        request->local_positions.emplace(LocalColumnId(1), LocalIndex(0));
        EXPECT_FALSE(reader->open(request).ok());
    }
}

// Scenario: CSV v2 can count rows by scanning, but it cannot answer min/max or mixed aggregate
// requests from metadata.
TEST_F(CsvV2ReaderTest, UnsupportedAggregateReturnsNotSupported) {
    auto reader = create_reader(_file_path, &_params, _slots, &_state, &_profile);
    auto request = std::make_shared<FileScanRequest>();
    ASSERT_TRUE(reader->open(request).ok());

    FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::type::MINMAX;
    FileAggregateResult aggregate_result;
    EXPECT_FALSE(reader->get_aggregate_result(aggregate_request, &aggregate_result).ok());
}

} // namespace
} // namespace doris::format::csv
