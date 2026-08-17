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

#include <arpa/inet.h>
#include <cctz/time_zone.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/NetworkTestService.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/parquet_types.h>
#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <thrift/Thrift.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <memory>
#include <new>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "core/data_type/define_primitive_type.h"
#include "core/string_ref.h"
#include "core/value/decimalv2_value.h"
#include "exprs/aggregate/aggregate_function.h"
#include "format/parquet/parquet_common.h"
#include "format/parquet/parquet_thrift_util.h"
#include "format/parquet/schema_desc.h"
#include "format/parquet/vparquet_column_chunk_reader.h"
#include "format/parquet/vparquet_file_metadata.h"
#include "format/parquet/vparquet_group_reader.h"
#include "gtest/gtest_pred_impl.h"
#include "information_schema/schema_scanner.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/slice.h"
#include "util/thrift_container_size.h"
#include "util/thrift_server.h"
#include "util/thrift_util.h"
#include "util/timezone_utils.h"

namespace doris {
namespace {

struct LargeDecodedElement {
    std::array<uint8_t, 1024> bytes {};
};

struct ThriftContainerProbe {
    uint32_t read(apache::thrift::protocol::TProtocol* protocol) {
        apache::thrift::protocol::TType element_type;
        uint32_t size = 0;
        uint32_t consumed = protocol->readListBegin(element_type, size);
        reserve_thrift_container_memory(protocol, &elements, size);
        elements.resize(size);
        return consumed + protocol->readListEnd();
    }

    std::vector<LargeDecodedElement> elements;
};

struct ThriftStringProbe {
    uint32_t read(apache::thrift::protocol::TProtocol* protocol) {
        return protocol->readString(value);
    }

    std::string value;
};

struct ThriftSkipProbe {
    uint32_t read(apache::thrift::protocol::TProtocol* protocol) {
        return protocol->skip(apache::thrift::protocol::T_LIST);
    }
};

std::vector<uint8_t> thrift_string_bytes(bool compact, std::string_view value);

class ContextlessDeserializeService final : public doristest::NetworkTestServiceIf {
public:
    void Send(doristest::ThriftDataResult& result,
              const doristest::ThriftDataParams& params) override {
        contextless_before_deserialize = !pthread_context_ptr_init;
        auto bytes = thrift_string_bytes(/*compact=*/true, params.data);
        uint32_t length = bytes.size();
        ThriftStringProbe probe;
        const Status status = deserialize_thrift_msg(bytes.data(), &length, true, &probe);
        context_cleaned_after_deserialize = !pthread_context_ptr_init;
        if (!status.ok()) {
            throw apache::thrift::TException(status.to_string());
        }
        result.__set_bytes_received(static_cast<int64_t>(probe.value.size()));
    }

    std::atomic_bool contextless_before_deserialize = false;
    std::atomic_bool context_cleaned_after_deserialize = false;
};

int find_available_port() {
    const int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) {
        return -1;
    }
    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (bind(socket_fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
        close(socket_fd);
        return -1;
    }
    socklen_t address_size = sizeof(address);
    if (getsockname(socket_fd, reinterpret_cast<sockaddr*>(&address), &address_size) != 0) {
        close(socket_fd);
        return -1;
    }
    close(socket_fd);
    return ntohs(address.sin_port);
}

std::vector<uint8_t> thrift_list_bytes(bool compact, uint32_t count, size_t total_size) {
    std::vector<uint8_t> bytes;
    if (compact) {
        bytes = {0xfc, static_cast<uint8_t>(count)};
    } else {
        bytes = {static_cast<uint8_t>(apache::thrift::protocol::T_STRUCT),
                 static_cast<uint8_t>(count >> 24), static_cast<uint8_t>(count >> 16),
                 static_cast<uint8_t>(count >> 8), static_cast<uint8_t>(count)};
    }
    bytes.resize(std::max(bytes.size(), total_size));
    return bytes;
}

std::vector<uint8_t> thrift_string_bytes(bool compact, std::string_view value) {
    std::vector<uint8_t> bytes;
    if (compact) {
        bytes.push_back(static_cast<uint8_t>(value.size()));
    } else {
        const uint32_t size = value.size();
        bytes = {static_cast<uint8_t>(size >> 24), static_cast<uint8_t>(size >> 16),
                 static_cast<uint8_t>(size >> 8), static_cast<uint8_t>(size)};
    }
    bytes.insert(bytes.end(), value.begin(), value.end());
    return bytes;
}

} // namespace

class ParquetThriftReaderTest : public testing::Test {
public:
    ParquetThriftReaderTest() = default;
    void SetUp() override { TimezoneUtils::load_timezones_to_cache(); }
    void TearDown() override { TimezoneUtils::clear_timezone_caches(); }
};

TEST_F(ParquetThriftReaderTest, reject_compact_container_larger_than_input) {
    // A tiny payload must not make generated Thrift code resize a container to an
    // attacker-controlled length before the transport discovers that the bytes are absent.
    std::vector<uint8_t> compact_metadata {0x29, 0xfc, 0x88, 0x27, 0x00};
    uint32_t length = compact_metadata.size();
    tparquet::FileMetaData metadata;

    Status status = deserialize_thrift_msg(compact_metadata.data(), &length, true, &metadata);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(metadata.schema.empty());
}

TEST_F(ParquetThriftReaderTest, reject_decoded_container_that_exceeds_task_budget) {
    constexpr uint32_t element_count = 64;
    constexpr int64_t memory_limit = 4 * 1024;
    for (const bool compact : {true, false}) {
        auto bytes = thrift_list_bytes(compact, element_count, element_count + 8);
        uint32_t length = bytes.size();
        ThriftContainerProbe probe;
        auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                        "ThriftContainerProbe", memory_limit);
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);

        Status status = deserialize_thrift_msg(bytes.data(), &length, compact, &probe);

        EXPECT_FALSE(status.ok()) << "compact=" << compact;
        EXPECT_TRUE(probe.elements.empty()) << "compact=" << compact;
    }
}

TEST_F(ParquetThriftReaderTest, retain_decoded_container_charge_until_output_is_destroyed) {
    constexpr size_t element_count = 32;
    tparquet::PageLocation page_location;
    page_location.__set_offset(0);
    page_location.__set_compressed_page_size(1);
    page_location.__set_first_row_index(0);
    tparquet::OffsetIndex source;
    source.__set_page_locations(std::vector<tparquet::PageLocation>(element_count, page_location));
    std::vector<uint8_t> bytes;
    ThriftSerializer serializer(/*compact=*/true, 1024);
    ASSERT_TRUE(serializer.serialize(&source, &bytes).ok());

    const auto one_container_bytes =
            static_cast<int64_t>(element_count * sizeof(tparquet::PageLocation));
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                    "RetainedThriftContainers",
                                                    one_container_bytes * 3 / 2);
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);

    {
        tparquet::OffsetIndex first;
        uint32_t first_length = bytes.size();
        ASSERT_TRUE(deserialize_thrift_msg(bytes.data(), &first_length, true, &first).ok());
        ASSERT_EQ(first.page_locations.size(), element_count);

        tparquet::OffsetIndex second;
        uint32_t second_length = bytes.size();
        const Status second_status =
                deserialize_thrift_msg(bytes.data(), &second_length, true, &second);
        EXPECT_TRUE(second_status.is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) << second_status;
        EXPECT_TRUE(second.page_locations.empty());
    }

    // Destroying the retained output must return its charge so the same task can deserialize again.
    tparquet::OffsetIndex after_release;
    uint32_t after_release_length = bytes.size();
    EXPECT_TRUE(
            deserialize_thrift_msg(bytes.data(), &after_release_length, true, &after_release).ok());
}

TEST_F(ParquetThriftReaderTest, use_generated_target_type_for_container_budget) {
    constexpr int64_t memory_limit = 4 * 1024;
    // FileMetaData.schema is vector<SchemaElement>, but the forged compact wire tag advertises
    // bool elements. Generated readers must budget the actual target vector before resizing it.
    std::vector<uint8_t> compact_metadata {0x29, 0xf1, 0x40, 0x00};
    compact_metadata.resize(72);
    uint32_t length = compact_metadata.size();
    tparquet::FileMetaData metadata;
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                    "GeneratedThriftTargetType", memory_limit);
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);

    Status status = deserialize_thrift_msg(compact_metadata.data(), &length, true, &metadata);

    EXPECT_TRUE(status.is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) << status;
    EXPECT_TRUE(metadata.schema.empty());
}

TEST_F(ParquetThriftReaderTest, skip_unknown_container_without_phantom_reservation) {
    constexpr uint32_t element_count = 64;
    constexpr int64_t memory_limit = 4 * 1024;
    for (const bool compact : {true, false}) {
        auto bytes = thrift_list_bytes(compact, element_count, element_count + 8);
        uint32_t length = bytes.size();
        ThriftSkipProbe probe;
        auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                        "SkippedThriftContainer", memory_limit);
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);

        EXPECT_TRUE(deserialize_thrift_msg(bytes.data(), &length, compact, &probe).ok())
                << "compact=" << compact;
    }
}

TEST_F(ParquetThriftReaderTest, accept_small_message_from_large_readable_window) {
    constexpr int64_t memory_limit = 16 * 1024;
    for (const bool compact : {true, false}) {
        auto bytes = thrift_string_bytes(compact, "valid");
        bytes.resize(64 * 1024);
        uint32_t length = bytes.size();
        ThriftStringProbe probe;
        auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                        "ThriftReadableWindow", memory_limit);
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);

        EXPECT_TRUE(deserialize_thrift_msg(bytes.data(), &length, compact, &probe).ok())
                << "compact=" << compact;
        EXPECT_EQ(probe.value, "valid");
        EXPECT_LT(length, bytes.size());
    }
}

TEST_F(ParquetThriftReaderTest, deserialize_on_contextless_threaded_service_worker) {
    const int port = find_available_port();
    ASSERT_GT(port, 0);
    auto handler = std::make_shared<ContextlessDeserializeService>();
    auto processor = std::make_shared<doristest::NetworkTestServiceProcessor>(handler);
    ThriftServer server("contextless-deserialize-test", processor, port,
                        ThriftServer::DEFAULT_WORKER_THREADS, ThriftServer::THREADED);
    ASSERT_TRUE(server.start().ok());

    auto socket = std::make_shared<apache::thrift::transport::TSocket>("127.0.0.1", port);
    auto transport = std::make_shared<apache::thrift::transport::TBufferedTransport>(socket);
    auto protocol = std::make_shared<apache::thrift::protocol::TBinaryProtocol>(transport);
    doristest::NetworkTestServiceClient client(protocol);
    transport->open();
    doristest::ThriftDataParams params;
    params.__set_data("valid");
    doristest::ThriftDataResult result;
    client.Send(result, params);
    transport->close();

    EXPECT_EQ(result.bytes_received, params.data.size());
    EXPECT_TRUE(handler->contextless_before_deserialize.load());
    EXPECT_TRUE(handler->context_cleaned_after_deserialize.load());
}

TEST_F(ParquetThriftReaderTest, bound_strings_and_accept_valid_controls_for_both_protocols) {
    for (const bool compact : {true, false}) {
        auto malformed_container = thrift_list_bytes(compact, /*count=*/64, /*total_size=*/8);
        uint32_t malformed_container_length = malformed_container.size();
        ThriftContainerProbe malformed_container_probe;
        EXPECT_FALSE(deserialize_thrift_msg(malformed_container.data(), &malformed_container_length,
                                            compact, &malformed_container_probe)
                             .ok())
                << "compact=" << compact;
        EXPECT_TRUE(malformed_container_probe.elements.empty()) << "compact=" << compact;

        std::vector<uint8_t> malformed_string =
                compact ? std::vector<uint8_t> {0xff, 0xff, 0xff, 0xff, 0x07}
                        : std::vector<uint8_t> {0x7f, 0xff, 0xff, 0xff};
        uint32_t malformed_length = malformed_string.size();
        ThriftStringProbe malformed_probe;
        EXPECT_FALSE(deserialize_thrift_msg(malformed_string.data(), &malformed_length, compact,
                                            &malformed_probe)
                             .ok())
                << "compact=" << compact;

        auto valid_string = thrift_string_bytes(compact, "valid");
        uint32_t valid_string_length = valid_string.size();
        ThriftStringProbe valid_string_probe;
        EXPECT_TRUE(deserialize_thrift_msg(valid_string.data(), &valid_string_length, compact,
                                           &valid_string_probe)
                            .ok())
                << "compact=" << compact;
        EXPECT_EQ(valid_string_probe.value, "valid");

        auto valid_container = thrift_list_bytes(compact, /*count=*/1, /*total_size=*/8);
        uint32_t valid_container_length = valid_container.size();
        ThriftContainerProbe valid_container_probe;
        EXPECT_TRUE(deserialize_thrift_msg(valid_container.data(), &valid_container_length, compact,
                                           &valid_container_probe)
                            .ok())
                << "compact=" << compact;
        EXPECT_EQ(valid_container_probe.elements.size(), 1);
    }
}

TEST_F(ParquetThriftReaderTest, normal) {
    auto local_fs = io::global_local_filesystem();
    io::FileReaderSPtr reader;
    auto st = local_fs->open_file("./be/test/exec/test_data/parquet_scanner/localfile.parquet",
                                  &reader);
    EXPECT_TRUE(st.ok());

    std::unique_ptr<FileMetaData> meta_data;
    size_t meta_size;
    static_cast<void>(parse_thrift_footer(reader, &meta_data, &meta_size, nullptr, true, true));
    tparquet::FileMetaData t_metadata = meta_data->to_thrift();

    LOG(WARNING) << "=====================================";
    for (auto value : t_metadata.row_groups) {
        LOG(WARNING) << "row group num_rows: " << value.num_rows;
    }
    LOG(WARNING) << "=====================================";
    for (auto value : t_metadata.schema) {
        LOG(WARNING) << "schema column name: " << value.name;
        LOG(WARNING) << "schema column type: " << value.type;
        LOG(WARNING) << "schema column repetition_type: " << value.repetition_type;
        LOG(WARNING) << "schema column num children: " << value.num_children;
    }
}

TEST_F(ParquetThriftReaderTest, complex_nested_file) {
    // hive-complex.parquet is the part of following table:
    // complex_nested_table(
    //   `name` string,
    //   `income` array<array<int>>,
    //   `hobby` array<map<string,string>>,
    //   `friend` map<string,string>,
    //   `mark` struct<math:int,english:int>)

    auto local_fs = io::global_local_filesystem();
    io::FileReaderSPtr reader;
    auto st = local_fs->open_file("./be/test/exec/test_data/parquet_scanner/hive-complex.parquet",
                                  &reader);
    EXPECT_TRUE(st.ok());

    std::unique_ptr<FileMetaData> metadata;
    size_t meta_size;
    static_cast<void>(parse_thrift_footer(reader, &metadata, &meta_size, nullptr, true, true));
    tparquet::FileMetaData t_metadata = metadata->to_thrift();
    FieldDescriptor schemaDescriptor;
    static_cast<void>(schemaDescriptor.parse_from_thrift(t_metadata.schema));

    // table columns
    ASSERT_EQ(schemaDescriptor.get_column_index("name"), 0);
    auto name = schemaDescriptor.get_column("name");
    ASSERT_TRUE(name->children.size() == 0 && name->physical_column_index >= 0);
    ASSERT_TRUE(name->repetition_level == 0 && name->definition_level == 1);

    ASSERT_EQ(schemaDescriptor.get_column_index("income"), 1);
    auto income = schemaDescriptor.get_column("income");
    // should be parsed as ARRAY<ARRAY<INT32>>
    ASSERT_TRUE(income->data_type->get_primitive_type() == TYPE_ARRAY);
    ASSERT_TRUE(income->children.size() == 1);
    ASSERT_TRUE(income->children[0].data_type->get_primitive_type() == TYPE_ARRAY);
    ASSERT_TRUE(income->children[0].children.size() == 1);
    auto i_physical = income->children[0].children[0];
    // five levels for ARRAY<ARRAY<INT32>>
    // income --- bag --- array_element --- bag --- array_element
    //  opt       rep          opt          rep         opt
    // R=0,D=1  R=1,D=2       R=1,D=3     R=2,D=4      R=2,D=5
    ASSERT_TRUE(i_physical.repetition_level == 2 && i_physical.definition_level == 5);

    ASSERT_EQ(schemaDescriptor.get_column_index("hobby"), 2);
    auto hobby = schemaDescriptor.get_column("hobby");
    // should be parsed as ARRAY<MAP<KEY,VALUE>>
    ASSERT_TRUE(hobby->children.size() == 1 && hobby->children[0].children.size() == 2);
    ASSERT_TRUE(hobby->data_type->get_primitive_type() == TYPE_ARRAY &&
                hobby->children[0].data_type->get_primitive_type() == TYPE_MAP);
    // hobby(opt) --- bag(rep) --- array_element(opt) --- map(rep)
    //                                                      \------- key(req)
    //                                                      \------- value(opt)
    // R=0,D=1        R=1,D=2          R=1,D=3             R=2,D=4
    //                                                       \------ R=2,D=4
    //                                                       \------ R=2,D=5
    auto h_key = hobby->children[0].children[0];
    auto h_value = hobby->children[0].children[1];
    ASSERT_TRUE(h_key.repetition_level == 2 && h_key.definition_level == 4);
    ASSERT_TRUE(h_value.repetition_level == 2 && h_value.definition_level == 5);

    ASSERT_EQ(schemaDescriptor.get_column_index("friend"), 3);
    ASSERT_EQ(schemaDescriptor.get_column_index("mark"), 4);
}

static int fill_nullable_column(ColumnPtr& doris_column, level_t* definitions, size_t num_values) {
    CHECK(doris_column->is_nullable());
    doris_column = IColumn::mutate(std::move(doris_column));
    auto* nullable_column = assert_cast<ColumnNullable*>(doris_column->assert_mutable().get());
    NullMap& map_data = nullable_column->get_null_map_data();
    int null_cnt = 0;
    for (int i = 0; i < num_values; ++i) {
        bool nullable = definitions[i] == 0;
        if (nullable) {
            null_cnt++;
        }
        map_data.emplace_back(nullable);
    }
    return null_cnt;
}

static Status get_column_values(io::FileReaderSPtr file_reader, tparquet::ColumnChunk* column_chunk,
                                FieldSchema* field_schema, ColumnPtr& doris_column,
                                DataTypePtr& data_type, level_t* definitions, size_t total_rows) {
    tparquet::ColumnMetaData chunk_meta = column_chunk->meta_data;
    size_t start_offset = has_dict_page(chunk_meta) ? chunk_meta.dictionary_page_offset
                                                    : chunk_meta.data_page_offset;
    size_t chunk_size = chunk_meta.total_compressed_size;

    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    auto _converter = parquet::PhysicalToLogicalConverter::get_converter(
            field_schema, field_schema->data_type, data_type, &ctz, false);
    if (!_converter->support()) {
        return Status::InternalError("Not support");
    }

    ColumnPtr src_column = _converter->get_physical_column(
            field_schema->physical_type, field_schema->data_type, doris_column, data_type, false);
    if (_converter->read_directly_into_dst_logical_column()) {
        src_column = std::move(doris_column);
    }
    DataTypePtr& resolved_type = _converter->get_physical_type();

    io::BufferedFileStreamReader stream_reader(file_reader, start_offset, chunk_size, 1024);

    ParquetPageReadContext page_read_ctx;
    ColumnChunkReader<false, false> chunk_reader(&stream_reader, column_chunk, field_schema,
                                                 nullptr, total_rows, nullptr, page_read_ctx);
    // initialize chunk reader
    static_cast<void>(chunk_reader.init());
    // seek to next page header
    static_cast<void>(chunk_reader.parse_page_header());
    // load page data into underlying container
    static_cast<void>(chunk_reader.load_page_data());
    int rows = chunk_reader.remaining_num_values();
    // definition levels
    if (field_schema->definition_level == 0) { // required field
        std::fill(definitions, definitions + rows, 1);
    } else {
        chunk_reader._def_level_decoder.get_levels(definitions, rows);
    }
    MutableColumnPtr data_column;
    if (src_column->is_nullable()) {
        // fill nullable values
        fill_nullable_column(src_column, definitions, rows);
        auto* nullable_column = assert_cast<ColumnNullable*>(src_column->assert_mutable().get());
        data_column = nullable_column->get_nested_column_ptr();
    } else {
        src_column = IColumn::mutate(std::move(src_column));
        data_column = src_column->assert_mutable();
    }
    FilterMap filter_map;
    RETURN_IF_ERROR(filter_map.init(nullptr, 0, false));
    ColumnSelectVector run_length_map;
    // decode page data
    if (field_schema->definition_level == 0) {
        // required column
        std::vector<u_short> null_map = {(u_short)rows};
        RETURN_IF_ERROR(run_length_map.init(null_map, rows, nullptr, &filter_map, 0));
        RETURN_IF_ERROR(
                chunk_reader.decode_values(data_column, resolved_type, run_length_map, false));
    } else {
        // column with null values
        level_t level_type = definitions[0];
        int num_values = 1;
        for (int i = 1; i < rows; ++i) {
            if (definitions[i] != level_type) {
                if (level_type == 0) {
                    // null values
                    data_column->insert_many_defaults(num_values);
                } else {
                    std::vector<u_short> null_map = {(u_short)num_values};
                    RETURN_IF_ERROR(
                            run_length_map.init(null_map, num_values, nullptr, &filter_map, 0));
                    RETURN_IF_ERROR(chunk_reader.decode_values(data_column, resolved_type,
                                                               run_length_map, false));
                }
                level_type = definitions[i];
                num_values = 1;
            } else {
                num_values++;
            }
        }
        if (level_type == 0) {
            // null values
            data_column->insert_many_defaults(num_values);
        } else {
            std::vector<u_short> null_map = {(u_short)num_values};
            RETURN_IF_ERROR(run_length_map.init(null_map, num_values, nullptr, &filter_map, 0));
            RETURN_IF_ERROR(
                    chunk_reader.decode_values(data_column, resolved_type, run_length_map, false));
        }
    }
    return _converter->convert(src_column, field_schema->data_type, data_type, doris_column, false);
}

// Only the unit test depend on this, but it is wrong, should not use TTupleDesc to create tuple desc, not
// use columndesc
static doris::TupleDescriptor* create_tuple_desc(
        doris::ObjectPool* pool, std::vector<doris::SchemaScanner::ColumnDesc>& column_descs) {
    using namespace doris;
    int null_column = 0;
    for (int i = 0; i < column_descs.size(); ++i) {
        if (column_descs[i].is_null) {
            null_column++;
        }
    }

    int offset = (null_column + 7) / 8;
    std::vector<SlotDescriptor*> slots;
    int null_byte = 0;
    int null_bit = 0;

    for (int i = 0; i < column_descs.size(); ++i) {
        TSlotDescriptor t_slot_desc;
        if (column_descs[i].type == TYPE_DECIMAL128I) {
            t_slot_desc.__set_slotType(
                    DataTypeFactory::instance()
                            .create_data_type(PrimitiveType::TYPE_DECIMAL128I, false, 27, 9)
                            ->to_thrift());
        } else {
            t_slot_desc.__set_slotType(DataTypeFactory::instance()
                                               .create_data_type(column_descs[i].type, false,
                                                                 column_descs[i].precision,
                                                                 column_descs[i].scale)
                                               ->to_thrift());
        }
        t_slot_desc.__set_colName(column_descs[i].name);
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);

        if (column_descs[i].is_null) {
            t_slot_desc.__set_nullIndicatorByte(null_byte);
            t_slot_desc.__set_nullIndicatorBit(null_bit);
            null_bit = (null_bit + 1) % 8;

            if (0 == null_bit) {
                null_byte++;
            }
        } else {
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
        }

        t_slot_desc.id = i;
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);

        SlotDescriptor* slot = pool->add(new (std::nothrow) SlotDescriptor(t_slot_desc));
        slots.push_back(slot);
        offset += column_descs[i].size;
    }

    TTupleDescriptor t_tuple_desc;
    t_tuple_desc.__set_byteSize(offset);
    t_tuple_desc.__set_numNullBytes(0);
    doris::TupleDescriptor* tuple_desc =
            pool->add(new (std::nothrow) doris::TupleDescriptor(t_tuple_desc));

    for (int i = 0; i < slots.size(); ++i) {
        tuple_desc->add_slot(slots[i]);
    }

    return tuple_desc;
}

static void create_block(std::unique_ptr<Block>& block) {
    // Current supported column type:
    std::vector<SchemaScanner::ColumnDesc> column_descs = {
            {"tinyint_col", TYPE_TINYINT, sizeof(int8_t), true},
            {"smallint_col", TYPE_SMALLINT, sizeof(int16_t), true},
            {"int_col", TYPE_INT, sizeof(int32_t), true},
            {"bigint_col", TYPE_BIGINT, sizeof(int64_t), true},
            {"boolean_col", TYPE_BOOLEAN, sizeof(bool), true},
            {"float_col", TYPE_FLOAT, sizeof(float_t), true},
            {"double_col", TYPE_DOUBLE, sizeof(double_t), true},
            {"string_col", TYPE_STRING, sizeof(StringRef), true},
            {"binary_col", TYPE_VARBINARY, sizeof(StringView), true},
            // 64-bit-length, see doris::get_slot_size in primitive_type.cpp
            {"timestamp_col", TYPE_DATETIMEV2, sizeof(int128_t), true, 0, 6},
            {"decimal_col", TYPE_DECIMAL128I, sizeof(Decimal128V3), true},
            {"char_col", TYPE_CHAR, sizeof(StringRef), true},
            {"varchar_col", TYPE_VARCHAR, sizeof(StringRef), true},
            {"date_col", TYPE_DATEV2, sizeof(uint32_t), true},
            {"date_v2_col", TYPE_DATEV2, sizeof(uint32_t), true},
            {"timestamp_v2_col", TYPE_DATETIMEV2, sizeof(int128_t), true, 18, 0}};
    ObjectPool object_pool;
    doris::TupleDescriptor* tuple_desc = create_tuple_desc(&object_pool, column_descs);
    auto tuple_slots = tuple_desc->slots();
    block = Block::create_unique();
    for (const auto& slot_desc : tuple_slots) {
        auto data_type = slot_desc->type();
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }
}

static void read_parquet_data_and_check(const std::string& parquet_file,
                                        const std::string& result_file, int rows) {
    /*
     * table schema in parquet file:
     * create table `decoder`(
     * `tinyint_col` tinyint, // 0
     * `smallint_col` smallint, // 1
     * `int_col` int, // 2
     * `bigint_col` bigint, // 3
     * `boolean_col` boolean, // 4
     * `float_col` float, // 5
     * `double_col` double, // 6
     * `string_col` string, // 7
     * `binary_col` binary, // 8
     * `timestamp_col` timestamp, // 9
     * `decimal_col` decimal(10,2), // 10
     * `char_col` char(10), // 11
     * `varchar_col` varchar(50), // 12
     * `date_col` date, // 13
     * `list_string` array<string>) // 14
     */

    auto local_fs = io::global_local_filesystem();
    io::FileReaderSPtr reader;
    auto st = local_fs->open_file(parquet_file, &reader);
    EXPECT_TRUE(st.ok());

    std::unique_ptr<Block> block;
    create_block(block);

    std::unique_ptr<FileMetaData> metadata;
    size_t meta_size;
    static_cast<void>(parse_thrift_footer(reader, &metadata, &meta_size, nullptr, true, true));
    tparquet::FileMetaData t_metadata = metadata->to_thrift();
    FieldDescriptor schema_descriptor;
    static_cast<void>(schema_descriptor.parse_from_thrift(t_metadata.schema));
    std::vector<level_t> defs(rows);

    for (int c = 0; c < 14; ++c) {
        auto& column_name_with_type = block->get_by_position(c);
        auto& data_column = column_name_with_type.column;
        auto& data_type = column_name_with_type.type;
        static_cast<void>(
                get_column_values(reader, &t_metadata.row_groups[0].columns[c],
                                  const_cast<FieldSchema*>(schema_descriptor.get_column(c)),
                                  data_column, data_type, defs.data(), rows));
    }
    // `date_v2_col` date, // 14 - 13, DATEV2
    {
        auto& column_name_with_type = block->get_by_position(14);
        auto& data_column = column_name_with_type.column;
        auto& data_type = column_name_with_type.type;
        static_cast<void>(
                get_column_values(reader, &t_metadata.row_groups[0].columns[13],
                                  const_cast<FieldSchema*>(schema_descriptor.get_column(13)),
                                  data_column, data_type, defs.data(), rows));
    }
    // `timestamp_v2_col` timestamp, // 15 - 9, DATETIMEV2
    {
        auto& column_name_with_type = block->get_by_position(15);
        auto& data_column = column_name_with_type.column;
        auto& data_type = column_name_with_type.type;
        static_cast<void>(
                get_column_values(reader, &t_metadata.row_groups[0].columns[9],
                                  const_cast<FieldSchema*>(schema_descriptor.get_column(9)),
                                  data_column, data_type, defs.data(), rows));
    }

    io::FileReaderSPtr result;
    auto rst = local_fs->open_file(result_file, &result);
    EXPECT_TRUE(rst.ok());
    std::vector<uint8_t> result_buf(result->size() + 1);
    result_buf[result->size()] = '\0';
    size_t bytes_read;
    Slice res(result_buf.data(), result->size());
    static_cast<void>(result->read_at(0, res, &bytes_read));
    ASSERT_STREQ(block->dump_data(0, rows).c_str(), reinterpret_cast<char*>(result_buf.data()));
}

TEST_F(ParquetThriftReaderTest, type_decoder) {
    read_parquet_data_and_check("./be/test/exec/test_data/parquet_scanner/type-decoder.parquet",
                                "./be/test/exec/test_data/parquet_scanner/type-decoder.txt", 10);
}

TEST_F(ParquetThriftReaderTest, dict_decoder) {
    read_parquet_data_and_check("./be/test/exec/test_data/parquet_scanner/dict-decoder.parquet",
                                "./be/test/exec/test_data/parquet_scanner/dict-decoder.txt", 12);
}

TEST_F(ParquetThriftReaderTest, is_dictionary_encoded_rejects_plain_data_page_v2) {
    tparquet::ColumnMetaData column_metadata;
    column_metadata.type = tparquet::Type::BYTE_ARRAY;
    column_metadata.__isset.encoding_stats = true;

    tparquet::PageEncodingStats dict_page;
    dict_page.page_type = tparquet::PageType::DATA_PAGE_V2;
    dict_page.encoding = tparquet::Encoding::RLE_DICTIONARY;
    dict_page.count = 2;

    tparquet::PageEncodingStats plain_page;
    plain_page.page_type = tparquet::PageType::DATA_PAGE_V2;
    plain_page.encoding = tparquet::Encoding::PLAIN;
    plain_page.count = 1;

    column_metadata.encoding_stats = {dict_page, plain_page};

    tparquet::RowGroup row_group;
    row_group.num_rows = 0;
    RowGroupReader::PositionDeleteContext position_delete_ctx(row_group.num_rows, 0);
    RowGroupReader::LazyReadContext lazy_read_ctx;
    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;
    RowGroupReader row_group_reader(nullptr, {}, 0, row_group, nullptr, nullptr,
                                    position_delete_ctx, lazy_read_ctx, nullptr, column_ids,
                                    filter_column_ids);

    EXPECT_FALSE(row_group_reader.is_dictionary_encoded(column_metadata));
}
} // namespace doris
