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
#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include <array>
#include <filesystem>
#include <memory>

#include "agent/be_exec_version_manager.h"
#include "common/object_pool.h"
#include "core/data_type/data_type_number.h"
#include "exec/exchange/vdata_stream_mgr.h"
#include "exec/exchange/vdata_stream_recvr.h"
#include "gmock/gmock.h"
#include "io/fs/local_file_system.h"
#include "load/group_commit/wal/wal_file_reader.h"
#include "load/group_commit/wal/wal_writer.h"
#include "runtime/exec_env.h"
#include "service/brpc.h"
#include "testutil/test_util.h"
#include "util/proto_util.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

class WalReaderWriterTest : public testing::Test {
public:
    // create a mock cgroup folder
    virtual void SetUp() {
        static_cast<void>(io::global_local_filesystem()->create_directory(_s_test_data_path));
    }

    // delete the mock cgroup folder
    virtual void TearDown() {
        static_cast<void>(io::global_local_filesystem()->delete_directory(_s_test_data_path));
    }

    static std::string _s_test_data_path;
};

std::string WalReaderWriterTest::_s_test_data_path = "./log/wal_reader_writer_test/0/0";
size_t block_rows = 1024;

void covert_block_to_pb(
        const Block& block, PBlock* pblock,
        segment_v2::CompressionTypePB compression_type = segment_v2::CompressionTypePB::SNAPPY) {
    size_t uncompressed_bytes = 0;
    size_t compressed_bytes = 0;
    int64_t compressed_time = 0;
    Status st =
            block.serialize(BeExecVersionManager::get_newest_version(), pblock, &uncompressed_bytes,
                            &compressed_bytes, &compressed_time, compression_type);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(uncompressed_bytes >= compressed_bytes);
    EXPECT_EQ(compressed_bytes, pblock->column_values().size());

    const ColumnWithTypeAndName& type_and_name = block.get_columns_with_type_and_name()[0];
    EXPECT_EQ(type_and_name.name, pblock->column_metas()[0].name());
}

void generate_block(PBlock& pblock, int row_index) {
    auto vec = ColumnInt32::create();
    auto& data = vec->get_data();
    for (int i = 0; i < block_rows; ++i) {
        data.push_back(i + row_index);
    }
    DataTypePtr data_type(std::make_shared<DataTypeInt32>());
    ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, "test_int");
    Block block({type_and_name});
    covert_block_to_pb(block, &pblock, segment_v2::CompressionTypePB::SNAPPY);
}

TEST_F(WalReaderWriterTest, TestWriteAndRead1) {
    std::string file_name = _s_test_data_path + "/abcd123.txt";
    auto wal_writer = WalWriter(file_name);
    static_cast<void>(wal_writer.init(io::global_local_filesystem()));
    size_t file_len = 0;
    int64_t file_size = -1;
    // add 1 block
    {
        PBlock pblock;
        generate_block(pblock, 0);

        EXPECT_EQ(Status::OK(), wal_writer.append_blocks(std::vector<PBlock*> {&pblock}));
        file_len += pblock.ByteSizeLong() + WalWriter::LENGTH_SIZE + WalWriter::CHECKSUM_SIZE;
        EXPECT_TRUE(io::global_local_filesystem()->file_size(file_name, &file_size).ok());
        EXPECT_EQ(file_len, file_size);
    }
    // add 2 block
    {
        PBlock pblock;
        generate_block(pblock, 1024);
        file_len += pblock.ByteSizeLong() + WalWriter::LENGTH_SIZE + WalWriter::CHECKSUM_SIZE;

        PBlock pblock1;
        generate_block(pblock1, 2048);
        file_len += pblock1.ByteSizeLong() + WalWriter::LENGTH_SIZE + WalWriter::CHECKSUM_SIZE;

        EXPECT_EQ(Status::OK(), wal_writer.append_blocks(std::vector<PBlock*> {&pblock, &pblock1}));
        EXPECT_TRUE(io::global_local_filesystem()->file_size(file_name, &file_size).ok());
        EXPECT_EQ(file_len, file_size);
    }
    static_cast<void>(wal_writer.finalize());
    // read block
    auto wal_reader = WalFileReader(file_name);
    static_cast<void>(wal_reader.init());
    auto block_count = 0;
    while (true) {
        doris::PBlock pblock;
        Status st = wal_reader.read_block(pblock);
        EXPECT_TRUE(st.ok() || st.is<ErrorCode::END_OF_FILE>());
        if (st.ok()) {
            ++block_count;
        } else if (st.is<ErrorCode::END_OF_FILE>()) {
            break;
        }
        Block block;
        size_t uncompress_size = 0;
        int64_t uncompressed_time = 0;
        EXPECT_TRUE(block.deserialize(pblock, &uncompress_size, &uncompressed_time).ok());
        EXPECT_EQ(block_rows, block.rows());
    }
    static_cast<void>(wal_reader.finalize());
    EXPECT_EQ(3, block_count);
}

TEST_F(WalReaderWriterTest, TestReadIncompleteLastRecord) {
    PBlock first_block;
    PBlock last_block;
    generate_block(first_block, 0);
    generate_block(last_block, block_rows);

    const size_t first_record_size =
            WalWriter::LENGTH_SIZE + first_block.ByteSizeLong() + WalWriter::CHECKSUM_SIZE;
    const size_t last_block_end =
            first_record_size + WalWriter::LENGTH_SIZE + last_block.ByteSizeLong();
    const std::array<size_t, 6> truncated_sizes = {
            first_record_size + WalWriter::LENGTH_SIZE / 2,
            first_record_size + WalWriter::LENGTH_SIZE + last_block.ByteSizeLong() / 2,
            last_block_end,
            last_block_end + 1,
            last_block_end + 2,
            last_block_end + 3};

    for (size_t i = 0; i < truncated_sizes.size(); ++i) {
        std::string file_name = _s_test_data_path + "/incomplete_last_record_" + std::to_string(i);
        auto wal_writer = WalWriter(file_name);
        ASSERT_TRUE(wal_writer.init(io::global_local_filesystem()).ok());
        ASSERT_TRUE(wal_writer.append_blocks({&first_block, &last_block}).ok());
        ASSERT_TRUE(wal_writer.finalize().ok());
        ASSERT_NO_THROW(std::filesystem::resize_file(file_name, truncated_sizes[i]));

        auto wal_reader = WalFileReader(file_name);
        ASSERT_TRUE(wal_reader.init().ok());
        PBlock block;
        EXPECT_TRUE(wal_reader.read_block(block).ok());
        auto st = wal_reader.read_block(block);
        if (i < 2) {
            EXPECT_TRUE(st.is<ErrorCode::END_OF_FILE>());
        } else {
            EXPECT_TRUE(st.ok());
            Block deserialized_block;
            size_t uncompressed_size = 0;
            int64_t uncompressed_time = 0;
            EXPECT_TRUE(
                    deserialized_block.deserialize(block, &uncompressed_size, &uncompressed_time)
                            .ok());
            EXPECT_EQ(block_rows, deserialized_block.rows());
            EXPECT_TRUE(wal_reader.read_block(block).is<ErrorCode::END_OF_FILE>());
        }
        EXPECT_TRUE(wal_reader.finalize().ok());
    }
}

TEST_F(WalReaderWriterTest, TestReadIncompleteHeader) {
    const std::string column_ids = "1,2";
    const size_t version_header_size = k_wal_magic_length + WalWriter::VERSION_SIZE;
    const size_t fixed_header_size = version_header_size + WalWriter::LENGTH_SIZE;
    const std::array<size_t, 8> truncated_sizes = {0,
                                                   k_wal_magic_length - 1,
                                                   k_wal_magic_length,
                                                   version_header_size - 1,
                                                   version_header_size,
                                                   fixed_header_size - 1,
                                                   fixed_header_size,
                                                   fixed_header_size + column_ids.size() - 1};

    for (size_t i = 0; i < truncated_sizes.size(); ++i) {
        std::string file_name = _s_test_data_path + "/incomplete_header_" + std::to_string(i);
        auto wal_writer = WalWriter(file_name);
        ASSERT_TRUE(wal_writer.init(io::global_local_filesystem()).ok());
        ASSERT_TRUE(wal_writer.append_header(column_ids).ok());
        ASSERT_TRUE(wal_writer.finalize().ok());
        ASSERT_NO_THROW(std::filesystem::resize_file(file_name, truncated_sizes[i]));

        auto wal_reader = WalFileReader(file_name);
        ASSERT_TRUE(wal_reader.init().ok());
        uint32_t version = 0;
        std::string actual_column_ids;
        EXPECT_TRUE(wal_reader.read_header(version, actual_column_ids)
                            .is<ErrorCode::DATA_QUALITY_ERROR>());
        EXPECT_TRUE(wal_reader.finalize().ok());
    }
}
} // namespace doris
