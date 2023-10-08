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
#include <gtest/gtest.h>

#include <filesystem>

#include "agent/be_exec_version_manager.h"
#include "common/object_pool.h"
#include "gen_cpp/internal_service.pb.h"
#include "gmock/gmock.h"
#include "io/fs/local_file_system.h"
#include "olap/wal_reader.h"
#include "olap/wal_writer.h"
#include "runtime/exec_env.h"
#include "service/brpc.h"
#include "testutil/test_util.h"
#include "util/proto_util.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_number.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/runtime/vdata_stream_recvr.h"

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

std::string WalReaderWriterTest::_s_test_data_path = "./log/wal_reader_writer_test";
size_t block_rows = 1024;

void covert_block_to_pb(
        const vectorized::Block& block, PBlock* pblock,
        segment_v2::CompressionTypePB compression_type = segment_v2::CompressionTypePB::SNAPPY) {
    size_t uncompressed_bytes = 0;
    size_t compressed_bytes = 0;
    Status st = block.serialize(BeExecVersionManager::get_newest_version(), pblock,
                                &uncompressed_bytes, &compressed_bytes, compression_type);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(uncompressed_bytes >= compressed_bytes);
    EXPECT_EQ(compressed_bytes, pblock->column_values().size());

    const vectorized::ColumnWithTypeAndName& type_and_name =
            block.get_columns_with_type_and_name()[0];
    EXPECT_EQ(type_and_name.name, pblock->column_metas()[0].name());
}

void generate_block(PBlock& pblock, int row_index) {
    auto vec = vectorized::ColumnVector<int32_t>::create();
    auto& data = vec->get_data();
    for (int i = 0; i < block_rows; ++i) {
        data.push_back(i + row_index);
    }
    vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
    vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, "test_int");
    vectorized::Block block({type_and_name});
    covert_block_to_pb(block, &pblock, segment_v2::CompressionTypePB::SNAPPY);
}

TEST_F(WalReaderWriterTest, TestWriteAndRead1) {
    std::string file_name = _s_test_data_path + "/abcd123.txt";
    auto wal_writer = WalWriter(file_name);
    static_cast<void>(wal_writer.init());
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
    auto wal_reader = WalReader(file_name);
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
        vectorized::Block block;
        EXPECT_TRUE(block.deserialize(pblock).ok());
        EXPECT_EQ(block_rows, block.rows());
    }
    static_cast<void>(wal_reader.finalize());
    EXPECT_EQ(3, block_count);
}
} // namespace doris