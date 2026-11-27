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

#include "exec/sink/writer/vfile_result_writer.h"

#include <gen_cpp/internal_service.pb.h>
#include <google/protobuf/stubs/callback.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <map>

#include "exec/operator/result_sink_operator.h"
#include "exec/sink/writer/vmysql_result_writer.h"
#include "io/fs/local_file_system.h"
#include "service/backend_options.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"

namespace doris {

class VFileResultWriterTest : public testing::Test {
protected:
    void SetUp() override {
        auto fs = io::global_local_filesystem();
        ASSERT_TRUE(fs->delete_directory(_directory).ok());
        ASSERT_TRUE(fs->create_directory(_directory).ok());
        _options.file_path = _directory + "/part_";
        _options.file_format = TFileFormatType::FORMAT_CSV_PLAIN;
        _fragment_id.__set_hi(0x123456789abcdef);
        _fragment_id.__set_lo(0xfedcba987654321);
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_directory).ok());
    }

    std::unique_ptr<VFileResultWriter> make_writer(
            TStorageBackendType::type storage_type,
            std::shared_ptr<MySQLResultBlockBuffer> sinker = nullptr) {
        if (!sinker) {
            sinker = std::make_shared<MySQLResultBlockBuffer>(TUniqueId(), &_state, 16);
        }
        return std::make_unique<VFileResultWriter>(&_options, storage_type, _fragment_id, _exprs,
                                                   sinker, nullptr, false, RowDescriptor(), nullptr,
                                                   nullptr);
    }

    std::string mysql_url(const std::string& row) {
        size_t offset = 0;
        for (int i = 0; i < 4; ++i) {
            size_t length = static_cast<unsigned char>(row[offset++]);
            if (length >= 0xfc) {
                int bytes = 8;
                if (length == 0xfc) {
                    bytes = 2;
                } else if (length == 0xfd) {
                    bytes = 3;
                }
                length = 0;
                for (int j = 0; j < bytes; ++j) {
                    length |= static_cast<size_t>(static_cast<unsigned char>(row[offset++]))
                              << (8 * j);
                }
            }
            if (i == 3) {
                return row.substr(offset, length);
            }
            offset += length;
        }
        return "";
    }

    void check_single_file_urls(TStorageBackendType::type storage_type,
                                const std::string& expected_suffix) {
        RuntimeProfile profile {"VFileResultWriterTest"};
        auto writer = make_writer(storage_type);
        writer->_state = &_state;
        writer->_init_profile(&profile);
        ASSERT_TRUE(writer->_get_next_file_name(&writer->_file_name).ok());
        const auto prefix = _options.file_path + print_id(_fragment_id) + "_";
        const auto expected_path = prefix + "0." + expected_suffix;
        ASSERT_EQ(expected_path, writer->_file_name);
        ASSERT_TRUE(writer->_send_result().ok());
        const auto& batch = writer->_sinker->_result_batch_queue.front()->result_batch;
        const auto host = storage_type == TStorageBackendType::LOCAL
                                  ? "file:///" + BackendOptions::get_localhost()
                                  : "";
        EXPECT_EQ(host + expected_path, batch.attached_infos.at("SingleFileURL"));
        EXPECT_EQ(host + prefix + "*", batch.attached_infos.at("URL"));
        EXPECT_EQ(batch.attached_infos.at("URL"), mysql_url(batch.rows[0]));
    }

    void check_created_local_file(const std::string& expected_path) {
        RuntimeProfile profile {"VFileResultWriterTest"};
        auto writer = make_writer(TStorageBackendType::LOCAL);
        ASSERT_TRUE(writer->open(&_state, &profile).ok());
        ASSERT_EQ(expected_path, writer->_file_name);
        ASSERT_TRUE(writer->close(Status::OK()).ok());
        bool exists = false;
        ASSERT_TRUE(io::global_local_filesystem()->exists(expected_path, &exists).ok());
        ASSERT_TRUE(exists);
        ASSERT_EQ(1, writer->_sinker->_result_batch_queue.size());
        const auto& batch = writer->_sinker->_result_batch_queue.front()->result_batch;
        const auto& infos = batch.attached_infos;
        EXPECT_EQ("file:///" + BackendOptions::get_localhost() + expected_path,
                  infos.at("SingleFileURL"));
        EXPECT_EQ("1", infos.at("FileNumber"));
        EXPECT_EQ('*', infos.at("URL").back());
        EXPECT_EQ(infos.at("URL"), mysql_url(batch.rows[0]));
    }

    const std::string _directory = std::filesystem::absolute("ut_dir/vfile_result_writer_test");
    MockRuntimeState _state;
    VExprContextSPtrs _exprs;
    TResultFileSinkOptions _thrift_options;
    ResultFileOptions _options {_thrift_options};
    TUniqueId _fragment_id;
};

TEST_F(VFileResultWriterTest, SingleFileUrlMatchesCreatedFile) {
    _options.file_suffix = "custom.csv.gz";
    _options.compression_type = TFileCompressType::GZ;
    check_created_local_file(_options.file_path + print_id(_fragment_id) + "_0.custom.csv.gz");
}

TEST_F(VFileResultWriterTest, SingleFileSuffixesAndStorageUrls) {
    const std::vector<std::pair<TFileCompressType::type, std::string>> compressions = {
            {TFileCompressType::PLAIN, "csv"},
            {TFileCompressType::GZ, "csv.gz"},
            {TFileCompressType::BZ2, "csv.bz2"},
            {TFileCompressType::SNAPPYBLOCK, "csv.snappy"},
            {TFileCompressType::LZ4BLOCK, "csv.lz4"},
            {TFileCompressType::ZSTD, "csv.zstd"}};
    const std::pair<TStorageBackendType::type, std::string> paths[] = {
            {TStorageBackendType::LOCAL, _directory + "/part_"},
            {TStorageBackendType::S3, "s3://bucket/path/part_"},
            {TStorageBackendType::BROKER, "hdfs://namenode/path/part_"},
            {TStorageBackendType::HDFS, "hdfs://namenode/path/part_"}};
    for (const auto& [storage_type, path] : paths) {
        _options.file_path = path;
        for (const auto& [compression, csv_suffix] : compressions) {
            _options.compression_type = compression;
            const std::pair<TFileFormatType::type, std::string> formats[] = {
                    {TFileFormatType::FORMAT_CSV_PLAIN, csv_suffix},
                    {TFileFormatType::FORMAT_PARQUET, "parquet"},
                    {TFileFormatType::FORMAT_ORC, "orc"}};
            for (const auto& [format, suffix] : formats) {
                _options.file_format = format;
                _options.file_suffix.clear();
                check_single_file_urls(storage_type, suffix);
                _options.file_suffix = "custom.suffix";
                check_single_file_urls(storage_type, "custom.suffix");
            }
        }
    }
}

TEST_F(VFileResultWriterTest, SingleFileUrlPreservesPadding) {
    const std::vector<std::pair<int, std::string>> indices = {
            {0, "0"}, {1, "0"}, {5, "00000"}, {10, "0000000000"}};
    _options.file_suffix = "custom.csv.gz";
    _options.compression_type = TFileCompressType::GZ;
    for (const auto& [padding, index] : indices) {
        _options.file_path = _directory + "/padding_" + std::to_string(padding) + "_";
        _options.file_name_padding = padding;
        check_created_local_file(_options.file_path + print_id(_fragment_id) + "_" + index +
                                 ".custom.csv.gz");
    }
}

TEST_F(VFileResultWriterTest, ZeroAndMultipleFilesKeepWildcard) {
    for (int count : {0, 2}) {
        RuntimeProfile profile {"VFileResultWriterTest"};
        auto writer = make_writer(TStorageBackendType::LOCAL);
        writer->_state = &_state;
        writer->_init_profile(&profile);
        for (int i = 0; i < count; ++i) {
            ASSERT_TRUE(writer->_get_next_file_name(&writer->_file_name).ok());
        }
        ASSERT_TRUE(writer->_send_result().ok());
        const auto& batch = writer->_sinker->_result_batch_queue.front()->result_batch;
        EXPECT_EQ(std::to_string(count), batch.attached_infos.at("FileNumber"));
        EXPECT_EQ(0, batch.attached_infos.count("SingleFileURL"));
        EXPECT_EQ("file:///" + BackendOptions::get_localhost() + _options.file_path +
                          print_id(_fragment_id) + "_*",
                  batch.attached_infos.at("URL"));
        EXPECT_EQ(batch.attached_infos.at("URL"), mysql_url(batch.rows[0]));
    }
}

// GTest assertion macros count as branches in both the writing and fetching phases.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(VFileResultWriterTest, SharedSenderPreservesEverySingleFileUrl) {
    struct FetchDone : google::protobuf::Closure {
        void Run() override { ++calls; }
        int calls = 0;
    };

    auto sinker = std::make_shared<MySQLResultBlockBuffer>(TUniqueId(), &_state, 16);
    std::vector<std::map<std::string, std::string>> expected_infos;
    for (int i = 0; i < 3; ++i) {
        // Keep all results queued, as when parallel writers finish before FE fetches them.
        ++_fragment_id.lo;
        _state._fragment_instance_id = _fragment_id;
        RuntimeProfile profile {"VFileResultWriterTest"};
        auto writer = make_writer(TStorageBackendType::LOCAL, sinker);
        ASSERT_TRUE(writer->open(&_state, &profile).ok());
        const std::string path = _options.file_path + print_id(_fragment_id) + "_0.csv";
        ASSERT_EQ(path, writer->_file_name);
        ASSERT_TRUE(writer->close(Status::OK()).ok());
        bool exists = false;
        ASSERT_TRUE(io::global_local_filesystem()->exists(path, &exists).ok());
        ASSERT_TRUE(exists);
        const std::string url_prefix = "file:///" + BackendOptions::get_localhost();
        expected_infos.push_back(
                {{"FileNumber", "1"},
                 {"TotalRows", "0"},
                 {"FileSize", "0"},
                 {"URL", url_prefix + _options.file_path + print_id(_fragment_id) + "_*"},
                 {"SingleFileURL", url_prefix + path},
                 {"WriteTimeSec", "0.000"},
                 {"WriteSpeedKB", "0.00"}});
        ASSERT_EQ(i + 1, sinker->_result_batch_queue.size());
        EXPECT_EQ(expected_infos.back(),
                  sinker->_result_batch_queue.back()->result_batch.attached_infos);
    }

    // Exercise the same serialized TResultBatch that ResultReceiver passes to StmtExecutor.
    for (size_t i = 0; i < expected_infos.size(); ++i) {
        PFetchDataResult response;
        FetchDone done;
        auto ctx = GetResultBatchCtx::create_shared(&response, &done);
        ASSERT_TRUE(sinker->get_batch(ctx).ok());
        ASSERT_EQ(1, done.calls);
        ASSERT_EQ(ErrorCode::OK, response.status().status_code());
        EXPECT_EQ(i, response.packet_seq());
        EXPECT_FALSE(response.eos());
        TResultBatch batch;
        auto length = static_cast<uint32_t>(response.row_batch().size());
        ASSERT_TRUE(deserialize_thrift_msg(
                            reinterpret_cast<const uint8_t*>(response.row_batch().data()), &length,
                            false, &batch)
                            .ok());
        EXPECT_TRUE(batch.__isset.attached_infos);
        EXPECT_EQ(expected_infos[i], batch.attached_infos);
        ASSERT_EQ(1, batch.rows.size());
        EXPECT_EQ(expected_infos[i].at("URL"), mysql_url(batch.rows[0]));
    }
    EXPECT_TRUE(sinker->_result_batch_queue.empty());
    EXPECT_TRUE(sinker->_instance_rows_in_queue.empty());
    for (const auto& [id, rows] : sinker->_instance_rows) {
        EXPECT_EQ(0, rows);
    }
}

} // namespace doris
