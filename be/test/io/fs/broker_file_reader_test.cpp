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

#include <gen_cpp/PaloBrokerService_types.h>
#include <gen_cpp/TPaloBrokerService.h>
#include <gen_cpp/Types_types.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/broker_file_reader.h"
#include "io/fs/path.h"
#include "runtime/client_cache.h"
#include "util/slice.h"

namespace doris::io {

// Mock TPaloBrokerServiceIf for testing - this is the interface that TPaloBrokerServiceClient implements
class MockTPaloBrokerServiceIf : public TPaloBrokerServiceIf {
public:
    MOCK_METHOD(void, listPath, (TBrokerListResponse&, const TBrokerListPathRequest&), (override));
    MOCK_METHOD(void, listLocatedFiles, (TBrokerListResponse&, const TBrokerListPathRequest&), (override));
    MOCK_METHOD(void, isSplittable, (TBrokerIsSplittableResponse&, const TBrokerIsSplittableRequest&), (override));
    MOCK_METHOD(void, deletePath, (TBrokerOperationStatus&, const TBrokerDeletePathRequest&), (override));
    MOCK_METHOD(void, renamePath, (TBrokerOperationStatus&, const TBrokerRenamePathRequest&), (override));
    MOCK_METHOD(void, checkPathExist, (TBrokerCheckPathExistResponse&, const TBrokerCheckPathExistRequest&), (override));
    MOCK_METHOD(void, openReader, (TBrokerOpenReaderResponse&, const TBrokerOpenReaderRequest&), (override));
    MOCK_METHOD(void, pread, (TBrokerReadResponse&, const TBrokerPReadRequest&), (override));
    MOCK_METHOD(void, seek, (TBrokerOperationStatus&, const TBrokerSeekRequest&), (override));
    MOCK_METHOD(void, closeReader, (TBrokerOperationStatus&, const TBrokerCloseReaderRequest&), (override));
    MOCK_METHOD(void, openWriter, (TBrokerOpenWriterResponse&, const TBrokerOpenWriterRequest&), (override));
    MOCK_METHOD(void, pwrite, (TBrokerOperationStatus&, const TBrokerPWriteRequest&), (override));
    MOCK_METHOD(void, closeWriter, (TBrokerOperationStatus&, const TBrokerCloseWriterRequest&), (override));
    MOCK_METHOD(void, ping, (TBrokerOperationStatus&, const TBrokerPingBrokerRequest&), (override));
    MOCK_METHOD(void, fileSize, (TBrokerFileSizeResponse&, const TBrokerFileSizeRequest&), (override));
};

// Test fixture for BrokerFileReader
class BrokerFileReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _original_chunk_size = config::max_chunk_size_for_broker;
        _broker_addr.__set_hostname("localhost");
        _broker_addr.__set_port(8000);
        _path = Path("/test/file.txt");
        _file_size = 10000;
        // Create TBrokerFD - it has high and low fields
        _fd.high = 0;
        _fd.low = 123;
        
        // Create mock client
        _mock_client = std::make_unique<MockTPaloBrokerServiceIf>();
    }

    void TearDown() override { 
        config::max_chunk_size_for_broker = _original_chunk_size;
        _mock_client.reset();
    }

    // Helper to create a simple read response
    void setup_read_response(TBrokerReadResponse& response, TBrokerOperationStatusCode::type code,
                             const std::string& data) {
        TBrokerOperationStatus op_status;
        op_status.__set_statusCode(code);
        op_status.__set_message("");
        response.__set_opStatus(op_status);
        if (code == TBrokerOperationStatusCode::OK) {
            response.__set_data(data);
        }
    }

    int64_t _original_chunk_size;
    TNetworkAddress _broker_addr;
    Path _path;
    size_t _file_size;
    TBrokerFD _fd;
    
    std::unique_ptr<MockTPaloBrokerServiceIf> _mock_client;
};

// Test 1: Single read mode with <= 0 configuration (tests with -1)
TEST_F(BrokerFileReaderTest, SingleReadModeWithNonPositive) {
    config::max_chunk_size_for_broker = -1;

    // Expect a single pread call for all data
    std::string expected_data(1000, 'a');
    EXPECT_CALL(*_mock_client, pread(testing::_, testing::_))
            .WillOnce([this, &expected_data](TBrokerReadResponse& response,
                                              const TBrokerPReadRequest& request) {
                EXPECT_EQ(0, request.offset);
                EXPECT_EQ(1000, request.length);
                setup_read_response(response, TBrokerOperationStatusCode::OK, expected_data);
            });

    // Use the test constructor to inject mock client
    auto connection = std::make_shared<BrokerServiceConnection>(
            reinterpret_cast<TPaloBrokerServiceClient*>(_mock_client.get()));
    
    auto reader = std::make_unique<BrokerFileReader>(
            _broker_addr, _path, _file_size, _fd, connection);

    char buffer[1000];
    Slice result(buffer, sizeof(buffer));
    size_t bytes_read = 0;

    Status st = reader->read_at(0, result, &bytes_read, nullptr);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(1000, bytes_read);
    EXPECT_EQ(expected_data, std::string(buffer, bytes_read));
}

// Test 1b: Single read mode with 0 configuration
TEST_F(BrokerFileReaderTest, SingleReadModeWithZero) {
    config::max_chunk_size_for_broker = 0;

    // Expect a single pread call for all data
    std::string expected_data(1000, 'a');
    EXPECT_CALL(*_mock_client, pread(testing::_, testing::_))
            .WillOnce([this, &expected_data](TBrokerReadResponse& response,
                                              const TBrokerPReadRequest& request) {
                EXPECT_EQ(0, request.offset);
                EXPECT_EQ(1000, request.length);
                setup_read_response(response, TBrokerOperationStatusCode::OK, expected_data);
            });

    // Use the test constructor to inject mock client
    auto connection = std::make_shared<BrokerServiceConnection>(
            reinterpret_cast<TPaloBrokerServiceClient*>(_mock_client.get()));
    
    auto reader = std::make_unique<BrokerFileReader>(
            _broker_addr, _path, _file_size, _fd, connection);

    char buffer[1000];
    Slice result(buffer, sizeof(buffer));
    size_t bytes_read = 0;

    Status st = reader->read_at(0, result, &bytes_read, nullptr);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(1000, bytes_read);
    EXPECT_EQ(expected_data, std::string(buffer, bytes_read));
}

// Test 2: Chunked read mode with 100-byte chunks
TEST_F(BrokerFileReaderTest, ChunkedReadMode) {
    config::max_chunk_size_for_broker = 100;

    // Expect multiple pread calls
    int call_count = 0;
    EXPECT_CALL(*_mock_client, pread(testing::_, testing::_))
            .WillRepeatedly([this, &call_count](TBrokerReadResponse& response,
                                                 const TBrokerPReadRequest& request) {
                call_count++;
                // Return 100 bytes each time (or less on last call)
                size_t to_return = std::min(request.length, (int64_t)100);
                setup_read_response(response, TBrokerOperationStatusCode::OK,
                                    std::string(to_return, 'x'));
            });

    auto connection = std::make_shared<BrokerServiceConnection>(
            reinterpret_cast<TPaloBrokerServiceClient*>(_mock_client.get()));
    
    auto reader = std::make_unique<BrokerFileReader>(
            _broker_addr, _path, _file_size, _fd, connection);

    char buffer[350];
    Slice result(buffer, sizeof(buffer));
    size_t bytes_read = 0;

    Status st = reader->read_at(0, result, &bytes_read, nullptr);

    EXPECT_TRUE(st.ok());
    EXPECT_EQ(350, bytes_read);
    EXPECT_EQ(4, call_count); // 100 + 100 + 100 + 50
}

// Test 3: EOF handling
TEST_F(BrokerFileReaderTest, EOFHandling) {
    config::max_chunk_size_for_broker = 1000;

    EXPECT_CALL(*_mock_client, pread(testing::_, testing::_))
            .WillOnce([this](TBrokerReadResponse& response,
                             const TBrokerPReadRequest& request) {
                setup_read_response(response, TBrokerOperationStatusCode::END_OF_FILE, "");
            });

    auto connection = std::make_shared<BrokerServiceConnection>(
            reinterpret_cast<TPaloBrokerServiceClient*>(_mock_client.get()));
    
    auto reader = std::make_unique<BrokerFileReader>(
            _broker_addr, _path, _file_size, _fd, connection);

    char buffer[100];
    Slice result(buffer, sizeof(buffer));
    size_t bytes_read = 0;

    Status st = reader->read_at(0, result, &bytes_read, nullptr);

    // Should return OK on EOF
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(0, bytes_read);
}

// Test 4: Partial read (broker returns less than requested)
TEST_F(BrokerFileReaderTest, PartialRead) {
    config::max_chunk_size_for_broker = 1024;

    EXPECT_CALL(*_mock_client, pread(testing::_, testing::_))
            .WillOnce([this](TBrokerReadResponse& response,
                             const TBrokerPReadRequest& request) {
                // Return only 512 bytes instead of 1024
                setup_read_response(response, TBrokerOperationStatusCode::OK,
                                    std::string(512, 'z'));
            });

    auto connection = std::make_shared<BrokerServiceConnection>(
            reinterpret_cast<TPaloBrokerServiceClient*>(_mock_client.get()));
    
    auto reader = std::make_unique<BrokerFileReader>(
            _broker_addr, _path, _file_size, _fd, connection);

    char buffer[1024];
    Slice result(buffer, sizeof(buffer));
    size_t bytes_read = 0;

    Status st = reader->read_at(0, result, &bytes_read, nullptr);

    EXPECT_TRUE(st.ok());
    EXPECT_EQ(512, bytes_read);
}

// Test 5: Error handling from broker
TEST_F(BrokerFileReaderTest, BrokerError) {
    config::max_chunk_size_for_broker = 1000;

    EXPECT_CALL(*_mock_client, pread(testing::_, testing::_))
            .WillOnce([](TBrokerReadResponse& response,
                         const TBrokerPReadRequest& request) {
                TBrokerOperationStatus op_status;
                op_status.__set_statusCode(TBrokerOperationStatusCode::INVALID_INPUT_OFFSET);
                op_status.__set_message("Invalid input");
                response.__set_opStatus(op_status);
            });

    auto connection = std::make_shared<BrokerServiceConnection>(
            reinterpret_cast<TPaloBrokerServiceClient*>(_mock_client.get()));
    
    auto reader = std::make_unique<BrokerFileReader>(
            _broker_addr, _path, _file_size, _fd, connection);

    char buffer[100];
    Slice result(buffer, sizeof(buffer));
    size_t bytes_read = 0;

    Status st = reader->read_at(0, result, &bytes_read, nullptr);

    // Should return error
    EXPECT_FALSE(st.ok());
}

// Test 6: Read at different offsets with chunk verification
TEST_F(BrokerFileReaderTest, ReadAtDifferentOffsets) {
    config::max_chunk_size_for_broker = 100;

    std::vector<std::pair<int64_t, int64_t>> requests;
    
    EXPECT_CALL(*_mock_client, pread(testing::_, testing::_))
            .WillRepeatedly([this, &requests](TBrokerReadResponse& response,
                                               const TBrokerPReadRequest& request) {
                requests.push_back({request.offset, request.length});
                setup_read_response(response, TBrokerOperationStatusCode::OK,
                                    std::string(request.length, 'o'));
            });

    auto connection = std::make_shared<BrokerServiceConnection>(
            reinterpret_cast<TPaloBrokerServiceClient*>(_mock_client.get()));
    
    auto reader = std::make_unique<BrokerFileReader>(
            _broker_addr, _path, _file_size, _fd, connection);

    char buffer[250];
    Slice result(buffer, sizeof(buffer));
    size_t bytes_read = 0;

    // Read at offset 500
    Status st = reader->read_at(500, result, &bytes_read, nullptr);

    EXPECT_TRUE(st.ok());
    EXPECT_EQ(250, bytes_read);

    // Verify the requests made
    ASSERT_EQ(3, requests.size());
    EXPECT_EQ(500, requests[0].first);  // offset
    EXPECT_EQ(100, requests[0].second); // length
    EXPECT_EQ(600, requests[1].first);
    EXPECT_EQ(100, requests[1].second);
    EXPECT_EQ(700, requests[2].first);
    EXPECT_EQ(50, requests[2].second);
}

// Test 7: Zero-byte read
TEST_F(BrokerFileReaderTest, ZeroByteRead) {
    config::max_chunk_size_for_broker = 100;

    // Should not call pread for zero-byte read
    EXPECT_CALL(*_mock_client, pread(testing::_, testing::_)).Times(0);

    auto connection = std::make_shared<BrokerServiceConnection>(
            reinterpret_cast<TPaloBrokerServiceClient*>(_mock_client.get()));
    
    auto reader = std::make_unique<BrokerFileReader>(
            _broker_addr, _path, _file_size, _fd, connection);

    char buffer[1];
    Slice result(buffer, 0);
    size_t bytes_read = 0;

    Status st = reader->read_at(0, result, &bytes_read, nullptr);

    EXPECT_TRUE(st.ok());
    EXPECT_EQ(0, bytes_read);
}

// Test 8: Large chunk size (boundary test)
TEST_F(BrokerFileReaderTest, LargeChunkSize) {
    config::max_chunk_size_for_broker = 1024 * 1024; // 1MB

    EXPECT_CALL(*_mock_client, pread(testing::_, testing::_))
            .WillOnce([this](TBrokerReadResponse& response,
                             const TBrokerPReadRequest& request) {
                setup_read_response(response, TBrokerOperationStatusCode::OK,
                                    std::string(request.length, 'L'));
            });

    auto connection = std::make_shared<BrokerServiceConnection>(
            reinterpret_cast<TPaloBrokerServiceClient*>(_mock_client.get()));
    
    auto reader = std::make_unique<BrokerFileReader>(
            _broker_addr, _path, _file_size, _fd, connection);

    char buffer[10000];
    Slice result(buffer, sizeof(buffer));
    size_t bytes_read = 0;

    Status st = reader->read_at(0, result, &bytes_read, nullptr);

    EXPECT_TRUE(st.ok());
    EXPECT_EQ(10000, bytes_read);
}

} // namespace doris::io