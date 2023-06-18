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

#include "io/fs/stream_sink_file_writer.h"

#include <brpc/channel.h>
#include <brpc/server.h>

#include "gtest/gtest_pred_impl.h"

namespace doris {

#ifndef CHECK_STATUS_OK
#define CHECK_STATUS_OK(stmt)                   \
    do {                                        \
        Status _status_ = (stmt);               \
        ASSERT_TRUE(_status_.ok()) << _status_; \
    } while (false)
#endif

DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_int32(idle_timeout_s, -1,
             "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");

class StreamSinkFileWriterTest : public testing::Test {
    class MockStreamSinkFileRecevier : public brpc::StreamInputHandler {
    public:
        virtual int on_received_messages(brpc::StreamId id, butil::IOBuf* const messages[],
                                         size_t size) {
            std::stringstream str;
            for (size_t i = 0; i < size; ++i) {
                str << "msg[" << i << "]=" << *messages[i];
            }
            LOG(INFO) << "Received from Stream=" << id << ": " << str.str();
            return 0;
        }
        virtual void on_idle_timeout(brpc::StreamId id) {
            LOG(INFO) << "Stream=" << id << " has no data transmission for a while";
        }
        virtual void on_closed(brpc::StreamId id) { LOG(INFO) << "Stream=" << id << " is closed"; }
    };

    class StreamingSinkFileService : public PBackendService_Stub {
    public:
        StreamingSinkFileService(brpc::Channel* channel)
                : PBackendService_Stub(channel), _sd(brpc::INVALID_STREAM_ID) {}
        virtual ~StreamingSinkFileService() { brpc::StreamClose(_sd); };
        virtual void open_stream_sink(google::protobuf::RpcController* controller,
                                      const POpenStreamSinkRequest*,
                                      POpenStreamSinkResponse* response,
                                      google::protobuf::Closure* done) {
            brpc::ClosureGuard done_guard(done);

            brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
            brpc::StreamOptions stream_options;
            stream_options.handler = &_receiver;
            CHECK_EQ(0, brpc::StreamAccept(&_sd, *cntl, &stream_options));
            std::unique_ptr<PStatus> status = std::make_unique<PStatus>();
            status->set_status_code(0);
            response->set_allocated_status(status.get());
            response->release_status();
        }

        brpc::StreamId get_stream() { return _sd; }

    private:
        MockStreamSinkFileRecevier _receiver;
        brpc::StreamId _sd;
    };

public:
    StreamSinkFileWriterTest() {}
    ~StreamSinkFileWriterTest() {
        delete _data;
        delete[] _buffer;
    }

protected:
    virtual void SetUp() {
        // init channel
        brpc::Channel channel;
        brpc::ChannelOptions options;
        options.protocol = brpc::PROTOCOL_BAIDU_STD;
        options.connection_type = FLAGS_connection_type;
        options.timeout_ms = FLAGS_timeout_ms;
        options.max_retry = FLAGS_max_retry;
        std::stringstream port;
        while (true) {
            srand(time(nullptr));
            port << "0.0.0.0:" << (rand() % 1000 + 8000);
            if (channel.Init(port.str().c_str(), NULL) == 0) {
                break;
            }
            port.clear();
        }

        // init server
        _stream_service = new StreamingSinkFileService(&channel);
        CHECK_EQ(0, _server.AddService(_stream_service, brpc::SERVER_DOESNT_OWN_SERVICE));
        brpc::ServerOptions server_options;
        server_options.idle_timeout_sec = FLAGS_idle_timeout_s;
        CHECK_EQ(0, _server.Start(port.str().c_str(), &server_options));

        // init stream connect
        PBackendService_Stub stub(&channel);
        brpc::Controller cntl;
        brpc::StreamId stream;
        CHECK_EQ(0, brpc::StreamCreate(&stream, cntl, NULL));

        POpenStreamSinkRequest request;
        POpenStreamSinkResponse response;
        PUniqueId id;
        id.set_hi(1);
        id.set_lo(1);
        request.set_allocated_id(&id);
        stub.open_stream_sink(&cntl, &request, &response, NULL);

        request.release_id();
        brpc::Join(cntl.call_id());
        _stream = stream;
    }

    virtual void TearDown() {
        CHECK_EQ(0, brpc::StreamClose(_stream));
        CHECK_EQ(0, _server.Stop(1000));
        CHECK_EQ(0, _server.Join());
        delete _stream_service;
    }

    StreamingSinkFileService* _stream_service;
    brpc::StreamId _stream;
    brpc::Server _server;
    Slice* _data = new Slice();
    char* _buffer = new char[6];
};

TEST_F(StreamSinkFileWriterTest, TestInit) {
    io::StreamSinkFileWriter writer(_stream);
    PUniqueId load_id;
    load_id.set_hi(1);
    load_id.set_lo(1);
    CHECK_STATUS_OK(writer.init("test", load_id, 1, 1, 1, 1, true));
}

TEST_F(StreamSinkFileWriterTest, TestAppend) {
    io::StreamSinkFileWriter writer(_stream);
    PUniqueId load_id;
    load_id.set_hi(1);
    load_id.set_lo(1);
    CHECK_STATUS_OK(writer.init("test", load_id, 1, 1, 1, 1, true));
    strcpy(_buffer, "hello");
    _data->data = _buffer;
    CHECK_STATUS_OK(writer.appendv(_data, 6));
}

TEST_F(StreamSinkFileWriterTest, TestFinalize) {
    io::StreamSinkFileWriter writer(_stream);
    PUniqueId load_id;
    load_id.set_hi(1);
    load_id.set_lo(1);
    CHECK_STATUS_OK(writer.init("test", load_id, 1, 1, 1, 1, true));
    CHECK_STATUS_OK(writer.finalize());
}

} // namespace doris