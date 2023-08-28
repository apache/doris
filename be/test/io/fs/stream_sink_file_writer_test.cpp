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
#include "olap/olap_common.h"
#include "util/debug/leakcheck_disabler.h"
#include "util/faststring.h"

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

    class StreamingSinkFileService : public PBackendService {
    public:
        StreamingSinkFileService() : _sd(brpc::INVALID_STREAM_ID) {}
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
            Status::OK().to_protobuf(response->mutable_status());
        }

    private:
        MockStreamSinkFileRecevier _receiver;
        brpc::StreamId _sd;
    };

public:
    StreamSinkFileWriterTest() { srand(time(nullptr)); }
    ~StreamSinkFileWriterTest() {}

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
        CHECK_EQ(0, channel.Init("127.0.0.1:18946", nullptr));

        // init server
        _stream_service = new StreamingSinkFileService();
        CHECK_EQ(0, _server.AddService(_stream_service, brpc::SERVER_DOESNT_OWN_SERVICE));
        brpc::ServerOptions server_options;
        server_options.idle_timeout_sec = FLAGS_idle_timeout_s;
        {
            debug::ScopedLeakCheckDisabler disable_lsan;
            CHECK_EQ(0, _server.Start("127.0.0.1:18946", &server_options));
        }

        // init stream connect
        PBackendService_Stub stub(&channel);
        brpc::Controller cntl;
        brpc::StreamId stream;
        CHECK_EQ(0, brpc::StreamCreate(&stream, cntl, NULL));

        POpenStreamSinkRequest request;
        POpenStreamSinkResponse response;
        request.mutable_load_id()->set_hi(1);
        request.mutable_load_id()->set_lo(1);
        stub.open_stream_sink(&cntl, &request, &response, NULL);

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
};

TEST_F(StreamSinkFileWriterTest, TestInit) {
    std::vector<brpc::StreamId> stream_ids {_stream};
    io::StreamSinkFileWriter writer(0, stream_ids);
    PUniqueId load_id;
    load_id.set_hi(1);
    load_id.set_lo(2);
    writer.init(load_id, 3, 4, 5, 6);
}

TEST_F(StreamSinkFileWriterTest, TestAppend) {
    std::vector<brpc::StreamId> stream_ids {_stream};
    io::StreamSinkFileWriter writer(0, stream_ids);
    PUniqueId load_id;
    load_id.set_hi(1);
    load_id.set_lo(2);
    writer.init(load_id, 3, 4, 5, 6);
    std::vector<Slice> slices {"hello"};
    CHECK_STATUS_OK(writer.appendv(&slices[0], slices.size()));
}

TEST_F(StreamSinkFileWriterTest, TestFinalize) {
    std::vector<brpc::StreamId> stream_ids {_stream};
    io::StreamSinkFileWriter writer(0, stream_ids);
    PUniqueId load_id;
    load_id.set_hi(1);
    load_id.set_lo(2);
    writer.init(load_id, 3, 4, 5, 6);
    CHECK_STATUS_OK(writer.finalize());
}
} // namespace doris
