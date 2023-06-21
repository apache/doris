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

#include <gen_cpp/Types_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <unistd.h>

#include "common/config.h"
#include "common/status.h"
#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/FrontendService_types.h"
#include "gtest/gtest_pred_impl.h"
#include "runtime/exec_env.h"
#include "runtime/sink_stream_mgr.h"
#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/channel.h>
#include <brpc/stream.h>
#include <brpc/server.h>
#include <gen_cpp/internal_service.pb.h>
#include <service/internal_service.h>
#include <functional>
#include <olap/storage_engine.h>

namespace doris {

static const uint32_t MAX_PATH_LEN = 1024;
StorageEngine* z_engine = nullptr;
static const std::string zTestDir = "./data_test/data/sink_stream_mgr_test";

class SinkStreamMgrTest: public testing::Test {
public:

    class Handler : public StreamInputHandler {
    public:
        int on_received_messages(StreamId id, butil::IOBuf* const messages[], size_t size) override {
            std::cerr << "on_received_messages" << std::endl;
            for (size_t i = 0; i < size; ++i) {
                    std::cerr << "message[" << i << "]: " << messages[i]->to_string() << std::endl;
            }
            return 0;
        }
        void on_idle_timeout(StreamId id) override {
            std::cerr << "on_idle_timeout" << std::endl;
        }
        void on_closed(StreamId id) override {
            std::cerr << "on_closed" << std::endl;
        }
    };

    class MockSinkClient {
    public:
        MockSinkClient() = default;
        ~MockSinkClient() = default;

        class MockClosure : public google::protobuf::Closure {
        public:
            MockClosure(std::function<void ()> cb) : _cb(cb) {}
            void Run() override {
                _cb();
                delete this;
            }
        private:
            std::function<void ()> _cb;
        };

        Status connect_stream() {
            brpc::Channel channel;
            std::cerr << "connect_stream" << std::endl;
            // Initialize the channel, NULL means using default options.
            brpc::ChannelOptions options;
            options.protocol = brpc::PROTOCOL_BAIDU_STD;
            options.connection_type = "single";
            options.timeout_ms = 10000/*milliseconds*/;
            options.max_retry = 3;
            CHECK_EQ(0, channel.Init("127.0.0.1:18947", nullptr));

            // Normally, you should not call a Channel directly, but instead construct
            // a stub Service wrapping it. stub can be shared by all threads as well.
            PBackendService_Stub stub(&channel);

            _stream_options.handler = &_handler;
            if (brpc::StreamCreate(&_stream, _cntl, &_stream_options) != 0) {
                LOG(ERROR) << "Fail to create stream";
                return Status::InternalError("Fail to create stream");
            }

            POpenStreamSinkRequest* request = new POpenStreamSinkRequest();
            POpenStreamSinkResponse* response = new POpenStreamSinkResponse();
            std::shared_ptr<PUniqueId> id = std::make_shared<PUniqueId>();
            id->set_hi(1);
            id->set_lo(1);
            request->set_allocated_id(id.get());
            stub.open_stream_sink(&_cntl, request, response, nullptr);
            request->release_id();
            delete request;
            delete response;
            if (_cntl.Failed()) {
                std::cerr << "open_stream_sink failed" << std::endl;
                LOG(ERROR) << "Fail to open stream sink";
                return Status::InternalError("Fail to open stream sink");
            }

            return Status::OK();
        }

        StreamId get_stream_id() {
            return _stream;
        }

        void disconnect() {
            std::cerr << "disconnect" << std::endl;
            CHECK_EQ(0, brpc::StreamClose(_stream));
        }

        Status send(butil::IOBuf* buf) {
            int ret = brpc::StreamWrite(_stream, *buf);
            if (ret != 0) {
                LOG(ERROR) << "Fail to write stream";
                return Status::InternalError("Fail to write stream");
            }
            std::cerr << "sent by stream successfully" << std::endl;
            return Status::OK();
        }

        Status close() {
            return Status::OK();
        }
    private:
        brpc::StreamId _stream;
        brpc::Controller _cntl;
        brpc::StreamOptions _stream_options;
        Handler _handler;
    };

    SinkStreamMgrTest() = default;

    void SetUp() override {
        srand(time(nullptr));
        config::sink_stream_pool_thread_num_min = 20;
        config::sink_stream_pool_thread_num_max = 20;
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        config::storage_root_path = std::string(buffer) + "/data_test";

        EXPECT_TRUE(io::global_local_filesystem()
                            ->delete_and_create_directory(config::storage_root_path)
                            .ok());

        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path, -1);

        doris::EngineOptions options;
        options.store_paths = paths;
        Status s = doris::StorageEngine::open(options, &z_engine);
        EXPECT_TRUE(s.ok()) << s.to_string();

        _env = doris::ExecEnv::GetInstance();
        _env->set_storage_engine(z_engine);

        EXPECT_TRUE(io::global_local_filesystem()->create_directory(zTestDir).ok());

        z_engine->start_bg_threads();

        _internal_service = new PInternalServiceImpl(_env);
        CHECK_EQ(0, _server.AddService(_internal_service, brpc::SERVER_DOESNT_OWN_SERVICE));
        brpc::ServerOptions server_options;
        server_options.idle_timeout_sec = 300;
        CHECK_EQ(0, _server.Start("127.0.0.1:18947", &server_options)); // TODO: make port random
    }

    void TearDown() override {
        if (z_engine != nullptr) {
            z_engine->stop();
            delete z_engine;
            z_engine = nullptr;
        }
        _server.Stop(1000);
        CHECK_EQ(0, _server.Join());
        delete _internal_service;
    }

    ExecEnv* _env;
    brpc::Server _server;
    PInternalServiceImpl*  _internal_service;
};

TEST_F(SinkStreamMgrTest, open_append_close_file_twice) {
    //SinkStreamMgr* stream_mgr = _env.get_sink_stream_mgr();
    //StreamIdPtr stream = stream_mgr->get_free_stream_id();

    MockSinkClient client;
    client.connect_stream();
    StreamId stream = client.get_stream_id();
    CHECK_NE(stream, INVALID_STREAM_ID);
    std::cerr << "stream id = " << stream << std::endl;

    std::stringstream path1;
    path1 << zTestDir << "/" << std::to_string(1047);

    /************** OPEN FILE **************/
    {
        std::cerr << "openfile" << std::endl;
        butil::IOBuf open_buf;
        PStreamHeader header;
        header.set_opcode(PStreamHeader::OPEN_FILE);
        std::shared_ptr<PUniqueId> loadid = std::make_shared<PUniqueId>();
        loadid->set_hi(1);
        loadid->set_lo(1);
        header.set_allocated_load_id(loadid.get());
        header.set_index_id(2);
        header.set_tablet_id(3);
        header.set_segment_id(4);
        header.set_is_last_segment(false);
        size_t hdr_len = header.ByteSizeLong();
        std::cerr << "on client side: hdr_len = " << hdr_len << std::endl;
        open_buf.append((char*)&hdr_len, sizeof(size_t));
        open_buf.append(header.SerializeAsString());
        open_buf.append(path1.str());
        client.send(&open_buf);
        sleep(2);
        header.release_load_id();
        CHECK_EQ(true, std::filesystem::exists(path1.str()));
    }

    /************* APPEND FILE *************/
    {
        butil::IOBuf append_buf;
        PStreamHeader header;
        std::string data = "file1 hello world 123 !@#$%^&*()_+";
        header.set_opcode(PStreamHeader::APPEND_DATA);
        std::shared_ptr<PUniqueId> loadid = std::make_shared<PUniqueId>();
        loadid->set_hi(1);
        loadid->set_lo(1);
        header.set_allocated_load_id(loadid.get());
        header.set_index_id(2);
        header.set_tablet_id(3);
        header.set_segment_id(4);
        header.set_is_last_segment(false);
        size_t hdr_len = header.ByteSizeLong();
        append_buf.append((char*)&hdr_len, sizeof(size_t));
        append_buf.append(header.SerializeAsString());
        append_buf.append(data);
        client.send(&append_buf);
        header.release_load_id();
        sleep(2);
    }

    /************* CLOSE FILE **************/
    {
        butil::IOBuf close_buf;
        PStreamHeader header;
        header.set_opcode(PStreamHeader::CLOSE_FILE);
        std::shared_ptr<PUniqueId> loadid = std::make_shared<PUniqueId>();
        loadid->set_hi(1);
        loadid->set_lo(1);
        header.set_allocated_load_id(loadid.get());
        header.set_index_id(2);
        header.set_tablet_id(3);
        header.set_segment_id(4);
        header.set_is_last_segment(false);
        size_t hdr_len = header.ByteSizeLong();
        close_buf.append((char*)&hdr_len, sizeof(size_t));
        close_buf.append(header.SerializeAsString());
        client.send(&close_buf);
        sleep(2);
        std::ifstream ifs(path1.str());
        std::string content((std::istreambuf_iterator<char>(ifs)),
                            (std::istreambuf_iterator<char>()));
        std::string data = "file1 hello world 123 !@#$%^&*()_+";
        CHECK_EQ(content, data);
        header.release_load_id();
    }

    /****************************************/
    /**** DO IT AGAIN (AS LAST SEGMENT)  ****/
    /****************************************/
    std::stringstream path2;
    path2 << zTestDir << "/" << std::to_string(1048);

    /************** OPEN FILE **************/
    {
        std::cerr << "openfile" << std::endl;
        butil::IOBuf open_buf;
        PStreamHeader header;
        header.set_opcode(PStreamHeader::OPEN_FILE);
        std::shared_ptr<PUniqueId> loadid = std::make_shared<PUniqueId>();
        loadid->set_hi(1);
        loadid->set_lo(1);
        header.set_allocated_load_id(loadid.get());
        header.set_index_id(2);
        header.set_tablet_id(3);
        header.set_segment_id(4);
        header.set_is_last_segment(false);
        size_t hdr_len = header.ByteSizeLong();
        std::cerr << "on client side: hdr_len = " << hdr_len << std::endl;
        open_buf.append((char*)&hdr_len, sizeof(size_t));
        open_buf.append(header.SerializeAsString());
        open_buf.append(path2.str());
        client.send(&open_buf);
        sleep(2);
        CHECK_EQ(true, std::filesystem::exists(path2.str()));
        header.release_load_id();
    }

    /************* APPEND FILE *************/
    {
        butil::IOBuf append_buf;
        PStreamHeader header;
        std::string data = "file2 hello world 123 !@#$%^&*()_+";
        header.set_opcode(PStreamHeader::APPEND_DATA);
        std::shared_ptr<PUniqueId> loadid = std::make_shared<PUniqueId>();
        loadid->set_hi(1);
        loadid->set_lo(1);
        header.set_allocated_load_id(loadid.get());
        header.set_index_id(2);
        header.set_tablet_id(3);
        header.set_segment_id(4);
        header.set_is_last_segment(false);
        size_t hdr_len = header.ByteSizeLong();
        append_buf.append((char*)&hdr_len, sizeof(size_t));
        append_buf.append(header.SerializeAsString());
        append_buf.append(data);
        client.send(&append_buf);
        sleep(2);
        header.release_load_id();
    }

    /************* CLOSE FILE **************/
    {
        butil::IOBuf close_buf;
        PStreamHeader header;
        header.set_opcode(PStreamHeader::CLOSE_FILE);
        std::shared_ptr<PUniqueId> loadid = std::make_shared<PUniqueId>();
        loadid->set_hi(1);
        loadid->set_lo(1);
        header.set_allocated_load_id(loadid.get());
        header.set_index_id(2);
        header.set_tablet_id(3);
        header.set_segment_id(4);
        header.set_is_last_segment(true); // change it to true when last segment
        header.set_allocated_rowset_meta(new RowsetMetaPB()); // last segment has rowset meta
        int64_t rowset_id = 1;
        header.mutable_rowset_meta()->set_rowset_id(rowset_id);
        size_t hdr_len = header.ByteSizeLong();
        close_buf.append((char*)&hdr_len, sizeof(size_t));
        close_buf.append(header.SerializeAsString());
        client.send(&close_buf);
        sleep(2);
        std::ifstream ifs(path2.str());
        std::string content((std::istreambuf_iterator<char>(ifs)),
                            (std::istreambuf_iterator<char>()));
        std::string data = "file2 hello world 123 !@#$%^&*()_+";
        CHECK_EQ(content, data);
        header.release_load_id();
        header.release_rowset_meta();
        // CHECK
    }

    sleep(2);
    client.disconnect();
}

} // namespace doris
