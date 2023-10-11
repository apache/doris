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

#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/stream.h>
#include <butil/logging.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gflags/gflags.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <olap/storage_engine.h>
#include <service/internal_service.h>
#include <unistd.h>

#include <functional>
#include <memory>

#include "common/config.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/FrontendService_types.h"
#include "gtest/gtest_pred_impl.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/tablet_manager.h"
#include "olap/txn_manager.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/load_stream_mgr.h"
#include "util/debug/leakcheck_disabler.h"
#include "util/runtime_profile.h"

using namespace brpc;

namespace doris {

static const uint32_t MAX_PATH_LEN = 1024;
static std::unique_ptr<StorageEngine> k_engine;
static const std::string zTestDir = "./data_test/data/load_stream_mgr_test";

const int64_t NORMAL_TABLET_ID = 10000;
const int64_t ABNORMAL_TABLET_ID = 40000;
const int64_t NORMAL_INDEX_ID = 50000;
const int64_t ABNORMAL_INDEX_ID = 60000;
const int64_t NORMAL_PARTITION_ID = 50000;
const int64_t SCHEMA_HASH = 90000;
const uint32_t NORMAL_SENDER_ID = 0;
const uint32_t ABNORMAL_SENDER_ID = 10000;
const int64_t NORMAL_TXN_ID = 600001;
const UniqueId NORMAL_LOAD_ID(1, 1);
const UniqueId ABNORMAL_LOAD_ID(1, 0);
std::string ABNORMAL_STRING("abnormal");

void construct_schema(OlapTableSchemaParam* schema) {
    // construct schema
    TOlapTableSchemaParam tschema;
    tschema.db_id = 1;
    tschema.table_id = 2;
    tschema.version = 0;

    // descriptor
    {
        TDescriptorTableBuilder dtb;
        {
            TTupleDescriptorBuilder tuple_builder;

            tuple_builder.add_slot(TSlotDescriptorBuilder()
                                           .type(TYPE_INT)
                                           .column_name("c1")
                                           .column_pos(1)
                                           .build());
            tuple_builder.add_slot(TSlotDescriptorBuilder()
                                           .type(TYPE_BIGINT)
                                           .column_name("c2")
                                           .column_pos(2)
                                           .build());
            tuple_builder.add_slot(TSlotDescriptorBuilder()
                                           .string_type(10)
                                           .column_name("c3")
                                           .column_pos(3)
                                           .build());

            tuple_builder.build(&dtb);
        }
        {
            TTupleDescriptorBuilder tuple_builder;

            tuple_builder.add_slot(TSlotDescriptorBuilder()
                                           .type(TYPE_INT)
                                           .column_name("c1")
                                           .column_pos(1)
                                           .build());
            tuple_builder.add_slot(TSlotDescriptorBuilder()
                                           .type(TYPE_BIGINT)
                                           .column_name("c2")
                                           .column_pos(2)
                                           .build());
            tuple_builder.add_slot(TSlotDescriptorBuilder()
                                           .string_type(20)
                                           .column_name("c3")
                                           .column_pos(3)
                                           .build());

            tuple_builder.build(&dtb);
        }

        auto desc_tbl = dtb.desc_tbl();
        tschema.slot_descs = desc_tbl.slotDescriptors;
        tschema.tuple_desc = desc_tbl.tupleDescriptors[0];
    }
    // index
    tschema.indexes.resize(2);
    tschema.indexes[0].id = NORMAL_INDEX_ID;
    tschema.indexes[0].columns = {"c1", "c2", "c3"};

    tschema.indexes[1].id = NORMAL_INDEX_ID + 1;
    tschema.indexes[1].columns = {"c1", "c2", "c3"};

    static_cast<void>(schema->init(tschema));
}

// copied from delta_writer_test.cpp
static void create_tablet_request(int64_t tablet_id, int32_t schema_hash,
                                  TCreateTabletReq* request) {
    request->tablet_id = tablet_id;
    request->__set_version(1);
    request->tablet_schema.schema_hash = schema_hash;
    request->tablet_schema.short_key_column_count = 6;
    request->tablet_schema.keys_type = TKeysType::AGG_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;
    request->__set_storage_format(TStorageFormat::V2);

    TColumn k1;

    k1.__set_is_key(true);
    k1.column_type.type = TPrimitiveType::TINYINT;
    request->tablet_schema.columns.push_back(k1);

    TColumn k2;
    k2.column_name = "k2";
    k2.__set_is_key(true);
    k2.column_type.type = TPrimitiveType::SMALLINT;
    request->tablet_schema.columns.push_back(k2);

    TColumn k3;
    k3.column_name = "k3";
    k3.__set_is_key(true);
    k3.column_type.type = TPrimitiveType::INT;
    request->tablet_schema.columns.push_back(k3);

    TColumn k4;
    k4.column_name = "k4";
    k4.__set_is_key(true);
    k4.column_type.type = TPrimitiveType::BIGINT;
    request->tablet_schema.columns.push_back(k4);

    TColumn k5;
    k5.column_name = "k5";
    k5.__set_is_key(true);
    k5.column_type.type = TPrimitiveType::LARGEINT;
    request->tablet_schema.columns.push_back(k5);

    TColumn k6;
    k6.column_name = "k6";
    k6.__set_is_key(true);
    k6.column_type.type = TPrimitiveType::DATE;
    request->tablet_schema.columns.push_back(k6);

    TColumn k7;
    k7.column_name = "k7";
    k7.__set_is_key(true);
    k7.column_type.type = TPrimitiveType::DATETIME;
    request->tablet_schema.columns.push_back(k7);

    TColumn k8;
    k8.column_name = "k8";
    k8.__set_is_key(true);
    k8.column_type.type = TPrimitiveType::CHAR;
    k8.column_type.__set_len(4);
    request->tablet_schema.columns.push_back(k8);

    TColumn k9;
    k9.column_name = "k9";
    k9.__set_is_key(true);
    k9.column_type.type = TPrimitiveType::VARCHAR;
    k9.column_type.__set_len(65);
    request->tablet_schema.columns.push_back(k9);

    TColumn k10;
    k10.column_name = "k10";
    k10.__set_is_key(true);
    k10.column_type.type = TPrimitiveType::DECIMALV2;
    k10.column_type.__set_precision(6);
    k10.column_type.__set_scale(3);
    request->tablet_schema.columns.push_back(k10);

    TColumn k11;
    k11.column_name = "k11";
    k11.__set_is_key(true);
    k11.column_type.type = TPrimitiveType::DATEV2;
    request->tablet_schema.columns.push_back(k11);

    TColumn v1;
    v1.column_name = "v1";
    v1.__set_is_key(false);
    v1.column_type.type = TPrimitiveType::TINYINT;
    v1.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v1);

    TColumn v2;
    v2.column_name = "v2";
    v2.__set_is_key(false);
    v2.column_type.type = TPrimitiveType::SMALLINT;
    v2.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v2);

    TColumn v3;
    v3.column_name = "v3";
    v3.__set_is_key(false);
    v3.column_type.type = TPrimitiveType::INT;
    v3.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v3);

    TColumn v4;
    v4.column_name = "v4";
    v4.__set_is_key(false);
    v4.column_type.type = TPrimitiveType::BIGINT;
    v4.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v4);

    TColumn v5;
    v5.column_name = "v5";
    v5.__set_is_key(false);
    v5.column_type.type = TPrimitiveType::LARGEINT;
    v5.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v5);

    TColumn v6;
    v6.column_name = "v6";
    v6.__set_is_key(false);
    v6.column_type.type = TPrimitiveType::DATE;
    v6.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v6);

    TColumn v7;
    v7.column_name = "v7";
    v7.__set_is_key(false);
    v7.column_type.type = TPrimitiveType::DATETIME;
    v7.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v7);

    TColumn v8;
    v8.column_name = "v8";
    v8.__set_is_key(false);
    v8.column_type.type = TPrimitiveType::CHAR;
    v8.column_type.__set_len(4);
    v8.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v8);

    TColumn v9;
    v9.column_name = "v9";
    v9.__set_is_key(false);
    v9.column_type.type = TPrimitiveType::VARCHAR;
    v9.column_type.__set_len(65);
    v9.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v9);

    TColumn v10;
    v10.column_name = "v10";
    v10.__set_is_key(false);
    v10.column_type.type = TPrimitiveType::DECIMALV2;
    v10.column_type.__set_precision(6);
    v10.column_type.__set_scale(3);
    v10.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v10);

    TColumn v11;
    v11.column_name = "v11";
    v11.__set_is_key(false);
    v11.column_type.type = TPrimitiveType::DATEV2;
    v11.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v11);
}

struct ResponseStat {
    std::atomic<int32_t> num;
    std::vector<int64_t> success_tablet_ids;
    std::vector<int64_t> failed_tablet_ids;
};
bthread::Mutex g_stat_lock;
static ResponseStat g_response_stat;

void reset_response_stat() {
    std::lock_guard lock_guard(g_stat_lock);
    g_response_stat.num = 0;
    g_response_stat.success_tablet_ids.clear();
    g_response_stat.failed_tablet_ids.clear();
}

class LoadStreamMgrTest : public testing::Test {
public:
    class Handler : public brpc::StreamInputHandler {
    public:
        int on_received_messages(StreamId id, butil::IOBuf* const messages[],
                                 size_t size) override {
            for (size_t i = 0; i < size; i++) {
                PWriteStreamSinkResponse response;
                butil::IOBufAsZeroCopyInputStream wrapper(*messages[i]);
                response.ParseFromZeroCopyStream(&wrapper);
                LOG(INFO) << "response " << response.DebugString();
                std::lock_guard lock_guard(g_stat_lock);
                for (auto& id : response.success_tablet_ids()) {
                    g_response_stat.success_tablet_ids.push_back(id);
                }
                for (auto& id : response.failed_tablet_ids()) {
                    g_response_stat.failed_tablet_ids.push_back(id);
                }
                g_response_stat.num++;
            }

            return 0;
        }
        void on_idle_timeout(StreamId id) override { std::cerr << "on_idle_timeout" << std::endl; }
        void on_closed(StreamId id) override { std::cerr << "on_closed" << std::endl; }
    };

    class StreamService : public PBackendService {
    public:
        StreamService(LoadStreamMgr* load_stream_mgr)
                : _sd(brpc::INVALID_STREAM_ID), _load_stream_mgr(load_stream_mgr) {}
        virtual ~StreamService() { brpc::StreamClose(_sd); };
        virtual void open_stream_sink(google::protobuf::RpcController* controller,
                                      const POpenStreamSinkRequest* request,
                                      POpenStreamSinkResponse* response,
                                      google::protobuf::Closure* done) {
            brpc::ClosureGuard done_guard(done);
            std::unique_ptr<PStatus> status = std::make_unique<PStatus>();
            brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
            brpc::StreamOptions stream_options;

            for (const auto& req : request->tablets()) {
                TabletManager* tablet_mgr = StorageEngine::instance()->tablet_manager();
                TabletSharedPtr tablet = tablet_mgr->get_tablet(req.tablet_id());
                if (tablet == nullptr) {
                    cntl->SetFailed("Tablet not found");
                    status->set_status_code(TStatusCode::NOT_FOUND);
                    response->set_allocated_status(status.get());
                    static_cast<void>(response->release_status());
                    return;
                }
                auto resp = response->add_tablet_schemas();
                resp->set_index_id(req.index_id());
                resp->set_enable_unique_key_merge_on_write(
                        tablet->enable_unique_key_merge_on_write());
                tablet->tablet_schema()->to_schema_pb(resp->mutable_tablet_schema());
            }

            LoadStreamSharedPtr load_stream;
            auto st = _load_stream_mgr->open_load_stream(request, load_stream);

            stream_options.handler = load_stream.get();

            StreamId streamid;
            if (brpc::StreamAccept(&streamid, *cntl, &stream_options) != 0) {
                cntl->SetFailed("Fail to accept stream");
                status->set_status_code(TStatusCode::CANCELLED);
                response->set_allocated_status(status.get());
                static_cast<void>(response->release_status());
                return;
            }

            load_stream->add_rpc_stream();

            status->set_status_code(TStatusCode::OK);
            response->set_allocated_status(status.get());
            static_cast<void>(response->release_status());
        }

    private:
        Handler _receiver;
        brpc::StreamId _sd;
        LoadStreamMgr* _load_stream_mgr = nullptr;
    };

    class MockSinkClient {
    public:
        MockSinkClient() = default;
        ~MockSinkClient() { disconnect(); }

        class MockClosure : public google::protobuf::Closure {
        public:
            MockClosure(std::function<void()> cb) : _cb(cb) {}
            void Run() override {
                _cb();
                delete this;
            }

        private:
            std::function<void()> _cb;
        };

        Status connect_stream(int64_t sender_id = NORMAL_SENDER_ID) {
            brpc::Channel channel;
            std::cerr << "connect_stream" << std::endl;
            // Initialize the channel, NULL means using default options.
            brpc::ChannelOptions options;
            options.protocol = brpc::PROTOCOL_BAIDU_STD;
            options.connection_type = "single";
            options.timeout_ms = 10000 /*milliseconds*/;
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

            POpenStreamSinkRequest request;
            POpenStreamSinkResponse response;
            PUniqueId id;
            id.set_hi(1);
            id.set_lo(1);

            OlapTableSchemaParam param;
            construct_schema(&param);
            *request.mutable_schema() = *param.to_protobuf();
            *request.mutable_load_id() = id;
            request.set_txn_id(NORMAL_TXN_ID);
            request.set_src_id(sender_id);
            auto ptablet = request.add_tablets();
            ptablet->set_tablet_id(NORMAL_TABLET_ID);
            ptablet->set_index_id(NORMAL_INDEX_ID);
            stub.open_stream_sink(&_cntl, &request, &response, nullptr);
            if (_cntl.Failed()) {
                std::cerr << "open_stream_sink failed" << std::endl;
                LOG(ERROR) << "Fail to open stream sink " << _cntl.ErrorText();
                return Status::InternalError("Fail to open stream sink");
            }

            return Status::OK();
        }

        void disconnect() const {
            std::cerr << "disconnect" << std::endl;
            CHECK_EQ(0, brpc::StreamClose(_stream));
        }

        Status send(butil::IOBuf* buf) {
            int ret = brpc::StreamWrite(_stream, *buf);
            if (ret != 0) {
                LOG(ERROR) << "Fail to write stream";
                return Status::InternalError("Fail to write stream");
            }
            LOG(INFO) << "sent by stream successfully" << std::endl;
            return Status::OK();
        }

        Status close() { return Status::OK(); }

    private:
        brpc::StreamId _stream;
        brpc::Controller _cntl;
        brpc::StreamOptions _stream_options;
        Handler _handler;
    };

    LoadStreamMgrTest()
            : _heavy_work_pool(4, 32, "load_stream_test_heavy"),
              _light_work_pool(4, 32, "load_stream_test_light") {}

    void close_load(MockSinkClient& client, uint32_t sender_id) {
        butil::IOBuf append_buf;
        PStreamHeader header;
        header.mutable_load_id()->set_hi(1);
        header.mutable_load_id()->set_lo(1);
        header.set_opcode(PStreamHeader::CLOSE_LOAD);
        header.set_src_id(sender_id);
        /* TODO: fix test with tablets_to_commit 
        PTabletID* tablets_to_commit = header.add_tablets_to_commit();
        tablets_to_commit->set_partition_id(NORMAL_PARTITION_ID);
        tablets_to_commit->set_index_id(NORMAL_INDEX_ID);
        tablets_to_commit->set_tablet_id(NORMAL_TABLET_ID);
        */
        size_t hdr_len = header.ByteSizeLong();
        append_buf.append((char*)&hdr_len, sizeof(size_t));
        append_buf.append(header.SerializeAsString());
        static_cast<void>(client.send(&append_buf));
    }

    void write_one_tablet(MockSinkClient& client, UniqueId load_id, uint32_t sender_id,
                          int64_t index_id, int64_t tablet_id, uint32_t segid, std::string& data,
                          bool segment_eos) {
        // append data
        butil::IOBuf append_buf;
        PStreamHeader header;
        header.set_opcode(PStreamHeader::APPEND_DATA);
        header.mutable_load_id()->set_hi(load_id.hi);
        header.mutable_load_id()->set_lo(load_id.lo);
        header.set_index_id(index_id);
        header.set_tablet_id(tablet_id);
        header.set_segment_id(segid);
        header.set_segment_eos(segment_eos);
        header.set_src_id(sender_id);
        header.set_partition_id(NORMAL_PARTITION_ID);
        size_t hdr_len = header.ByteSizeLong();
        append_buf.append((char*)&hdr_len, sizeof(size_t));
        append_buf.append(header.SerializeAsString());
        size_t data_len = data.length();
        append_buf.append((char*)&data_len, sizeof(size_t));
        append_buf.append(data);
        LOG(INFO) << "send " << header.DebugString() << data;
        static_cast<void>(client.send(&append_buf));
    }

    void write_abnormal_load(MockSinkClient& client) {
        write_one_tablet(client, ABNORMAL_LOAD_ID, NORMAL_SENDER_ID, NORMAL_INDEX_ID,
                         NORMAL_TABLET_ID, 0, ABNORMAL_STRING, true);
    }

    void write_abnormal_index(MockSinkClient& client) {
        write_one_tablet(client, NORMAL_LOAD_ID, NORMAL_SENDER_ID, ABNORMAL_INDEX_ID,
                         NORMAL_TABLET_ID, 0, ABNORMAL_STRING, true);
    }

    void write_abnormal_sender(MockSinkClient& client) {
        write_one_tablet(client, NORMAL_LOAD_ID, ABNORMAL_SENDER_ID, NORMAL_INDEX_ID,
                         NORMAL_TABLET_ID, 0, ABNORMAL_STRING, true);
    }

    void write_abnormal_tablet(MockSinkClient& client) {
        write_one_tablet(client, NORMAL_LOAD_ID, NORMAL_SENDER_ID, NORMAL_INDEX_ID,
                         ABNORMAL_TABLET_ID, 0, ABNORMAL_STRING, true);
    }

    void wait_for_ack(int32_t num) {
        for (int i = 0; i < 1000 && g_response_stat.num < num; i++) {
            LOG(INFO) << "waiting ack, current " << g_response_stat.num << ", expected " << num;
            bthread_usleep(1000);
        }
    }

    void wait_for_close() {
        for (int i = 0; i < 1000 && _load_stream_mgr->get_load_stream_num() != 0; i++) {
            bthread_usleep(1000);
        }
    }

    void SetUp() override {
        _server = new brpc::Server();
        srand(time(nullptr));
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
        k_engine = std::make_unique<StorageEngine>(options);
        Status s = k_engine->open();
        EXPECT_TRUE(s.ok()) << s.to_string();
        doris::ExecEnv::GetInstance()->set_storage_engine(k_engine.get());

        EXPECT_TRUE(io::global_local_filesystem()->create_directory(zTestDir).ok());

        static_cast<void>(k_engine->start_bg_threads());

        _load_stream_mgr = std::make_unique<LoadStreamMgr>(4, &_heavy_work_pool, &_light_work_pool);
        _stream_service = new StreamService(_load_stream_mgr.get());
        CHECK_EQ(0, _server->AddService(_stream_service, brpc::SERVER_OWNS_SERVICE));
        brpc::ServerOptions server_options;
        server_options.idle_timeout_sec = 300;
        {
            debug::ScopedLeakCheckDisabler disable_lsan;
            CHECK_EQ(0, _server->Start("127.0.0.1:18947", &server_options));
        }

        for (int i = 0; i < 3; i++) {
            TCreateTabletReq request;
            create_tablet_request(NORMAL_TABLET_ID + i, SCHEMA_HASH, &request);
            auto profile = std::make_unique<RuntimeProfile>("test");
            Status res = k_engine->create_tablet(request, profile.get());
            EXPECT_EQ(Status::OK(), res);
        }
    }

    void TearDown() override {
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        k_engine.reset();
        _server->Stop(1000);
        _load_stream_mgr.reset();
        CHECK_EQ(0, _server->Join());
        SAFE_DELETE(_server);
    }

    std::string read_data(int64_t txn_id, int64_t partition_id, int64_t tablet_id, uint32_t segid) {
        auto tablet = k_engine->tablet_manager()->get_tablet(tablet_id);
        std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
        k_engine->txn_manager()->get_txn_related_tablets(txn_id, partition_id, &tablet_related_rs);
        LOG(INFO) << "get txn related tablet, txn_id=" << txn_id << ", tablet_id=" << tablet_id
                  << "partition_id=" << partition_id;
        for (auto& [tablet, rowset] : tablet_related_rs) {
            if (tablet.tablet_id != tablet_id || rowset == nullptr) {
                continue;
            }
            auto path = static_cast<BetaRowset*>(rowset.get())->segment_file_path(segid);
            LOG(INFO) << "read data from " << path;
            std::ifstream inputFile(path, std::ios::binary);
            inputFile.seekg(0, std::ios::end);
            std::streampos file_size = inputFile.tellg();
            inputFile.seekg(0, std::ios::beg);

            // Read the file contents
            std::unique_ptr<char[]> buffer = std::make_unique<char[]>(file_size);
            inputFile.read(buffer.get(), file_size);
            return std::string(buffer.get(), file_size);
        }
        return std::string();
    }

    ExecEnv* _env;
    brpc::Server* _server;
    StreamService* _stream_service;

    FifoThreadPool _heavy_work_pool;
    FifoThreadPool _light_work_pool;

    std::unique_ptr<LoadStreamMgr> _load_stream_mgr;
};

// <client, index, bucket>
// one client
TEST_F(LoadStreamMgrTest, one_client_abnormal_load) {
    MockSinkClient client;
    auto st = client.connect_stream();
    EXPECT_TRUE(st.ok());

    write_abnormal_load(client);
    // TODO check abnormal load id

    reset_response_stat();
    close_load(client, 1);
    wait_for_ack(1);
    EXPECT_EQ(g_response_stat.num, 1);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(_load_stream_mgr->get_load_stream_num(), 1);

    close_load(client, 0);
    wait_for_ack(2);
    EXPECT_EQ(g_response_stat.num, 2);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 1);
    EXPECT_EQ(g_response_stat.success_tablet_ids[0], NORMAL_TABLET_ID);

    // server will close stream on CLOSE_LOAD
    wait_for_close();
    EXPECT_EQ(_load_stream_mgr->get_load_stream_num(), 0);
}

TEST_F(LoadStreamMgrTest, one_client_abnormal_index) {
    MockSinkClient client;
    auto st = client.connect_stream();
    EXPECT_TRUE(st.ok());

    reset_response_stat();
    write_abnormal_index(client);
    wait_for_ack(1);
    EXPECT_EQ(g_response_stat.num, 1);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 1);
    EXPECT_EQ(g_response_stat.failed_tablet_ids[0], NORMAL_TABLET_ID);

    close_load(client, 1);
    wait_for_ack(2);
    EXPECT_EQ(_load_stream_mgr->get_load_stream_num(), 1);
    EXPECT_EQ(g_response_stat.num, 2);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 1);

    close_load(client, 0);
    wait_for_ack(3);
    EXPECT_EQ(g_response_stat.num, 3);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 1);

    // server will close stream on CLOSE_LOAD
    wait_for_close();
    EXPECT_EQ(_load_stream_mgr->get_load_stream_num(), 0);
}

TEST_F(LoadStreamMgrTest, one_client_abnormal_sender) {
    MockSinkClient client;
    auto st = client.connect_stream();
    EXPECT_TRUE(st.ok());

    reset_response_stat();
    write_abnormal_sender(client);
    wait_for_ack(1);
    EXPECT_EQ(g_response_stat.num, 1);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 1);
    EXPECT_EQ(g_response_stat.failed_tablet_ids[0], NORMAL_TABLET_ID);

    close_load(client, 1);
    wait_for_ack(2);
    EXPECT_EQ(g_response_stat.num, 2);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 1);

    close_load(client, 0);
    wait_for_ack(3);
    EXPECT_EQ(g_response_stat.num, 3);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 1);

    // server will close stream on CLOSE_LOAD
    wait_for_close();
    EXPECT_EQ(_load_stream_mgr->get_load_stream_num(), 0);
}

TEST_F(LoadStreamMgrTest, one_client_abnormal_tablet) {
    MockSinkClient client;
    auto st = client.connect_stream();
    EXPECT_TRUE(st.ok());

    reset_response_stat();
    write_abnormal_tablet(client);
    wait_for_ack(1);
    EXPECT_EQ(g_response_stat.num, 1);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 1);
    EXPECT_EQ(g_response_stat.failed_tablet_ids[0], ABNORMAL_TABLET_ID);

    close_load(client, 1);
    wait_for_ack(2);
    EXPECT_EQ(g_response_stat.num, 2);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 1);

    close_load(client, 0);
    wait_for_ack(3);
    EXPECT_EQ(g_response_stat.num, 3);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 1);

    // server will close stream on CLOSE_LOAD
    wait_for_close();
    EXPECT_EQ(_load_stream_mgr->get_load_stream_num(), 0);
}

TEST_F(LoadStreamMgrTest, one_client_one_index_one_tablet_single_segment0_zero_bytes) {
    MockSinkClient client;
    auto st = client.connect_stream();
    EXPECT_TRUE(st.ok());

    reset_response_stat();

    // append data
    butil::IOBuf append_buf;
    PStreamHeader header;
    std::string data;
    write_one_tablet(client, NORMAL_LOAD_ID, NORMAL_SENDER_ID, NORMAL_INDEX_ID, NORMAL_TABLET_ID, 0,
                     data, true);

    EXPECT_EQ(g_response_stat.num, 0);
    // CLOSE_LOAD
    close_load(client, 1);
    wait_for_ack(1);
    EXPECT_EQ(g_response_stat.num, 1);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);

    // duplicated close
    close_load(client, 1);
    wait_for_ack(2);
    EXPECT_EQ(g_response_stat.num, 2);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);

    close_load(client, 0);
    wait_for_ack(3);
    EXPECT_EQ(g_response_stat.num, 3);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 1);
    EXPECT_EQ(g_response_stat.failed_tablet_ids[0], NORMAL_TABLET_ID);

    // server will close stream on CLOSE_LOAD
    wait_for_close();
    EXPECT_EQ(_load_stream_mgr->get_load_stream_num(), 0);
}

TEST_F(LoadStreamMgrTest, one_client_one_index_one_tablet_single_segment0) {
    MockSinkClient client;
    auto st = client.connect_stream();
    EXPECT_TRUE(st.ok());

    reset_response_stat();

    // append data
    butil::IOBuf append_buf;
    PStreamHeader header;
    std::string data = "file1 hello world 123 !@#$%^&*()_+";
    write_one_tablet(client, NORMAL_LOAD_ID, NORMAL_SENDER_ID, NORMAL_INDEX_ID, NORMAL_TABLET_ID, 0,
                     data, false);
    write_one_tablet(client, NORMAL_LOAD_ID, NORMAL_SENDER_ID, NORMAL_INDEX_ID, NORMAL_TABLET_ID, 0,
                     data, true);

    EXPECT_EQ(g_response_stat.num, 0);
    // CLOSE_LOAD
    close_load(client, 1);
    wait_for_ack(1);
    EXPECT_EQ(g_response_stat.num, 1);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);

    // duplicated close
    close_load(client, 1);
    wait_for_ack(2);
    EXPECT_EQ(g_response_stat.num, 2);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);

    close_load(client, 0);
    wait_for_ack(3);
    EXPECT_EQ(g_response_stat.num, 3);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 1);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.success_tablet_ids[0], NORMAL_TABLET_ID);

    auto written_data = read_data(NORMAL_TXN_ID, NORMAL_PARTITION_ID, NORMAL_TABLET_ID, 0);
    EXPECT_EQ(written_data, data + data);

    // server will close stream on CLOSE_LOAD
    wait_for_close();
    EXPECT_EQ(_load_stream_mgr->get_load_stream_num(), 0);
}

TEST_F(LoadStreamMgrTest, one_client_one_index_one_tablet_single_segment_without_eos) {
    MockSinkClient client;
    auto st = client.connect_stream();
    EXPECT_TRUE(st.ok());

    reset_response_stat();

    // append data
    butil::IOBuf append_buf;
    PStreamHeader header;
    std::string data = "file1 hello world 123 !@#$%^&*()_+";
    write_one_tablet(client, NORMAL_LOAD_ID, NORMAL_SENDER_ID, NORMAL_INDEX_ID, NORMAL_TABLET_ID, 0,
                     data, false);

    EXPECT_EQ(g_response_stat.num, 0);
    // CLOSE_LOAD
    close_load(client, 1);
    wait_for_ack(1);
    EXPECT_EQ(g_response_stat.num, 1);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);

    // duplicated close
    close_load(client, 1);
    wait_for_ack(2);
    EXPECT_EQ(g_response_stat.num, 2);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);

    close_load(client, 0);
    wait_for_ack(3);
    EXPECT_EQ(g_response_stat.num, 3);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 1);
    EXPECT_EQ(g_response_stat.failed_tablet_ids[0], NORMAL_TABLET_ID);

    // server will close stream on CLOSE_LOAD
    wait_for_close();
    EXPECT_EQ(_load_stream_mgr->get_load_stream_num(), 0);
}

TEST_F(LoadStreamMgrTest, one_client_one_index_one_tablet_single_segment1) {
    MockSinkClient client;
    auto st = client.connect_stream();
    EXPECT_TRUE(st.ok());

    reset_response_stat();

    // append data
    butil::IOBuf append_buf;
    PStreamHeader header;
    std::string data = "file1 hello world 123 !@#$%^&*()_+";
    write_one_tablet(client, NORMAL_LOAD_ID, NORMAL_SENDER_ID, NORMAL_INDEX_ID, NORMAL_TABLET_ID, 1,
                     data, false);
    write_one_tablet(client, NORMAL_LOAD_ID, NORMAL_SENDER_ID, NORMAL_INDEX_ID, NORMAL_TABLET_ID, 1,
                     data, true);

    EXPECT_EQ(g_response_stat.num, 0);
    // CLOSE_LOAD
    close_load(client, 1);
    wait_for_ack(1);
    EXPECT_EQ(g_response_stat.num, 1);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);

    // duplicated close
    close_load(client, 1);
    wait_for_ack(2);
    EXPECT_EQ(g_response_stat.num, 2);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);

    close_load(client, 0);
    wait_for_ack(3);
    EXPECT_EQ(g_response_stat.num, 3);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 1);
    EXPECT_EQ(g_response_stat.failed_tablet_ids[0], NORMAL_TABLET_ID);

    // server will close stream on CLOSE_LOAD
    wait_for_close();
    EXPECT_EQ(_load_stream_mgr->get_load_stream_num(), 0);
}

TEST_F(LoadStreamMgrTest, one_client_one_index_one_tablet_two_segment) {
    MockSinkClient client;
    auto st = client.connect_stream();
    EXPECT_TRUE(st.ok());

    reset_response_stat();

    // append data
    butil::IOBuf append_buf;
    PStreamHeader header;
    std::string data1 = "file1 hello world 123 !@#$%^&*()_+1";
    write_one_tablet(client, NORMAL_LOAD_ID, NORMAL_SENDER_ID, NORMAL_INDEX_ID, NORMAL_TABLET_ID, 0,
                     data1, false);
    std::string empty;
    write_one_tablet(client, NORMAL_LOAD_ID, NORMAL_SENDER_ID, NORMAL_INDEX_ID, NORMAL_TABLET_ID, 0,
                     empty, true);
    std::string data2 = "file1 hello world 123 !@#$%^&*()_+2";
    write_one_tablet(client, NORMAL_LOAD_ID, NORMAL_SENDER_ID, NORMAL_INDEX_ID, NORMAL_TABLET_ID, 1,
                     data2, true);

    EXPECT_EQ(g_response_stat.num, 0);
    // CLOSE_LOAD
    close_load(client, 1);
    wait_for_ack(1);
    EXPECT_EQ(g_response_stat.num, 1);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);

    // duplicated close
    close_load(client, 1);
    wait_for_ack(2);
    EXPECT_EQ(g_response_stat.num, 2);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);

    close_load(client, 0);
    wait_for_ack(3);
    EXPECT_EQ(g_response_stat.num, 3);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 1);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.success_tablet_ids[0], NORMAL_TABLET_ID);

    auto written_data = read_data(NORMAL_TXN_ID, NORMAL_PARTITION_ID, NORMAL_TABLET_ID, 0);
    EXPECT_EQ(written_data, data1);

    written_data = read_data(NORMAL_TXN_ID, NORMAL_PARTITION_ID, NORMAL_TABLET_ID, 1);
    EXPECT_EQ(written_data, data2);

    // server will close stream on CLOSE_LOAD
    wait_for_close();
    EXPECT_EQ(_load_stream_mgr->get_load_stream_num(), 0);
}

TEST_F(LoadStreamMgrTest, one_client_one_index_three_tablet) {
    MockSinkClient client;
    auto st = client.connect_stream();
    EXPECT_TRUE(st.ok());

    reset_response_stat();

    // append data
    butil::IOBuf append_buf;
    PStreamHeader header;
    std::string data1 = "file1 hello world 123 !@#$%^&*()_+1";
    write_one_tablet(client, NORMAL_LOAD_ID, NORMAL_SENDER_ID, NORMAL_INDEX_ID,
                     NORMAL_TABLET_ID + 0, 0, data1, true);
    write_one_tablet(client, NORMAL_LOAD_ID, NORMAL_SENDER_ID, NORMAL_INDEX_ID,
                     NORMAL_TABLET_ID + 1, 0, data1, true);
    std::string data2 = "file1 hello world 123 !@#$%^&*()_+2";
    write_one_tablet(client, NORMAL_LOAD_ID, NORMAL_SENDER_ID, NORMAL_INDEX_ID,
                     NORMAL_TABLET_ID + 2, 0, data2, true);

    EXPECT_EQ(g_response_stat.num, 0);
    // CLOSE_LOAD
    close_load(client, 1);
    wait_for_ack(1);
    EXPECT_EQ(g_response_stat.num, 1);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);

    // duplicated close
    close_load(client, 1);
    wait_for_ack(2);
    EXPECT_EQ(g_response_stat.num, 2);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);

    close_load(client, 0);
    wait_for_ack(3);
    EXPECT_EQ(g_response_stat.num, 3);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 3);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);
    std::set<int64_t> success_tablet_ids;
    for (auto& id : g_response_stat.success_tablet_ids) {
        success_tablet_ids.insert(id);
    }
    EXPECT_TRUE(success_tablet_ids.count(NORMAL_TABLET_ID));
    EXPECT_TRUE(success_tablet_ids.count(NORMAL_TABLET_ID + 1));
    EXPECT_TRUE(success_tablet_ids.count(NORMAL_TABLET_ID + 2));

    auto written_data = read_data(NORMAL_TXN_ID, NORMAL_PARTITION_ID, NORMAL_TABLET_ID, 0);
    EXPECT_EQ(written_data, data1);

    written_data = read_data(NORMAL_TXN_ID, NORMAL_PARTITION_ID, NORMAL_TABLET_ID + 1, 0);
    EXPECT_EQ(written_data, data1);

    written_data = read_data(NORMAL_TXN_ID, NORMAL_PARTITION_ID, NORMAL_TABLET_ID + 2, 0);
    EXPECT_EQ(written_data, data2);

    // server will close stream on CLOSE_LOAD
    wait_for_close();
    EXPECT_EQ(_load_stream_mgr->get_load_stream_num(), 0);
}

TEST_F(LoadStreamMgrTest, two_client_one_index_one_tablet_three_segment) {
    MockSinkClient clients[2];

    for (int i = 0; i < 2; i++) {
        auto st = clients[i].connect_stream(NORMAL_SENDER_ID + i);
        EXPECT_TRUE(st.ok());
    }
    reset_response_stat();

    std::vector<std::string> segment_data;
    segment_data.resize(6);
    // each sender three segments
    for (int32_t segid = 2; segid >= 0; segid--) {
        // append data
        for (int i = 0; i < 2; i++) {
            butil::IOBuf append_buf;
            PStreamHeader header;
            std::string data1 =
                    "sender_id=" + std::to_string(i) + ",segid=" + std::to_string(segid);
            write_one_tablet(clients[i], NORMAL_LOAD_ID, NORMAL_SENDER_ID + i, NORMAL_INDEX_ID,
                             NORMAL_TABLET_ID, segid, data1, true);
            segment_data[i * 3 + segid] = data1;
            LOG(INFO) << "segment_data[" << i * 3 + segid << "]" << data1;
        }
    }

    EXPECT_EQ(g_response_stat.num, 0);
    // CLOSE_LOAD
    close_load(clients[1], 1);
    wait_for_ack(1);
    EXPECT_EQ(g_response_stat.num, 1);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);

    // duplicated close
    close_load(clients[1], 1);
    wait_for_ack(2);
    // stream closed, no response will be sent
    EXPECT_EQ(g_response_stat.num, 1);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);

    close_load(clients[0], 0);
    wait_for_ack(2);
    EXPECT_EQ(g_response_stat.num, 2);
    EXPECT_EQ(g_response_stat.success_tablet_ids.size(), 1);
    EXPECT_EQ(g_response_stat.failed_tablet_ids.size(), 0);
    EXPECT_EQ(g_response_stat.success_tablet_ids[0], NORMAL_TABLET_ID);

    auto written_data = read_data(NORMAL_TXN_ID, NORMAL_PARTITION_ID, NORMAL_TABLET_ID, 0);
    size_t sender_pos = written_data.find('=');
    size_t sender_end = written_data.find(',');
    EXPECT_NE(sender_pos, std::string::npos);
    EXPECT_NE(sender_end, std::string::npos);
    auto sender_str = written_data.substr(sender_pos + 1, sender_end - sender_pos);
    LOG(INFO) << "sender_str " << sender_str;
    uint32_t sender_id = std::stoi(sender_str);

    for (int i = 0; i < 3; i++) {
        auto written_data = read_data(NORMAL_TXN_ID, NORMAL_PARTITION_ID, NORMAL_TABLET_ID, i);
        EXPECT_EQ(written_data, segment_data[sender_id * 3 + i]);
    }
    sender_id = (sender_id + 1) % 2;
    for (int i = 0; i < 3; i++) {
        auto written_data = read_data(NORMAL_TXN_ID, NORMAL_PARTITION_ID, NORMAL_TABLET_ID, i + 3);
        EXPECT_EQ(written_data, segment_data[sender_id * 3 + i]);
    }

    // server will close stream on CLOSE_LOAD
    wait_for_close();
    EXPECT_EQ(_load_stream_mgr->get_load_stream_num(), 0);
}

} // namespace doris
