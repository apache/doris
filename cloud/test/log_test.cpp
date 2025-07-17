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

//#define private public
#include "meta-service/meta_service.h"
//#undef private

#include <bthread/bthread.h>
#include <gtest/gtest.h>

#include <cstring>
#include <random>
#include <regex>
#include <sstream>
#include <thread>

#include "common/logging.h"
#include "meta-store/mem_txn_kv.h"
#include "mock_resource_manager.h"

using doris::cloud::AnnotateTag;

int main(int argc, char** argv) {
    if (!doris::cloud::init_glog("log_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
using namespace doris::cloud;

std::unique_ptr<MetaServiceProxy> get_meta_service() {
    int ret = 0;
    // MemKv
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    if (txn_kv != nullptr) {
        ret = txn_kv->init();
        [&] { ASSERT_EQ(ret, 0); }();
    }
    [&] { ASSERT_NE(txn_kv.get(), nullptr); }();

    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->remove("\x00", "\xfe"); // This is dangerous if the fdb is not correctly set
    EXPECT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl);
    return std::make_unique<MetaServiceProxy>(std::move(meta_service));
}

static std::string next_rowset_id() {
    static int cnt = 0;
    return std::to_string(++cnt);
}

static void add_tablet(CreateTabletsRequest& req, int64_t table_id, int64_t index_id,
                       int64_t partition_id, int64_t tablet_id) {
    auto* tablet = req.add_tablet_metas();
    tablet->set_table_id(table_id);
    tablet->set_index_id(index_id);
    tablet->set_partition_id(partition_id);
    tablet->set_tablet_id(tablet_id);
    auto* schema = tablet->mutable_schema();
    schema->set_schema_version(0);
    auto* first_rowset = tablet->add_rs_metas();
    first_rowset->set_rowset_id(0); // required
    first_rowset->set_rowset_id_v2(next_rowset_id());
    first_rowset->set_start_version(0);
    first_rowset->set_end_version(1);
    first_rowset->mutable_tablet_schema()->CopyFrom(*schema);
}

static void create_tablet(MetaServiceProxy* meta_service, int64_t table_id, int64_t index_id,
                          int64_t partition_id, int64_t tablet_id) {
    brpc::Controller cntl;
    CreateTabletsRequest req;
    CreateTabletsResponse res;
    add_tablet(req, table_id, index_id, partition_id, tablet_id);
    meta_service->create_tablets(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
}

TEST(LogTest, ConstructionTest) {
    // Heap allocation is disabled.
    // new AnnotateTag();

    // Arithmetics
    {
        char c = 0;
        bool b = false;
        int8_t i8 = 0;
        uint8_t u8 = 0;
        int16_t i16 = 0;
        uint16_t u16 = 0;
        int32_t i32 = 0;
        uint32_t u32 = 0;
        int64_t i64 = 0;
        uint64_t u64 = 0;

        AnnotateTag tag_char("char", c);
        AnnotateTag tag_bool("bool", b);
        AnnotateTag tag_i8("i8", i8);
        AnnotateTag tag_u8("u8", u8);
        AnnotateTag tag_i16("i16", i16);
        AnnotateTag tag_u16("u16", u16);
        AnnotateTag tag_i32("i32", i32);
        AnnotateTag tag_u32("u32", u32);
        AnnotateTag tag_i64("i64", i64);
        AnnotateTag tag_u64("u64", u64);
        LOG_INFO("hello");
    }

    // String literals.
    {
        const char* text = "hello";
        AnnotateTag tag_text("hello", text);
        LOG_INFO("hello");
    }

    // String view.
    {
        std::string test("abc");
        AnnotateTag tag_text("hello", std::string_view(test));
        LOG_INFO("hello");
    }

    // Const string.
    {
        const std::string test("abc");
        AnnotateTag tag_text("hello", test);
        LOG_INFO("hello");
    }
}

TEST(LogTest, ThreadTest) {
    // In pthread.
    {
        ASSERT_EQ(bthread_self(), 0);
        AnnotateTag tag("run_in_bthread", true);
        LOG_INFO("thread test");
    }

    // In bthread.
    {
        auto fn = +[](void*) -> void* {
            EXPECT_NE(bthread_self(), 0);
            AnnotateTag tag("run_in_bthread", true);
            LOG_INFO("thread test");
            return nullptr;
        };
        bthread_t tid;
        ASSERT_EQ(bthread_start_background(&tid, nullptr, fn, nullptr), 0);
        ASSERT_EQ(bthread_join(tid, nullptr), 0);
    }
}

TEST(LogTest, Variable) {
    // lvalue mutable
    {
        std::string s {"hello"};
        AnnotateTag tag("s", s);
        std::stringstream ss;
        AnnotateTag::format_tag_list(ss);
        ASSERT_EQ(ss.str(), " s=\"hello\"");
        s = "world";
        ss.str("");
        AnnotateTag::format_tag_list(ss);
        ASSERT_EQ(ss.str(), " s=\"world\"");
        s = "doris";
        ss.str("");
        AnnotateTag::format_tag_list(ss);
        ASSERT_EQ(ss.str(), " s=\"doris\"");
    }
    {
        int64_t test_id {};
        AnnotateTag tag_test_id("test_id", test_id);
        std::stringstream ss;
        AnnotateTag::format_tag_list(ss);
        ASSERT_EQ(ss.str(), " test_id=0");
        test_id = 2022;
        ss.str("");
        AnnotateTag::format_tag_list(ss);
        ASSERT_EQ(ss.str(), " test_id=2022");
    }
    // rvalue constant
    {
        AnnotateTag tag("log_id", 111111);
        std::stringstream ss;
        AnnotateTag::format_tag_list(ss);
        ASSERT_EQ(ss.str(), " log_id=111111");
    }
}

TEST(LogTest, RpcRunningOutput) {
    class StringLogSink : public google::LogSink {
    public:
        explicit StringLogSink(std::ostringstream& stream) : stream_(stream) {}

        void send(google::LogSeverity severity, const char* filename, const char* base_filename,
                  int line, const struct ::tm* tm_time, const char* message,
                  size_t message_len) override {
            stream_ << "[" << google::GetLogSeverityName(severity) << "] " << message << "\n";
        }

    private:
        std::ostringstream& stream_;
    };
    auto meta_service = get_meta_service();
    auto mem_kv = std::dynamic_pointer_cast<MemTxnKv>(meta_service->txn_kv());
    constexpr auto table_id = 10021, index_id = 10022, partition_id = 10023, tablet_id = 10024;

    std::ostringstream ss;
    StringLogSink sink(ss);

    google::AddLogSink(&sink);

    create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id);

    google::RemoveLogSink(&sink);
    std::string captured_log = ss.str();
    ASSERT_NE(captured_log.find("instance_id=\"test_instance\" table_id=10021 index_id=10022 "
                                "partition_id=10023 tablet_id=10024"),
              std::string::npos);

    std::regex log_pattern(
            R"(\[INFO\] finish "create_tablets" remote caller: [^ ]+ response=status \{ code: OK msg: *\} log_id=\S+ instance_id="test_instance")");
    ASSERT_TRUE(std::regex_search(captured_log, log_pattern));
    std::cerr << "Captured Log:\n" << captured_log << std::endl;
}