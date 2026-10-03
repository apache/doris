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

#include "common/query_log_context.h"

#include <bthread/bthread.h>
#include <gtest/gtest.h>

#include <array>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

#include "common/config.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/types.pb.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/query_task_controller.h"
#include "runtime/workload_management/resource_context.h"

namespace doris {
namespace {

TUniqueId log_test_id(int64_t hi, int64_t lo) {
    TUniqueId id;
    id.hi = hi;
    id.lo = lo;
    return id;
}

std::string current_log_marker() {
    std::ostringstream stream;
    append_query_log_identity(stream);
    return stream.str();
}

std::string log_marker(QueryLogIdentity identity) {
    ScopedQueryLogContext scope(identity);
    return current_log_marker();
}

void* check_bthread_log_identity(void* arg) {
    const auto identity = *static_cast<QueryLogIdentity*>(arg);
    EXPECT_EQ("", current_log_marker());
    {
        ScopedQueryLogContext scope(identity);
        for (int i = 0; i < 10; ++i) {
            // Suspending lets other bthreads use the same worker while this scope is live.
            EXPECT_EQ(0, bthread_usleep(100));
            EXPECT_EQ(identity.query_lo, current_query_log_identity().query_lo);
            EXPECT_EQ(identity.instance_lo, current_query_log_identity().instance_lo);
            {
                ScopedQueryLogContext empty_scope(QueryLogIdentity {});
                EXPECT_EQ("", current_log_marker());
            }
            EXPECT_EQ(identity.query_lo, current_query_log_identity().query_lo);
        }
    }
    EXPECT_EQ("", current_log_marker());
    return nullptr;
}

} // namespace

class QueryLogContextTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_enabled = config::sys_log_enable_query_id;
        config::sys_log_enable_query_id = true;
        // The key has process lifetime, just as it does after enabled logging initialization.
        init_query_log_context();
        _scope.reset(QueryLogIdentity {});
    }

    void TearDown() override { config::sys_log_enable_query_id = _saved_enabled; }

private:
    bool _saved_enabled = false;
    ScopedQueryLogContext _scope;
};

TEST_F(QueryLogContextTest, MissingAndQueryOnlyIdentity) {
    EXPECT_EQ("", current_log_marker());
    EXPECT_EQ("", log_marker(QueryLogIdentity(log_test_id(0, 0))));
    EXPECT_EQ(" [1234-abcd]", log_marker(QueryLogIdentity(log_test_id(0x1234, 0xabcd))));
    EXPECT_EQ(" [0-1]", log_marker(QueryLogIdentity(log_test_id(0, 1))));
    EXPECT_EQ(" [1-0]", log_marker(QueryLogIdentity(log_test_id(1, 0))));
    EXPECT_EQ(" [1234-abcd]",
              log_marker(QueryLogIdentity(log_test_id(0x1234, 0xabcd), log_test_id(0, 0))));
}

TEST_F(QueryLogContextTest, MessageSuffixDeduplicatesOnlyTheCurrentQuery) {
    const auto query = log_test_id(0x1234, 0xabcd);
    EXPECT_EQ(" [1234-abcd]", query_id_log_suffix(query));
    EXPECT_EQ("", query_id_log_suffix(log_test_id(0, 0)));
    {
        ScopedQueryLogContext scope(QueryLogIdentity(query, log_test_id(0x1234, 0xabce)));
        EXPECT_EQ(" [1234-abcd/1] Query finished",
                  current_log_marker() + " Query" + query_id_log_suffix(query) + " finished");
        EXPECT_EQ(" [5678-abcd]", query_id_log_suffix(log_test_id(0x5678, 0xabcd)));
        EXPECT_EQ(" [1234-abce]", query_id_log_suffix(log_test_id(0x1234, 0xabce)));
        {
            ScopedQueryLogContext background(QueryLogIdentity {});
            EXPECT_EQ(" [1234-abcd]", query_id_log_suffix(query));
        }
        EXPECT_EQ("", query_id_log_suffix(query));
    }
    EXPECT_EQ(" [1234-abcd]", query_id_log_suffix(query));
    EXPECT_EQ(" [ffffffffffffffff-8000000000000000]",
              query_id_log_suffix(log_test_id(-1, std::numeric_limits<int64_t>::min())));
}

TEST_F(QueryLogContextTest, DisabledPrefixKeepsTheMessageId) {
    config::sys_log_enable_query_id = false;
    EXPECT_EQ(" [1-2]", query_id_log_suffix(log_test_id(1, 2)));
}

TEST_F(QueryLogContextTest, ReusedWorkerDoesNotDeduplicateAnotherQuery) {
    const auto first = log_test_id(1, 2);
    const auto second = log_test_id(3, 4);
    std::thread worker([&] {
        {
            ScopedQueryLogContext scope {QueryLogIdentity(first)};
            EXPECT_EQ("", query_id_log_suffix(first));
            EXPECT_EQ(" [3-4]", query_id_log_suffix(second));
        }
        EXPECT_EQ(" [1-2]", query_id_log_suffix(first));
        {
            ScopedQueryLogContext scope {QueryLogIdentity(second)};
            EXPECT_EQ(" [1-2]", query_id_log_suffix(first));
            EXPECT_EQ("", query_id_log_suffix(second));
        }
        EXPECT_EQ("", current_log_marker());
    });
    worker.join();
}

TEST_F(QueryLogContextTest, CompactInstanceOffset) {
    const auto query = log_test_id(0x1234, 0xabcd);
    EXPECT_EQ(" [1234-abcd/1]", log_marker(QueryLogIdentity(query, log_test_id(0x1234, 0xabce))));
    EXPECT_EQ(" [1234-abcd/13]", log_marker(QueryLogIdentity(query, log_test_id(0x1234, 0xabda))));
    EXPECT_EQ(" [1234-abcd/0]", log_marker(QueryLogIdentity(query, query)));
}

TEST_F(QueryLogContextTest, InstanceOffsetUsesUnsignedArithmetic) {
    EXPECT_EQ(" [1234-ffffffffffffffff/1]",
              log_marker(QueryLogIdentity(log_test_id(0x1234, -1), log_test_id(0x1234, 0))));
    EXPECT_EQ(" [1234-10/18446744073709551615]",
              log_marker(QueryLogIdentity(log_test_id(0x1234, 16), log_test_id(0x1234, 15))));
    const int64_t min = std::numeric_limits<int64_t>::min();
    EXPECT_EQ(" [ffffffffffffffff-8000000000000000/1]",
              log_marker(QueryLogIdentity(log_test_id(-1, min), log_test_id(-1, min + 1))));
}

TEST_F(QueryLogContextTest, FullInstanceIdWhenQueryCannotReconstructIt) {
    EXPECT_EQ(
            " [1234-abcd fragment_instance_id=5678-abce]",
            log_marker(QueryLogIdentity(log_test_id(0x1234, 0xabcd), log_test_id(0x5678, 0xabce))));
    EXPECT_EQ(" [fragment_instance_id=1234-abce]",
              log_marker(QueryLogIdentity(log_test_id(0, 0), log_test_id(0x1234, 0xabce))));
}

TEST_F(QueryLogContextTest, FormattingPreservesStreamFlags) {
    ScopedQueryLogContext scope(
            QueryLogIdentity(log_test_id(0x1234, 0xabcd), log_test_id(0x1234, 0xabda)));
    std::ostringstream stream;
    stream << std::hex << std::showbase;
    const auto flags = stream.flags();
    append_query_log_identity(stream);
    EXPECT_EQ(flags, stream.flags());
    stream << ' ' << 255;
    EXPECT_EQ(" [1234-abcd/13] 0xff", stream.str());
}

TEST_F(QueryLogContextTest, IdentityCopiesThriftAndProtobufIds) {
    auto query = log_test_id(0x1234, 0xabcd);
    auto instance = log_test_id(0x1234, 0xabce);
    QueryLogIdentity thrift_query(query);
    QueryLogIdentity thrift_instance(query, instance);
    query.lo = 1;
    instance.lo = 2;
    EXPECT_EQ(" [1234-abcd]", log_marker(thrift_query));
    EXPECT_EQ(" [1234-abcd/1]", log_marker(thrift_instance));

    PUniqueId proto_query;
    proto_query.set_hi(-1);
    proto_query.set_lo(-2);
    PUniqueId proto_instance;
    proto_instance.set_hi(-1);
    proto_instance.set_lo(-1);
    QueryLogIdentity protobuf_query(proto_query);
    QueryLogIdentity protobuf_instance(proto_query, proto_instance);
    proto_query.set_lo(1);
    proto_instance.set_lo(2);
    EXPECT_EQ(" [ffffffffffffffff-fffffffffffffffe]", log_marker(protobuf_query));
    EXPECT_EQ(" [ffffffffffffffff-fffffffffffffffe/1]", log_marker(protobuf_instance));
}

TEST_F(QueryLogContextTest, NestedResetAndExceptionRestoreOuterIdentity) {
    ScopedQueryLogContext outer(QueryLogIdentity(log_test_id(1, 2)));
    {
        ScopedQueryLogContext inner(QueryLogIdentity(log_test_id(3, 4)));
        EXPECT_EQ(" [3-4]", current_log_marker());
        inner.reset(QueryLogIdentity(log_test_id(5, 6)));
        EXPECT_EQ(" [5-6]", current_log_marker());
        inner.reset(QueryLogIdentity {});
        EXPECT_EQ("", current_log_marker());
        inner.reset(QueryLogIdentity(log_test_id(7, 8)));
        EXPECT_EQ(" [7-8]", current_log_marker());
    }
    EXPECT_EQ(" [1-2]", current_log_marker());
    EXPECT_THROW(
            {
                ScopedQueryLogContext failing(QueryLogIdentity(log_test_id(9, 10)));
                throw std::runtime_error("test unwind");
            },
            std::runtime_error);
    EXPECT_EQ(" [1-2]", current_log_marker());
}

TEST_F(QueryLogContextTest, TaskAttachmentAndResourceSwitchRestoreIdentity) {
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "UT-QueryLogContextTest");
    auto query = ResourceContext::create_shared();
    query->memory_context()->set_mem_tracker(tracker);
    query->set_task_controller(QueryTaskController::create(nullptr));
    query->task_controller()->set_task_id(log_test_id(1, 2));
    auto other_query = ResourceContext::create_shared();
    other_query->memory_context()->set_mem_tracker(tracker);
    other_query->set_task_controller(QueryTaskController::create(nullptr));
    other_query->task_controller()->set_task_id(log_test_id(3, 4));
    auto background = ResourceContext::create_shared();
    background->memory_context()->set_mem_tracker(tracker);

    ScopedQueryLogContext callback(QueryLogIdentity(log_test_id(5, 6)));
    {
        AttachTask task(query);
        EXPECT_EQ(" [1-2]", current_log_marker());
        ScopedQueryLogContext fragment(QueryLogIdentity(log_test_id(1, 2), log_test_id(1, 3)));
        {
            // Reattaching the same resource must retain the more specific fragment identity.
            SwitchResourceContext same(query);
            EXPECT_EQ(" [1-2/1]", current_log_marker());
        }
        {
            SwitchResourceContext other(other_query);
            EXPECT_EQ(" [3-4]", current_log_marker());
        }
        EXPECT_EQ(" [1-2/1]", current_log_marker());
        {
            SwitchResourceContext non_query(background);
            EXPECT_EQ("", current_log_marker());
        }
        EXPECT_EQ(" [1-2/1]", current_log_marker());
    }
    EXPECT_EQ(" [5-6]", current_log_marker());
}

TEST_F(QueryLogContextTest, PthreadContextsAreIsolated) {
    ScopedQueryLogContext parent(QueryLogIdentity(log_test_id(1, 2)));
    std::thread worker([] {
        EXPECT_EQ("", current_log_marker());
        {
            ScopedQueryLogContext scope(QueryLogIdentity(log_test_id(3, 4)));
            std::thread child([] { EXPECT_EQ("", current_log_marker()); });
            child.join();
            EXPECT_EQ(" [3-4]", current_log_marker());
        }
        EXPECT_EQ("", current_log_marker());
    });
    worker.join();
    EXPECT_EQ(" [1-2]", current_log_marker());
}

TEST_F(QueryLogContextTest, BthreadContextsAreIsolatedAcrossSuspension) {
    ScopedQueryLogContext parent(QueryLogIdentity(log_test_id(1, 2)));
    std::array<QueryLogIdentity, 16> identities;
    std::vector<bthread_t> started;
    for (size_t i = 0; i < identities.size(); ++i) {
        identities[i] = QueryLogIdentity(log_test_id(3, static_cast<int64_t>(i + 1)),
                                         log_test_id(3, static_cast<int64_t>(i + 2)));
        bthread_t tid;
        const int status =
                bthread_start_background(&tid, nullptr, check_bthread_log_identity, &identities[i]);
        EXPECT_EQ(0, status);
        // Join every successfully started bthread even if a later start fails.
        if (status == 0) {
            started.push_back(tid);
        }
    }
    for (auto tid : started) {
        EXPECT_EQ(0, bthread_join(tid, nullptr));
    }
    EXPECT_EQ(" [1-2]", current_log_marker());
}

} // namespace doris
