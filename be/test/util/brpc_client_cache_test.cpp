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

#include "util/brpc_client_cache.h"

#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"

namespace doris {

class BrpcClientCacheTest : public testing::Test {
public:
    BrpcClientCacheTest() {}
    virtual ~BrpcClientCacheTest() {}
};

TEST_F(BrpcClientCacheTest, normal) {
    BrpcClientCache<PBackendService_Stub> cache;
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub1 = cache.get_client(address);
    EXPECT_NE(nullptr, stub1);
    address.port = 124;
    auto stub2 = cache.get_client(address);
    EXPECT_NE(nullptr, stub2);
    EXPECT_NE(stub1, stub2);
    address.port = 123;
    auto stub3 = cache.get_client(address);
    EXPECT_EQ(stub1, stub3);
}

TEST_F(BrpcClientCacheTest, invalid) {
    BrpcClientCache<PBackendService_Stub> cache;
    TNetworkAddress address;
    address.hostname = "invalid.cm.invalid";
    address.port = 123;
    auto stub1 = cache.get_client(address);
    EXPECT_EQ(nullptr, stub1);
}

TEST_F(BrpcClientCacheTest, failure) {
    BrpcClientCache<PBackendService_Stub> cache;
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    std::shared_ptr<PBackendService_Stub> stub1 = cache.get_client(address);
    EXPECT_NE(nullptr, stub1);
    std::shared_ptr<PBackendService_Stub> stub2 = cache.get_client(address);
    EXPECT_NE(nullptr, stub2);
    // The channel is ok, so that the stub is the same
    EXPECT_EQ(stub1, stub2);
    EXPECT_TRUE(static_cast<FailureDetectChannel*>(stub1->channel())->channel_status()->ok());

    // update channel st to error
    static_cast<FailureDetectChannel*>(stub1->channel())
            ->channel_status()
            ->update(Status::NetworkError("test brpc error"));
    std::shared_ptr<PBackendService_Stub> stub3 = cache.get_client(address);
    EXPECT_NE(nullptr, stub3);
    EXPECT_TRUE(static_cast<FailureDetectChannel*>(stub3->channel())->channel_status()->ok());
    // Then will get a new brpc stub not the previous one.
    EXPECT_NE(stub2, stub3);
    // The previous channel is not ok.
    EXPECT_FALSE(static_cast<FailureDetectChannel*>(stub2->channel())->channel_status()->ok());

    // Call handshake method, it will trigger host is down error.
    cache.available(stub3, "127.0.0.1:123");
    EXPECT_FALSE(static_cast<FailureDetectChannel*>(stub3->channel())->channel_status()->ok());
}

} // namespace doris
