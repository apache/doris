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

#include <gtest/gtest.h>

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
    ASSERT_NE(nullptr, stub1);
    address.port = 124;
    auto stub2 = cache.get_client(address);
    ASSERT_NE(nullptr, stub2);
    ASSERT_NE(stub1, stub2);
    address.port = 123;
    auto stub3 = cache.get_client(address);
    ASSERT_EQ(stub1, stub3);
}

TEST_F(BrpcClientCacheTest, invalid) {
    BrpcClientCache<PBackendService_Stub> cache;
    TNetworkAddress address;
    address.hostname = "invalid.cm.invalid";
    address.port = 123;
    auto stub1 = cache.get_client(address);
    ASSERT_EQ(nullptr, stub1);
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
