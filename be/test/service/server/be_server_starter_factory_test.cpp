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

#include "service/server/be_server_starter_factory.h"

#include <gtest/gtest.h>

#include <memory>

#include "service/backend_service.h"
#include "service/server/server_starter.h"

namespace doris {

TEST(BeServerStarterFactoryTest, BackendThriftJoinReleasesService) {
    bool service_released = false;
    std::shared_ptr<BaseBackendService> service(
            nullptr, [&service_released](BaseBackendService*) { service_released = true; });
    std::weak_ptr<BaseBackendService> weak_service = service;

    std::unique_ptr<server::IServerStarter> starter;
    ASSERT_TRUE(server::create_backend_thrift_starter(nullptr, 0, service, &starter).ok());
    service.reset();

    EXPECT_FALSE(weak_service.expired());
    EXPECT_FALSE(service_released);

    starter->join();

    EXPECT_TRUE(weak_service.expired());
    EXPECT_TRUE(service_released);
}

} // namespace doris
