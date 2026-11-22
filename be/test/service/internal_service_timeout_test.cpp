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

#include <gtest/gtest.h>

#include "service/internal_service.h"

namespace doris {

TEST(InternalServiceTimeoutTest, KafkaRequestKeepsOptionalFullWidthSeconds) {
    // Inspect the actual protobuf field with the conversion used by get_info. No consumer,
    // worker thread, broker, or partition is needed to distinguish the wire boundaries.
    PProxyRequest request;
    EXPECT_EQ(PInternalService::kafka_meta_timeout_ms(request), 60000);
    request.set_timeout_secs(2147483);
    EXPECT_EQ(PInternalService::kafka_meta_timeout_ms(request), 2147483000);
    request.set_timeout_secs(2147484);
    EXPECT_EQ(PInternalService::kafka_meta_timeout_ms(request), INT32_MAX);
    request.set_timeout_secs(4294967296LL);
    EXPECT_EQ(PInternalService::kafka_meta_timeout_ms(request), INT32_MAX);
    request.set_timeout_secs(0);
    EXPECT_EQ(PInternalService::kafka_meta_timeout_ms(request), 0);
}

} // namespace doris
