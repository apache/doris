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

#pragma once

#include "http/http_client.h"

#define POST_HTTP_TO_TEST_SERVER(uri)                                                     \
    {                                                                                     \
        std::string response;                                                             \
        HttpClient client;                                                                \
        std::string full_uri = global_test_http_host + uri;                               \
        auto status = client.init(full_uri);                                              \
        ASSERT_TRUE(status.ok()) << "uri=" << full_uri << ", err=" << status.to_string(); \
        status = client.execute_post_request("{}", &response);                            \
        ASSERT_TRUE(status.ok()) << status.to_string();                                   \
    }

namespace doris {

extern std::string global_test_http_host;

}
