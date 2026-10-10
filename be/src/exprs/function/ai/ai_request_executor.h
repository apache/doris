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

#include <memory>
#include <string>

#include "common/status.h"

namespace doris {

struct AIResource;
class AIAdapter;
class QueryContext;

// Shared synchronous AI transport with a client cache spanning batches and Blocks.
// Functions own payloads and result parsing; concurrent calls borrow exclusive clients.
class AIRequestExecutor {
public:
    AIRequestExecutor();
    ~AIRequestExecutor();
    AIRequestExecutor(const AIRequestExecutor&) = delete;
    AIRequestExecutor& operator=(const AIRequestExecutor&) = delete;

    // max_retries counts retries after the initial attempt (0 means one attempt).
    // fail_on_http_error preserves AI_AGG's legacy curl error mode;
    // scalar/EMBED read and validate non-200 response bodies.
    Status execute(const std::string& request_body, std::string& response, const AIResource& config,
                   const AIAdapter& adapter, QueryContext* query_context,
                   bool fail_on_http_error = false);

private:
    struct ClientCache;
    std::unique_ptr<ClientCache> _clients;
};

} // namespace doris
