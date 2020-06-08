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

#include "http/http_handler.h"
#include "common/status.h"

namespace doris {


enum CompactionActionType {
    SHOW_INFO = 1,
    RUN_COMPACTION = 2
};

// This action is used for viewing the compaction status.
// See compaction-action.md for details.
class CompactionAction : public HttpHandler {
public:
    CompactionAction(CompactionActionType type) : _type(type) {}

    virtual ~CompactionAction() {}

    void handle(HttpRequest *req) override;

private:
    Status _handle_show_compaction(HttpRequest *req, std::string* json_result);

private:
    CompactionActionType _type;
};

} // end namespace doris

