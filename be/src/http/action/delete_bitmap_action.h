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

#include <stdint.h>

#include <string>

#include "common/status.h"
#include "http/http_handler_with_auth.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"

namespace doris {
#include "common/compile_check_begin.h"
class HttpRequest;

class ExecEnv;

enum class DeleteBitmapActionType { COUNT_LOCAL = 1, COUNT_MS = 2 };

/// This action is used for viewing the delete bitmap status
class DeleteBitmapAction : public HttpHandlerWithAuth {
public:
    DeleteBitmapAction(DeleteBitmapActionType ctype, ExecEnv* exec_env, BaseStorageEngine& engine,
                       TPrivilegeHier::type hier, TPrivilegeType::type ptype);

    ~DeleteBitmapAction() override = default;

    void handle(HttpRequest* req) override;

private:
    Status _handle_show_local_delete_bitmap_count(HttpRequest* req, std::string* json_result);
    Status _handle_show_ms_delete_bitmap_count(HttpRequest* req, std::string* json_result);

private:
    BaseStorageEngine& _engine;
    DeleteBitmapActionType _delete_bitmap_action_type;
};
#include "common/compile_check_end.h"
} // namespace doris