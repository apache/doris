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

#include "cloud/cloud_storage_engine.h"
#include "common/status.h"
#include "http/http_handler_with_auth.h"
#include "olap/tablet.h"

namespace doris {
class HttpRequest;

class ExecEnv;

enum class DeleteBitmapActionType { COUNT_INFO = 1 };

/// This action is used for viewing the delete bitmap status
class CloudDeleteBitmapAction : public HttpHandlerWithAuth {
public:
    CloudDeleteBitmapAction(DeleteBitmapActionType ctype, ExecEnv* exec_env,
                            CloudStorageEngine& engine, TPrivilegeHier::type hier,
                            TPrivilegeType::type ptype);

    ~CloudDeleteBitmapAction() override = default;

    void handle(HttpRequest* req) override;

private:
    Status _handle_show_delete_bitmap_count(HttpRequest* req, std::string* json_result);

private:
    CloudStorageEngine& _engine;
    DeleteBitmapActionType _delete_bitmap_action_type;
};
} // namespace doris