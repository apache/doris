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

#include "common/status.h"
#include "http/http_handler.h"
#include "http/http_request.h"
#include "olap/tablet.h"

namespace doris {

class PadRowsetAction : public HttpHandler {
public:
    PadRowsetAction() = default;

    ~PadRowsetAction() override = default;

    void handle(HttpRequest* req) override;

private:
    Status _handle(HttpRequest* req);
    Status check_param(HttpRequest* req);

#ifdef BE_TEST
public:
#endif
    Status _pad_rowset(TabletSharedPtr tablet, const Version& version);
};
} // end namespace doris