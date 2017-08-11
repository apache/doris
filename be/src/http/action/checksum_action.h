// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#ifndef  BDG_PALO_BE_SRC_HTTP_CHECKSUM_ACTION_H
#define  BDG_PALO_BE_SRC_HTTP_CHECKSUM_ACTION_H

#include <boost/scoped_ptr.hpp>

#include "http/http_handler.h"
#include "olap/command_executor.h"

namespace palo {

class ExecEnv;

class ChecksumAction : public HttpHandler {
public:
    explicit ChecksumAction(ExecEnv* exec_env);

    virtual ~ChecksumAction();

    virtual void handle(HttpRequest *req, HttpChannel *channel);
private:
    int64_t do_checksum(int64_t tablet_id, int64_t version, int64_t version_hash,
            int32_t schema_hash, HttpRequest *req, HttpChannel *channel);

    ExecEnv* _exec_env;
    CommandExecutor* _command_executor;

}; // end class ChecksumAction

} // end namespace palo
#endif // BDG_PALO_BE_SRC_COMMON_UTIL_DOWNLOAD_ACTION_H

