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

#include "brpc_http_service.h"

#include <string>

namespace doris {

#define DEFINE_ENDPOINT(__SIGNATURE__)                                                           \
    void BrpcHttpService::__SIGNATURE__(                                                         \
            ::google::protobuf::RpcController* controller, const ::doris::PHttpRequest* request, \
            ::doris::PHttpResponse* response, ::google::protobuf::Closure* done) {               \
        _dispatcher->dispatch(#__SIGNATURE__, controller, done);                                 \
    }

DEFINE_ENDPOINT(check_rpc_channel)
DEFINE_ENDPOINT(reset_rpc_channel)
DEFINE_ENDPOINT(config)
DEFINE_ENDPOINT(health)
DEFINE_ENDPOINT(jeprofile)
DEFINE_ENDPOINT(meta)
DEFINE_ENDPOINT(metrics)
DEFINE_ENDPOINT(monitor)
DEFINE_ENDPOINT(pad_rowset)
DEFINE_ENDPOINT(pprof)
DEFINE_ENDPOINT(snapshot)
DEFINE_ENDPOINT(version)
DEFINE_ENDPOINT(check_tablet_segement)
DEFINE_ENDPOINT(check_sum)
DEFINE_ENDPOINT(compaction)
DEFINE_ENDPOINT(reload_tablet)
DEFINE_ENDPOINT(restore_tablet)
DEFINE_ENDPOINT(tablet_migration)
DEFINE_ENDPOINT(tablets_distribution)
DEFINE_ENDPOINT(tablets_info)
DEFINE_ENDPOINT(download)
DEFINE_ENDPOINT(stream_load)
DEFINE_ENDPOINT(stream_load_2pc)

#undef DEFINE_ENDPOINT

BrpcHttpService::BrpcHttpService(ExecEnv* exec_env) : _dispatcher(new HandlerDispatcher()) {
    _dispatcher->register_handlers(exec_env);
}

void add_brpc_http_service(brpc::Server* server, ExecEnv* env) {
    const int stat = server->AddService(new BrpcHttpService(env), brpc::SERVER_OWNS_SERVICE);
    if (stat != 0) {
        LOG(WARNING) << "fail to add brpc http service";
    }
}
} // namespace doris