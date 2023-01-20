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

#include <brpc/closure_guard.h>
#include <brpc/http_header.h>
#include <brpc/http_status_code.h>
#include <gen_cpp/internal_service.pb.h>

#include <string>

#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "util/brpc_client_cache.h"
#include "util/easy_json.h"
#include "util/md5.h"
namespace doris {
BrpcHttpService::BrpcHttpService(ExecEnv* exec_env) : _exec_env(exec_env) {}

void BrpcHttpService::check_rpc_channel(::google::protobuf::RpcController* controller,
                                        const ::doris::HttpRequest* request,
                                        ::doris::HttpResponse* response,
                                        ::google::protobuf::Closure* done) {}

void BrpcHttpService::reset_rpc_channel(::google::protobuf::RpcController* controller,
                                        const ::doris::HttpRequest* request,
                                        ::doris::HttpResponse* response,
                                        ::google::protobuf::Closure* done) {}

void BrpcHttpService::config(::google::protobuf::RpcController* controller,
                             const ::doris::HttpRequest* request, ::doris::HttpResponse* response,
                             ::google::protobuf::Closure* done) {}

void BrpcHttpService::health(::google::protobuf::RpcController* controller,
                             const ::doris::HttpRequest* request, ::doris::HttpResponse* response,
                             ::google::protobuf::Closure* done) {}

void BrpcHttpService::jeprofile(::google::protobuf::RpcController* controller,
                                const ::doris::HttpRequest* request,
                                ::doris::HttpResponse* response,
                                ::google::protobuf::Closure* done) {}

void BrpcHttpService::meta(::google::protobuf::RpcController* controller,
                           const ::doris::HttpRequest* request, ::doris::HttpResponse* response,
                           ::google::protobuf::Closure* done) {}

void BrpcHttpService::metrics(::google::protobuf::RpcController* controller,
                              const ::doris::HttpRequest* request, ::doris::HttpResponse* response,
                              ::google::protobuf::Closure* done) {}

void BrpcHttpService::monitor(::google::protobuf::RpcController* controller,
                              const ::doris::HttpRequest* request, ::doris::HttpResponse* response,
                              ::google::protobuf::Closure* done) {}

void BrpcHttpService::pad_rowset(::google::protobuf::RpcController* controller,
                                 const ::doris::HttpRequest* request,
                                 ::doris::HttpResponse* response,
                                 ::google::protobuf::Closure* done) {}

void BrpcHttpService::pprof(::google::protobuf::RpcController* controller,
                            const ::doris::HttpRequest* request, ::doris::HttpResponse* response,
                            ::google::protobuf::Closure* done) {}

void BrpcHttpService::snapshot(::google::protobuf::RpcController* controller,
                               const ::doris::HttpRequest* request, ::doris::HttpResponse* response,
                               ::google::protobuf::Closure* done) {}

void BrpcHttpService::version(::google::protobuf::RpcController* controller,
                              const ::doris::HttpRequest* request, ::doris::HttpResponse* response,
                              ::google::protobuf::Closure* done) {}

void BrpcHttpService::check_tablet_segement(::google::protobuf::RpcController* controller,
                                            const ::doris::HttpRequest* request,
                                            ::doris::HttpResponse* response,
                                            ::google::protobuf::Closure* done) {}

void BrpcHttpService::check_sum(::google::protobuf::RpcController* controller,
                                const ::doris::HttpRequest* request,
                                ::doris::HttpResponse* response,
                                ::google::protobuf::Closure* done) {}

void BrpcHttpService::compaction(::google::protobuf::RpcController* controller,
                                 const ::doris::HttpRequest* request,
                                 ::doris::HttpResponse* response,
                                 ::google::protobuf::Closure* done) {}

void BrpcHttpService::reload_tablet(::google::protobuf::RpcController* controller,
                                    const ::doris::HttpRequest* request,
                                    ::doris::HttpResponse* response,
                                    ::google::protobuf::Closure* done) {}

void BrpcHttpService::restore_tablet(::google::protobuf::RpcController* controller,
                                     const ::doris::HttpRequest* request,
                                     ::doris::HttpResponse* response,
                                     ::google::protobuf::Closure* done) {}

void BrpcHttpService::tablet_migration(::google::protobuf::RpcController* controller,
                                       const ::doris::HttpRequest* request,
                                       ::doris::HttpResponse* response,
                                       ::google::protobuf::Closure* done) {}

void BrpcHttpService::distribution(::google::protobuf::RpcController* controller,
                                   const ::doris::HttpRequest* request,
                                   ::doris::HttpResponse* response,
                                   ::google::protobuf::Closure* done) {}

void BrpcHttpService::tablet_info(::google::protobuf::RpcController* controller,
                                  const ::doris::HttpRequest* request,
                                  ::doris::HttpResponse* response,
                                  ::google::protobuf::Closure* done) {}

void BrpcHttpService::download(::google::protobuf::RpcController* controller,
                               const ::doris::HttpRequest* request, ::doris::HttpResponse* response,
                               ::google::protobuf::Closure* done) {}

void BrpcHttpService::stream_load(::google::protobuf::RpcController* controller,
                                  const ::doris::HttpRequest* request,
                                  ::doris::HttpResponse* response,
                                  ::google::protobuf::Closure* done) {}

void BrpcHttpService::stream_load_2pc(::google::protobuf::RpcController* controller,
                                      const ::doris::HttpRequest* request,
                                      ::doris::HttpResponse* response,
                                      ::google::protobuf::Closure* done) {}

const std::string* get_param(brpc::Controller* cntl, const std::string& key) {
    return cntl->http_request().uri().GetQuery(key);
}

void reply_err(brpc::Controller* cntl, const std::string& err_msg) {
    cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
    cntl->response_attachment().append(err_msg);
}

void reply_ok(brpc::Controller* cntl, const std::string& msg) {
    cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    cntl->response_attachment().append(msg);
}

void reply_ok_json(brpc::Controller* cntl, const EasyJson& ej) {
    cntl->http_response().set_content_type("application/json");
    cntl->response_attachment().append(ej.ToString());
}
} // namespace doris