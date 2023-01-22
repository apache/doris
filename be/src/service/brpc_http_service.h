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

#include <string>

#include "gen_cpp/http_service.pb.h"
#include "http/brpc/handler_dispatcher.h"

namespace doris {

class ExecEnv;

class BrpcHttpService : public PHttpService {
public:
    BrpcHttpService(ExecEnv* exec_env);
    ~BrpcHttpService() override = default;
    void check_rpc_channel(::google::protobuf::RpcController* controller,
                           const ::doris::PHttpRequest* request, ::doris::PHttpResponse* response,
                           ::google::protobuf::Closure* done) override;
    void reset_rpc_channel(::google::protobuf::RpcController* controller,
                           const ::doris::PHttpRequest* request, ::doris::PHttpResponse* response,
                           ::google::protobuf::Closure* done) override;

    void config(::google::protobuf::RpcController* controller, const ::doris::PHttpRequest* request,
                ::doris::PHttpResponse* response, ::google::protobuf::Closure* done) override;
    void health(::google::protobuf::RpcController* controller, const ::doris::PHttpRequest* request,
                ::doris::PHttpResponse* response, ::google::protobuf::Closure* done) override;
    void jeprofile(::google::protobuf::RpcController* controller,
                   const ::doris::PHttpRequest* request, ::doris::PHttpResponse* response,
                   ::google::protobuf::Closure* done) override;
    void meta(::google::protobuf::RpcController* controller, const ::doris::PHttpRequest* request,
              ::doris::PHttpResponse* response, ::google::protobuf::Closure* done) override;
    void metrics(::google::protobuf::RpcController* controller,
                 const ::doris::PHttpRequest* request, ::doris::PHttpResponse* response,
                 ::google::protobuf::Closure* done) override;
    void monitor(::google::protobuf::RpcController* controller,
                 const ::doris::PHttpRequest* request, ::doris::PHttpResponse* response,
                 ::google::protobuf::Closure* done) override;
    void pad_rowset(::google::protobuf::RpcController* controller,
                    const ::doris::PHttpRequest* request, ::doris::PHttpResponse* response,
                    ::google::protobuf::Closure* done) override;
    void pprof(::google::protobuf::RpcController* controller, const ::doris::PHttpRequest* request,
               ::doris::PHttpResponse* response, ::google::protobuf::Closure* done) override;
    void snapshot(::google::protobuf::RpcController* controller,
                  const ::doris::PHttpRequest* request, ::doris::PHttpResponse* response,
                  ::google::protobuf::Closure* done) override;
    void version(::google::protobuf::RpcController* controller,
                 const ::doris::PHttpRequest* request, ::doris::PHttpResponse* response,
                 ::google::protobuf::Closure* done) override;

    void check_tablet_segement(::google::protobuf::RpcController* controller,
                               const ::doris::PHttpRequest* request,
                               ::doris::PHttpResponse* response,
                               ::google::protobuf::Closure* done) override;
    void check_sum(::google::protobuf::RpcController* controller,
                   const ::doris::PHttpRequest* request, ::doris::PHttpResponse* response,
                   ::google::protobuf::Closure* done) override;
    void compaction(::google::protobuf::RpcController* controller,
                    const ::doris::PHttpRequest* request, ::doris::PHttpResponse* response,
                    ::google::protobuf::Closure* done) override;
    void reload_tablet(::google::protobuf::RpcController* controller,
                       const ::doris::PHttpRequest* request, ::doris::PHttpResponse* response,
                       ::google::protobuf::Closure* done) override;
    void restore_tablet(::google::protobuf::RpcController* controller,
                        const ::doris::PHttpRequest* request, ::doris::PHttpResponse* response,
                        ::google::protobuf::Closure* done) override;
    void tablet_migration(::google::protobuf::RpcController* controller,
                          const ::doris::PHttpRequest* request, ::doris::PHttpResponse* response,
                          ::google::protobuf::Closure* done) override;
    void tablets_distribution(::google::protobuf::RpcController* controller,
                              const ::doris::PHttpRequest* request,
                              ::doris::PHttpResponse* response,
                              ::google::protobuf::Closure* done) override;
    void tablets_info(::google::protobuf::RpcController* controller,
                      const ::doris::PHttpRequest* request, ::doris::PHttpResponse* response,
                      ::google::protobuf::Closure* done) override;

    void download(::google::protobuf::RpcController* controller,
                  const ::doris::PHttpRequest* request, ::doris::PHttpResponse* response,
                  ::google::protobuf::Closure* done) override;
    void stream_load(::google::protobuf::RpcController* controller,
                     const ::doris::PHttpRequest* request, ::doris::PHttpResponse* response,
                     ::google::protobuf::Closure* done) override;
    void stream_load_2pc(::google::protobuf::RpcController* controller,
                         const ::doris::PHttpRequest* request, ::doris::PHttpResponse* response,
                         ::google::protobuf::Closure* done) override;

private:
    std::unique_ptr<HandlerDispatcher> _dispatcher;
};

void add_brpc_http_service(brpc::Server* server, ExecEnv* env);
} // namespace doris