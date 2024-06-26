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

#include <gen_cpp/cloud.pb.h>

#include "meta-service/txn_kv.h"
#include "meta-service/txn_lazy_committer.h"

namespace doris::cloud {

class Recycler;
class Checker;

class RecyclerServiceImpl : public cloud::RecyclerService {
public:
    RecyclerServiceImpl(std::shared_ptr<TxnKv> txn_kv, Recycler* recycler, Checker* checker,
                        std::shared_ptr<TxnLazyCommitter> txn_lazy_committer);
    ~RecyclerServiceImpl() override;

    void recycle_instance(::google::protobuf::RpcController* controller,
                          const ::doris::cloud::RecycleInstanceRequest* request,
                          ::doris::cloud::RecycleInstanceResponse* response,
                          ::google::protobuf::Closure* done) override;

    void http(::google::protobuf::RpcController* controller,
              const ::doris::cloud::MetaServiceHttpRequest* request,
              ::doris::cloud::MetaServiceHttpResponse* response,
              ::google::protobuf::Closure* done) override;

private:
    void check_instance(const std::string& instance_id, MetaServiceCode& code, std::string& msg);

private:
    std::shared_ptr<TxnKv> txn_kv_;
    Recycler* recycler_; // Ref
    Checker* checker_;   // Ref
    std::shared_ptr<TxnLazyCommitter> txn_lazy_committer_;
};

} // namespace doris::cloud
