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

#include "cloud/cloud_internal_service.h"

#include <brpc/closure_guard.h>

#include "cloud/cloud_storage_engine.h"
#include "common/signal_handler.h"
#include "exec/rowid_fetcher.h"

namespace doris {

CloudInternalServiceImpl::CloudInternalServiceImpl(CloudStorageEngine& engine, ExecEnv* exec_env)
        : PInternalService(exec_env), _engine(engine) {}

CloudInternalServiceImpl::~CloudInternalServiceImpl() = default;

void CloudInternalServiceImpl::multiget_data(google::protobuf::RpcController* controller,
                                             const PMultiGetRequest* request,
                                             PMultiGetResponse* response,
                                             google::protobuf::Closure* done) {
    bool ret = _light_work_pool.try_offer([request, response, done, this]() {
        signal::set_signal_task_id(request->query_id());
        // multi get data by rowid
        MonotonicStopWatch watch;
        watch.start();
        brpc::ClosureGuard closure_guard(done);
        response->mutable_status()->set_status_code(0);
        RowIdStorageReader reader(&_engine);
        Status st = reader.read_by_rowids(*request, response);
        st.to_protobuf(response->mutable_status());
        LOG(INFO) << "multiget_data finished, cost(us):" << watch.elapsed_time() / 1000;
    });
    if (!ret) {
        offer_failed(response, done, _heavy_work_pool);
        return;
    }
}

} // namespace doris
