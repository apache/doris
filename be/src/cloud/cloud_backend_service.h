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

#include "service/backend_service.h"

namespace doris {

class CloudStorageEngine;

class CloudBackendService final : public BaseBackendService {
public:
    static Status create_service(CloudStorageEngine& engine, ExecEnv* exec_env, int port,
                                 std::unique_ptr<ThriftServer>* server,
                                 std::shared_ptr<doris::CloudBackendService> service);

    CloudBackendService(CloudStorageEngine& engine, ExecEnv* exec_env);

    ~CloudBackendService() override;

    // If another cluster load, FE need to notify the cluster to sync the load data
    void sync_load_for_tablets(TSyncLoadForTabletsResponse& response,
                               const TSyncLoadForTabletsRequest& request) override;

    // Get top n hot partition which count times in scanner
    void get_top_n_hot_partitions(TGetTopNHotPartitionsResponse& response,
                                  const TGetTopNHotPartitionsRequest& request) override;

    // Download target tablets data in cache
    void warm_up_tablets(TWarmUpTabletsResponse& response,
                         const TWarmUpTabletsRequest& request) override;

    // Download the datas which load in other cluster
    void warm_up_cache_async(TWarmUpCacheAsyncResponse& response,
                             const TWarmUpCacheAsyncRequest& request) override;

    // Check whether the tablets finish warm up or not
    void check_warm_up_cache_async(TCheckWarmUpCacheAsyncResponse& response,
                                   const TCheckWarmUpCacheAsyncRequest& request) override;

    void get_stream_load_record(TStreamLoadRecordResult& result,
                                int64_t last_stream_record_time) override;

private:
    CloudStorageEngine& _engine;
};

} // namespace doris
