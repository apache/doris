// // Licensed to the Apache Software Foundation (ASF) under one
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

#include <brpc/server.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>

#include "common/metric.h"
#include "txn_kv.h"

namespace doris::cloud {

class MetaServerRegister;

class MetaServer {
public:
    MetaServer(std::shared_ptr<TxnKv> txn_kv);
    ~MetaServer() = default;

    /**
     * Starts to listen and server
     *
     * return 0 for success otherwise failure
     */
    int start(brpc::Server* server);

    void stop();

private:
    std::shared_ptr<TxnKv> txn_kv_;
    std::unique_ptr<MetaServerRegister> server_register_;
    std::unique_ptr<FdbMetricExporter> fdb_metric_exporter_;
};

class ServiceRegistryPB;

class MetaServerRegister {
public:
    MetaServerRegister(std::shared_ptr<TxnKv> txn_kv);
    ~MetaServerRegister();

    /**
     * Starts registering
     *
     * @return 0 on success, otherwise failure.
     */
    int start();

    /**
     * Notifies all the threads to quit and stop registering current server.
     * TODO(gavin): should we remove the server from the registry list actively
     *              when we call stop().
     */
    void stop();

private:
    /**
     * Prepares registry with given existing registry. If the server already
     * exists in the registry list, update mtime and lease, otherwise create a
     * new item for the server in the registry list.
     *
     * @param reg input and output param
     */
    void prepare_registry(ServiceRegistryPB* reg);

private:
    std::unique_ptr<std::thread> register_thread_;
    std::atomic<int> running_;
    std::mutex mtx_;
    std::condition_variable cv_;
    std::string id_;

    std::shared_ptr<TxnKv> txn_kv_; // Relies on other members, must be the
                                    // first to destruct
};

} // namespace doris::cloud
