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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <thread>

#include "common/logging.h"
#include "meta-service/txn_kv.h"

namespace doris::cloud {

class FdbMetricExporter {
public:
    FdbMetricExporter(std::shared_ptr<TxnKv> txn_kv)
            : txn_kv_(std::move(txn_kv)), running_(false) {}
    ~FdbMetricExporter();

    int start();
    void stop();

    static void export_fdb_metrics(TxnKv* txn_kv);

private:
    std::shared_ptr<TxnKv> txn_kv_;
    std::unique_ptr<std::thread> thread_;
    std::atomic<bool> running_;
    std::mutex running_mtx_;
    std::condition_variable running_cond_;
    int sleep_interval_ms_ = 5000;
};

} // namespace doris::cloud