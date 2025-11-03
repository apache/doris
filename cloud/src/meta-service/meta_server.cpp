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

#include "meta_server.h"

#include <brpc/server.h>
#include <butil/endpoint.h>
#include <gen_cpp/cloud.pb.h>
#include <glog/logging.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <random>
#include <thread>

#include "common/bvars.h"
#include "common/config.h"
#include "common/defer.h"
#include "common/metric.h"
#include "common/network_util.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "rate-limiter/rate_limiter.h"
#include "resource-manager/resource_manager.h"

namespace doris::cloud {

MetaServer::MetaServer(std::shared_ptr<TxnKv> txn_kv) : txn_kv_(std::move(txn_kv)) {}

int MetaServer::start(brpc::Server* server) {
    DCHECK(server);
    auto rc_mgr = std::make_shared<ResourceManager>(txn_kv_);
    int ret = rc_mgr->init();
    TEST_SYNC_POINT_CALLBACK("MetaServer::start:1", &ret);
    if (ret != 0) {
        LOG(WARNING) << "failed to init resrouce manager, ret=" << ret;
        return 1;
    }

    // Add server register
    server_register_.reset(new MetaServerRegister(txn_kv_));
    ret = server_register_->start();
    TEST_SYNC_POINT_CALLBACK("MetaServer::start:2", &ret);
    if (ret != 0) {
        LOG(WARNING) << "failed to start server register";
        return -1;
    }

    // Add instance watcher
    if (config::enable_instance_update_watcher) {
        instance_watcher_.reset(new MetaServerInstanceWatcher(txn_kv_, rc_mgr));
        ret = instance_watcher_->start();
        TEST_SYNC_POINT_CALLBACK("MetaServer::start:3", &ret);
        if (ret != 0) {
            LOG(WARNING) << "failed to start instance watcher";
            return -1;
        }
    }

    auto rate_limiter = std::make_shared<RateLimiter>();
    auto snapshot_mgr = std::make_shared<SnapshotManager>(txn_kv_);

    // Add service
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv_, rc_mgr, rate_limiter,
                                                          std::move(snapshot_mgr));
    auto meta_service_proxy = new MetaServiceProxy(std::move(meta_service));

    brpc::ServiceOptions options;
    options.ownership = brpc::SERVER_OWNS_SERVICE;
    if (!config::secondary_package_name.empty()) {
        LOG(INFO) << "Add MetaService with secondary package name "
                  << config::secondary_package_name;
        options.secondary_package_name = config::secondary_package_name;
    }
    server->AddService(meta_service_proxy, options);

    return 0;
}

void MetaServer::stop() {
    server_register_->stop();
    if (config::enable_instance_update_watcher) {
        instance_watcher_->stop();
    }
}

void MetaServerRegister::prepare_registry(ServiceRegistryPB* reg) {
    using namespace std::chrono;
    auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    std::string ip = get_local_ip(config::priority_networks);
    int32_t port = config::brpc_listen_port;
    std::string id = ip + ":" + std::to_string(port);
    ServiceRegistryPB::Item item;
    item.set_id(id);
    item.set_ip(ip);
    item.set_port(port);
    item.set_ctime_ms(now);
    item.set_mtime_ms(now);
    item.set_expiration_time_ms(now + config::meta_server_lease_ms);
    if (!config::hostname.empty()) {
        item.set_host(config::hostname);
    }

    if (!id_.empty() && id_ != id) {
        LOG(WARNING) << "server id changed, old=" << id_ << " new=" << id;
        id_ = id;
    }

    int num_items = reg->items_size();
    ServiceRegistryPB out;
    for (int i = 0; i < num_items; ++i) {
        const auto& e = reg->items(i);
        if (e.expiration_time_ms() < now) continue;
        if (e.id() == id) {
            item.set_ctime_ms(e.ctime_ms());
            continue;
        }
        *out.add_items() = e;
    }
    *out.add_items() = item;
    *reg = out;
}

MetaServerRegister::MetaServerRegister(std::shared_ptr<TxnKv> txn_kv)
        : running_(0), txn_kv_(std::move(txn_kv)) {
    register_thread_.reset(new std::thread([this] {
        while (running_.load() == 0) {
            LOG(INFO) << "register thread wait for start";
            std::unique_lock l(mtx_);
            cv_.wait_for(l, std::chrono::milliseconds(config::meta_server_register_interval_ms));
        }
        LOG(INFO) << "register thread begins to run";
        std::mt19937 gen(std::random_device("/dev/urandom")());
        std::uniform_int_distribution<int> rd_len(50, 300);

        while (running_.load() == 1) {
            std::string key = system_meta_service_registry_key();
            std::string val;
            std::unique_ptr<Transaction> txn;
            int tried = 0;
            do {
                TxnErrorCode err = txn_kv_->create_txn(&txn);
                if (err != TxnErrorCode::TXN_OK) break;
                err = txn->get(key, &val);
                if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) break;
                ServiceRegistryPB reg;
                if (err == TxnErrorCode::TXN_OK && !reg.ParseFromString(val)) break;
                LOG_EVERY_N(INFO, 100)
                        << "get server registry, key=" << hex(key) << " reg=" << proto_to_json(reg);
                prepare_registry(&reg);
                val = reg.SerializeAsString();
                if (val.empty()) break;
                txn->put(key, val);
                LOG_EVERY_N(INFO, 100)
                        << "put server registry, key=" << hex(key) << " reg=" << proto_to_json(reg);
                err = txn->commit();
                if (err != TxnErrorCode::TXN_OK) {
                    LOG(WARNING) << "failed to commit registry, key=" << hex(key)
                                 << " val=" << proto_to_json(reg) << " retry times=" << ++tried;
                    std::this_thread::sleep_for(std::chrono::milliseconds(rd_len(gen)));
                    continue;
                }
            } while (false);
            std::unique_lock l(mtx_);
            cv_.wait_for(l, std::chrono::milliseconds(config::meta_server_register_interval_ms));
        }
        LOG(INFO) << "register thread quits";
    }));
    pthread_setname_np(register_thread_->native_handle(), "ms_register_thread");
}

MetaServerRegister::~MetaServerRegister() {
    stop();
}

int MetaServerRegister::start() {
    if (txn_kv_ == nullptr) return -1;
    std::unique_lock<std::mutex> lock(mtx_);
    running_.store(1);
    cv_.notify_all();
    return 0;
}

void MetaServerRegister::stop() {
    {
        std::unique_lock<std::mutex> lock(mtx_);
        running_.store(2);
        cv_.notify_all();
    }
    if (register_thread_ != nullptr && register_thread_->joinable()) {
        register_thread_->join();
        register_thread_.reset();
    }
}

MetaServerInstanceWatcher::~MetaServerInstanceWatcher() {
    stop();
}

int MetaServerInstanceWatcher::start() {
    if (txn_kv_ == nullptr) return -1;
    std::unique_lock<std::mutex> lock(mtx_);
    if (running_.load() != 0) {
        LOG(INFO) << "instance watcher already started";
        return -2;
    }
    running_.store(1);
    instance_watch_thread_ = std::thread(&MetaServerInstanceWatcher::watch_instance_loop, this);
    return 0;
}

void MetaServerInstanceWatcher::stop() {
    {
        std::unique_lock<std::mutex> lock(mtx_);
        if (running_.load() == 2) {
            LOG(INFO) << "instance watcher already stopped";
            return;
        }
        running_.store(2);
        cv_.notify_all();
    }
    if (instance_watch_thread_.joinable()) {
        TxnErrorCode err = trigger_instance_watch();
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to trigger instance watch when stopping instance watcher";
        }
        instance_watch_thread_.join();
    }
}

void MetaServerInstanceWatcher::watch_instance_loop() {
    pthread_setname_np(pthread_self(), "ms_instance_watcher");

    LOG(INFO) << "instance watch thread begins to run";

    constexpr int64_t MIN_SLEEP_INTERVAL_MS = 100;   // 100 microsecond
    constexpr int64_t MAX_SLEEP_INTERVAL_MS = 10000; // 10 second

    std::string original_value;
    std::unordered_map<std::string, std::string> instance_kvs_;

    int64_t sleep_intervals_ms = MIN_SLEEP_INTERVAL_MS;
    while (running_.load() == 1) {
        TxnErrorCode err = watch_instance(instance_kvs_, original_value);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to watch instance, err=" << err;
            std::unique_lock l(mtx_);
            cv_.wait_for(l, std::chrono::milliseconds(sleep_intervals_ms));
            sleep_intervals_ms = std::min(sleep_intervals_ms * 2, MAX_SLEEP_INTERVAL_MS);
            continue;
        }
        sleep_intervals_ms = MIN_SLEEP_INTERVAL_MS;
    }
    LOG(INFO) << "instance watch thread quits";
}

TxnErrorCode MetaServerInstanceWatcher::watch_instance(
        std::unordered_map<std::string, std::string>& instance_kvs_, std::string& original_value) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn in watch_instance, err=" << err;
        return err;
    }

    std::string key = system_meta_service_instance_update_key();
    std::string new_value;
    err = txn->get(key, &new_value);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG(WARNING) << "failed to get instance update key in watch_instance, err=" << err;
        return err;
    } else if (err == TxnErrorCode::TXN_OK && new_value != original_value) {
        LOG(INFO) << "instance update key changed, old_value=" << hex(original_value)
                  << " new_value=" << hex(new_value);
        err = scan_and_notify_instance_updates(instance_kvs_);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to scan and notify instance updates in watch_instance, err="
                         << err;
            return err;
        }

        // The instance update key is changed, update the original value, and continue to watch
        original_value = new_value;
        return TxnErrorCode::TXN_OK;
    }

    err = txn->watch_key(key);
    if (err == TxnErrorCode::TXN_TOO_MANY_WATCHES) {
        LOG(ERROR) << "too many watches registered in watch_instance, you may need to increase "
                      "the limitation";
        return err;
    } else if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to watch instance update key in watch_instance, err=" << err;
        return err;
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaServerInstanceWatcher::scan_and_notify_instance_updates(
        std::unordered_map<std::string, std::string>& instance_kvs_) {
    StopWatch sw;
    DORIS_CLOUD_DEFER {
        g_bvar_ms_scan_instance_update << sw.elapsed_us();
    };

    std::string begin_key = instance_key({""});
    std::string end_key = instance_key({"\xff"}); // instance id are human readable strings

    FullRangeGetOptions opts;
    opts.txn_kv = txn_kv_;
    opts.prefetch = true;
    auto it = txn_kv_->full_range_get(begin_key, end_key, opts);

    std::unordered_map<std::string, std::string> new_instance_kvs;
    for (auto&& kvp = it->next(); kvp.has_value(); kvp = it->next()) {
        std::string_view key = kvp->first;
        std::string instance_id;
        if (!decode_instance_key(&key, &instance_id)) {
            LOG(WARNING) << "failed to decode instance key in watch_instance, key="
                         << hex(kvp->first);
            return TxnErrorCode::TXN_INVALID_DATA;
        }
        new_instance_kvs.emplace(std::move(instance_id), kvp->second);
    }
    if (!it->is_valid()) {
        return it->error_code();
    }

    for (auto&& [id, v] : new_instance_kvs) {
        auto it = instance_kvs_.find(id);
        if (it == instance_kvs_.end() || it->second != v) {
            // New instance or updated instance info, notify resource manager to refresh
            //
            // The ResourceManager::refresh_instance() will new txn to get instance info,
            // in order to avoid refreshing instance info with stale data.
            auto&& [code, msg] = resource_manager_->refresh_instance(id);
            if (code != MetaServiceCode::OK) {
                LOG(WARNING) << "failed to refresh instance in watch_instance, instance_id=" << id
                             << " code=" << code << " msg=" << msg;
                return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
            }
        }
    }

    instance_kvs_.swap(new_instance_kvs);

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MetaServerInstanceWatcher::trigger_instance_watch() {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn to trigger instance watch";
        return err;
    }
    txn->atomic_add(system_meta_service_instance_update_key(), 1);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to commit txn to trigger instance watch";
        return err;
    }
    return TxnErrorCode::TXN_OK;
}

} // namespace doris::cloud
