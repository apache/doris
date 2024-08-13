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

#include "runtime/external_scan_context_mgr.h"

#include <glog/logging.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <ostream>
#include <vector>

#include "common/config.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"
#include "util/thread.h"
#include "util/uid_util.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(active_scan_context_count, MetricUnit::NOUNIT);

ExternalScanContextMgr::ExternalScanContextMgr(ExecEnv* exec_env)
        : _exec_env(exec_env), _stop_background_threads_latch(1) {
    // start the reaper thread for gc the expired context
    CHECK(Thread::create(
                  "ExternalScanContextMgr", "gc_expired_context",
                  [this]() { this->gc_expired_context(); }, &_keep_alive_reaper)
                  .ok());

    REGISTER_HOOK_METRIC(active_scan_context_count, [this]() {
        // std::lock_guard<std::mutex> l(_lock);
        return _active_contexts.size();
    });
}

void ExternalScanContextMgr::stop() {
    DEREGISTER_HOOK_METRIC(active_scan_context_count);
    _stop_background_threads_latch.count_down();
    if (_keep_alive_reaper) {
        _keep_alive_reaper->join();
    }
}

Status ExternalScanContextMgr::create_scan_context(std::shared_ptr<ScanContext>* p_context) {
    std::string context_id = generate_uuid_string();
    std::shared_ptr<ScanContext> context(new ScanContext(context_id));
    // context->last_access_time  = time(nullptr);
    {
        std::lock_guard<std::mutex> l(_lock);
        _active_contexts.insert(std::make_pair(context_id, context));
    }
    *p_context = context;
    return Status::OK();
}

Status ExternalScanContextMgr::get_scan_context(const std::string& context_id,
                                                std::shared_ptr<ScanContext>* p_context) {
    {
        std::lock_guard<std::mutex> l(_lock);
        auto iter = _active_contexts.find(context_id);
        if (iter != _active_contexts.end()) {
            *p_context = iter->second;
        } else {
            LOG(WARNING) << "get_scan_context error: context id [ " << context_id << " ] not found";
            std::stringstream msg;
            msg << "context_id: " << context_id << " not found";
            return Status::NotFound(msg.str());
        }
    }
    return Status::OK();
}

Status ExternalScanContextMgr::clear_scan_context(const std::string& context_id) {
    std::shared_ptr<ScanContext> context;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto iter = _active_contexts.find(context_id);
        if (iter != _active_contexts.end()) {
            context = iter->second;
            if (context == nullptr) {
                _active_contexts.erase(context_id);
                return Status::OK();
            }
            iter = _active_contexts.erase(iter);
        }
    }
    if (context != nullptr) {
        // first cancel the fragment instance, just ignore return status
        _exec_env->fragment_mgr()->cancel_query(context->query_id,
                                                Status::InternalError("cancelled by clear thread"));
        // clear the fragment instance's related result queue
        static_cast<void>(_exec_env->result_queue_mgr()->cancel(context->fragment_instance_id));
        LOG(INFO) << "close scan context: context id [ " << context_id << " ]";
    }
    return Status::OK();
}

void ExternalScanContextMgr::gc_expired_context() {
#ifndef BE_TEST
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(doris::config::scan_context_gc_interval_min * 60))) {
        time_t current_time = time(nullptr);
        std::vector<std::shared_ptr<ScanContext>> expired_contexts;
        {
            std::lock_guard<std::mutex> l(_lock);
            for (auto iter = _active_contexts.begin(); iter != _active_contexts.end();) {
                auto context = iter->second;
                if (context == nullptr) {
                    iter = _active_contexts.erase(iter);
                    continue;
                }
                // being processed or timeout is disabled
                if (context->last_access_time == -1) {
                    ++iter; // advance one entry
                    continue;
                }
                // free context if context is idle for context->keep_alive_min
                if (current_time - context->last_access_time > context->keep_alive_min * 60) {
                    LOG(INFO) << "gc expired scan context: context id [ " << context->context_id
                              << " ]";
                    expired_contexts.push_back(context);
                    iter = _active_contexts.erase(iter);
                } else {
                    ++iter; // advanced
                }
            }
        }
        for (auto expired_context : expired_contexts) {
            // must cancel the fragment instance, otherwise return thrift transport TTransportException
            _exec_env->fragment_mgr()->cancel_query(
                    expired_context->query_id, Status::InternalError("scan context is expired"));
            static_cast<void>(
                    _exec_env->result_queue_mgr()->cancel(expired_context->fragment_instance_id));
        }
    }
#endif
}
} // namespace doris
