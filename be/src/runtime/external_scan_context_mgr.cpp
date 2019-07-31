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

#include <chrono>
#include <functional>

#include "runtime/fragment_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "util/uid_util.h"

namespace doris {

ExternalScanContextMgr::ExternalScanContextMgr(ExecEnv* exec_env) : _exec_env(exec_env), _is_stop(false), _scan_context_gc_interval_min(doris::config::scan_context_gc_interval_min) {
    // start the reaper thread for gc the expired context
    _keep_alive_reaper.reset(
            new std::thread(
                    std::bind<void>(std::mem_fn(&ExternalScanContextMgr::gc_expired_context), this)));
}

Status ExternalScanContextMgr::create_scan_context(std::shared_ptr<Context>* p_context) {
    std::string context_id = generate_uuid_string();
    std::shared_ptr<Context> context(new Context(context_id));
    // context->last_access_time  = time(NULL);
    {
        std::lock_guard<std::mutex> l(_lock);        
        _active_contexts.insert(std::make_pair(context_id, context));
    }
    *p_context = context;
    return Status::OK();
}

Status ExternalScanContextMgr::get_scan_context(const std::string& context_id, std::shared_ptr<Context>* p_context) {
    {
        std::lock_guard<std::mutex> l(_lock);        
        auto iter = _active_contexts.find(context_id);
        if (iter != _active_contexts.end()) {
            *p_context = iter->second;
        } else {
            LOG(WARNING) << "get_scan_context error: context id [ " << context_id << " ] not found";
            std::stringstream msg;
            msg << "context_id: " << context_id << " not found"; 
            return  Status::NotFound(msg.str());
        }
    }
    return Status::OK();
}

Status ExternalScanContextMgr::clear_scan_context(const std::string& context_id) {
    std::shared_ptr<Context> context;
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _active_contexts.find(context_id);
    if (iter != _active_contexts.end()) {
        context = iter->second;
        // maybe do not this context-local-lock
        {
            std::lock_guard<std::mutex> l(context->_local_lock);
            // first cancel the fragment instance, just ignore return status
            _exec_env->fragment_mgr()->cancel(context->fragment_instance_id);
            // clear the fragment instance's related result queue
            _exec_env->result_queue_mgr()->cancel(context->fragment_instance_id);
        }
        iter = _active_contexts.erase(iter);
        LOG(INFO) << "close scan context: context id [ " << context_id << " ]";
    }
    return Status::OK();
}

void ExternalScanContextMgr::gc_expired_context() {
    while (!_is_stop) {
        std::this_thread::sleep_for(std::chrono::seconds(_scan_context_gc_interval_min * 60));
        time_t current_time = time(NULL);
        std::lock_guard<std::mutex> l(_lock);
        for(auto iter = _active_contexts.begin(); iter != _active_contexts.end(); ) {
            TUniqueId fragment_instance_id = iter->second->fragment_instance_id;
            auto context = iter->second;
            if (context == nullptr) {
                iter = _active_contexts.erase(iter);
                continue;
            }
            {
                // This lock maybe should deleted, all right? 
                // here we do not need lock guard in fact
                std::lock_guard<std::mutex> l(context->_local_lock);        
                // being processed or timeout is disabled
                if (context->last_access_time == -1) {
                    continue;
                }
                // free context if context is idle for context->keep_alive_min
                if (current_time - context->last_access_time > context->keep_alive_min * 60) {
                    LOG(INFO) << "gc expired scan context: context id [ " << context->context_id << " ]";
                    // must cancel the fragment instance, otherwise return thrift transport TTransportException
                    _exec_env->fragment_mgr()->cancel(context->fragment_instance_id);
                    _exec_env->result_queue_mgr()->cancel(context->fragment_instance_id);
                    iter = _active_contexts.erase(iter);
                }
            }
        }
    }
}
}