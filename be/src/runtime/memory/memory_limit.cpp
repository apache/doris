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

#include "runtime/memory/memory_limit.h"

#include <glog/logging.h>

#include <atomic>
#include <utility>

#include "common/status.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "util/pretty_printer.h"

namespace doris {
namespace {

class ProcessMemoryLimit final : public MemoryLimit {
public:
    ProcessMemoryLimit() : MemoryLimit(Scope::PROCESS, nullptr) {}

private:
    bool exceeds_local_memory_limit(int64_t bytes) override {
        return GlobalMemoryArbitrator::is_exceed_hard_mem_limit(bytes);
    }

    Status check_local_memory_limit(int64_t bytes) override {
        if (exceeds_local_memory_limit(bytes)) {
            return Status::Error<ErrorCode::PROCESS_MEMORY_EXCEEDED>(
                    "process memory limit exceeded, size: {}, {}",
                    PrettyPrinter::print_bytes(bytes), local_memory_limit_string());
        }
        return Status::OK();
    }

    Status reserve_local_memory(int64_t bytes, bool check_limit) override {
        if (check_limit) {
            if (!GlobalMemoryArbitrator::try_reserve_process_memory(bytes)) {
                return Status::Error<ErrorCode::PROCESS_MEMORY_EXCEEDED>(
                        "reserve memory failed, size: {}, because process memory exceeded, {}",
                        PrettyPrinter::print_bytes(bytes), local_memory_limit_string());
            }
        } else {
            GlobalMemoryArbitrator::reserve_process_memory(bytes);
        }
        return Status::OK();
    }

    void rollback_local_reservation(int64_t bytes) override {
        GlobalMemoryArbitrator::shrink_process_reserved(bytes);
    }

    std::string local_memory_limit_string() const override {
        return "Process[" + GlobalMemoryArbitrator::process_mem_log_str() + "]";
    }
};

} // namespace

MemoryLimit::MemoryLimit(Scope scope, std::shared_ptr<MemoryLimit> parent)
        : _scope(scope), _parent(std::move(parent)) {}

std::shared_ptr<MemoryLimit> MemoryLimit::memory_limit_parent() const {
    return _parent.load();
}

void MemoryLimit::set_memory_limit_parent(std::shared_ptr<MemoryLimit> parent) {
    DCHECK(parent);
    // Strictly increasing scopes prevent ownership cycles and recursive loops.
    DCHECK_GT(static_cast<int>(parent->memory_limit_scope()), static_cast<int>(_scope));
    _parent.store(std::move(parent));
}

const std::shared_ptr<MemoryLimit>& MemoryLimit::process_memory_limit() {
    static const std::shared_ptr<MemoryLimit> process = std::make_shared<ProcessMemoryLimit>();
    return process;
}

Status MemoryLimit::check_memory_limit(int64_t bytes, CheckScope checker) {
    const int checks = static_cast<int>(checker);
    if (checks & static_cast<int>(_scope)) {
        RETURN_IF_ERROR(check_local_memory_limit(bytes));
    }
    // Ordinary task-only allocation checks do not load/traverse parent pointers.
    if (checks >= static_cast<int>(_scope) * 2) {
        if (auto parent = memory_limit_parent()) {
            return parent->check_memory_limit(bytes, checker);
        }
    }
    return Status::OK();
}

bool MemoryLimit::exceeds_memory_limit(int64_t bytes, CheckScope checker) {
    const int checks = static_cast<int>(checker);
    if ((checks & static_cast<int>(_scope)) && exceeds_local_memory_limit(bytes)) {
        return true;
    }
    if (checks >= static_cast<int>(_scope) * 2) {
        if (auto parent = memory_limit_parent()) {
            return parent->exceeds_memory_limit(bytes, checker);
        }
    }
    return false;
}

Status MemoryLimit::try_reserve_memory(int64_t bytes, CheckScope checker,
                                       MemoryLimit* parent_override) {
    DCHECK_GE(bytes, 0);
    RETURN_IF_ERROR(
            reserve_local_memory(bytes, static_cast<int>(checker) & static_cast<int>(_scope)));
    auto parent = parent_override ? nullptr : memory_limit_parent();
    auto* next = parent_override ? parent_override : parent.get();
    if (next) {
        DCHECK_GT(static_cast<int>(next->memory_limit_scope()), static_cast<int>(_scope));
        auto status = next->try_reserve_memory(bytes, checker);
        if (!status.ok()) {
            rollback_local_reservation(bytes);
            return status;
        }
    }
    return Status::OK();
}

std::string MemoryLimit::memory_limit_tree_string() const {
    auto result = local_memory_limit_string();
    if (auto parent = memory_limit_parent()) {
        result += " -> " + parent->memory_limit_tree_string();
    }
    return result;
}

} // namespace doris
