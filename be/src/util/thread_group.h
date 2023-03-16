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

#include <list>
#include <shared_mutex>
#include <thread>

#include "common/status.h"

namespace doris {
class ThreadGroup {
public:
    ThreadGroup() {}
    ~ThreadGroup() {
        for (auto thrd : _threads) {
            delete thrd;
        }
    }

    bool is_this_thread_in() const {
        std::thread::id id = std::this_thread::get_id();
        std::shared_lock rdlock(_mutex);
        for (auto const& thrd : _threads) {
            if (thrd->get_id() == id) {
                return true;
            }
        }
        return false;
    }

    bool is_thread_in(std::thread* thrd) const {
        if (thrd) {
            std::thread::id id = thrd->get_id();
            std::shared_lock rdlock(_mutex);
            for (auto const& th : _threads) {
                if (th->get_id() == id) {
                    return true;
                }
            }
            return false;
        } else {
            return false;
        }
    }

    template <typename F>
    std::thread* create_thread(F threadfunc) {
        std::lock_guard<std::shared_mutex> wrlock(_mutex);
        std::unique_ptr<std::thread> new_thread = std::make_unique<std::thread>(threadfunc);
        _threads.push_back(new_thread.get());
        return new_thread.release();
    }

    Status add_thread(std::thread* thrd) {
        if (thrd) {
            if (!is_thread_in(thrd)) {
                std::lock_guard<std::shared_mutex> guard(_mutex);
                _threads.push_back(thrd);
                return Status::OK();
            } else {
                return Status::InvalidArgument("trying to add a duplicated thread");
            }
        } else {
            return Status::InvalidArgument("trying to add a nullptr as thread");
        }
    }

    void remove_thread(std::thread* thrd) {
        std::lock_guard<std::shared_mutex> wrlock(_mutex);
        std::list<std::thread*>::const_iterator it =
                std::find(_threads.begin(), _threads.end(), thrd);
        if (it != _threads.end()) {
            _threads.erase(it);
        }
    }

    Status join_all() {
        if (is_this_thread_in()) {
            return Status::RuntimeError("trying joining itself");
        }
        std::shared_lock rdlock(_mutex);

        for (auto thrd : _threads) {
            if (thrd->joinable()) {
                thrd->join();
            }
        }
        return Status::OK();
    }

    size_t size() const {
        std::shared_lock rdlock(_mutex);
        return _threads.size();
    }

private:
    std::list<std::thread*> _threads;
    mutable std::shared_mutex _mutex;
};
} // namespace doris
