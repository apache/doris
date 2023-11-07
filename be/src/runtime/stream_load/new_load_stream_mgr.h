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

#include <iterator>
#include <memory>
#include <mutex>
#include <ostream>
#include <unordered_map>
#include <utility>

#include "common/factory_creator.h"
#include "common/logging.h"
#include "common/status.h"
#include "util/uid_util.h"

namespace doris {

class StreamLoadContext;

// used to register all streams in process so that other module can get this stream
class NewLoadStreamMgr {
    ENABLE_FACTORY_CREATOR(NewLoadStreamMgr);

public:
    NewLoadStreamMgr();
    ~NewLoadStreamMgr();

    Status put(const UniqueId& id, std::shared_ptr<StreamLoadContext> stream) {
        {
            std::lock_guard<std::mutex> l(_lock);
            if (auto iter = _stream_map.find(id); iter != _stream_map.end()) {
                std::stringstream ss;
                ss << "id: " << id << " already exist";
                return Status::InternalError(ss.str());
            }
            _stream_map.emplace(id, stream);
        }

        LOG(INFO) << "put stream load pipe: " << id;
        return Status::OK();
    }

    std::shared_ptr<StreamLoadContext> get(const UniqueId& id) {
        {
            std::lock_guard<std::mutex> l(_lock);
            if (auto iter = _stream_map.find(id); iter != _stream_map.end()) {
                return iter->second;
            }
        }
        LOG(INFO) << "stream load pipe does not exist: " << id;
        return nullptr;
    }

    void remove(const UniqueId& id) {
        std::lock_guard<std::mutex> l(_lock);
        if (auto iter = _stream_map.find(id); iter != _stream_map.end()) {
            _stream_map.erase(iter);
            LOG(INFO) << "remove stream load pipe: " << id;
        }
    }

private:
    std::mutex _lock;
    std::unordered_map<UniqueId, std::shared_ptr<StreamLoadContext>> _stream_map;
};
} // namespace doris
