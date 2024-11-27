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

#include <chrono>
#include <iostream>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "common/status.h"

namespace doris {

// Same as
// fe/fe-core/src/main/java/org/apache/doris/common/DNSCache.java
class DNSCache {
public:
    DNSCache();
    ~DNSCache();

    // get ip by hostname
    Status get(const std::string& hostname, std::string* ip);

private:
    // update the ip of hostname in cache
    Status _update(const std::string& hostname);

    // a function for refresh daemon thread
    // update cache at fix internal
    void _refresh_cache();

private:
    // hostname -> ip
    std::unordered_map<std::string, std::string> cache;
    mutable std::shared_mutex mutex;
    std::thread refresh_thread;
    bool stop_refresh = false;
};

} // end of namespace doris
