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

#ifndef DORIS_BE_SRC_RUNTIME_CACHE_UTILS_H
#define DORIS_BE_SRC_RUNTIME_CACHE_UTILS_H

#include <gutil/integral_types.h>
#include <sys/time.h>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <list>
#include <map>
#include <shared_mutex>
#include <thread>

namespace doris {

typedef std::shared_lock<std::shared_mutex> CacheReadLock;
typedef std::unique_lock<std::shared_mutex> CacheWriteLock;

//#ifndef PARTITION_CACHE_DEV
//#define PARTITION_CACHE_DEV
//#endif

struct CacheStat {
    static const uint32 DAY_SECONDS = 86400;
    long cache_time;
    long last_update_time;
    long last_read_time;
    uint32 read_count;
    CacheStat() { init(); }

    long cache_time_second() {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        return tv.tv_sec;
    }

    void init() {
        cache_time = 0;
        last_update_time = 0;
        last_read_time = 0;
        read_count = 0;
    }

    void update() {
        last_update_time = cache_time_second();
        if (cache_time == 0) {
            cache_time = last_update_time;
        }
        last_read_time = last_update_time;
        read_count++;
    }

    void query() {
        last_read_time = cache_time_second();
        read_count++;
    }

    double last_query_day() { return (cache_time_second() - last_read_time) * 1.0 / DAY_SECONDS; }

    double avg_query_count() {
        return read_count * DAY_SECONDS * 1.0 / (cache_time_second() - last_read_time + 1);
    }
};

} // namespace doris
#endif
