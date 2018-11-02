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

#ifndef DORIS_BE_SRC_COMMON_UTIL_COUNT_DOWN_LATCH_H
#define DORIS_BE_SRC_COMMON_UTIL_COUNT_DOWN_LATCH_H

#include <errno.h>
#include <pthread.h>

namespace doris {

class CountDownLatch {
public:
    CountDownLatch(int count) : _count(count) {
        pthread_mutex_init(&_lock, nullptr);
        pthread_cond_init(&_cond, nullptr);
    }

    ~CountDownLatch() {
        // User must make sure no thread call await now.
        // std::shared_ptr is a choice
        pthread_mutex_destroy(&_lock);
        pthread_cond_destroy(&_cond);
    }

    void count_down(int count) {
        pthread_mutex_lock(&_lock);
        _count -= count;
        if (_count <= 0) {
            pthread_cond_broadcast(&_cond);
        }
        pthread_mutex_unlock(&_lock);
    }

    void count_down() {
        count_down(1);
    }

    // 0 means ok
    // -1 means fail
    void await() {
        pthread_mutex_lock(&_lock);
        while (_count > 0) {
            pthread_cond_wait(&_cond, &_lock);
        }
        pthread_mutex_unlock(&_lock);
    }

    // 0 means ok
    // -1 means: timeout
    int await(int64_t timeout_ms) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += timeout_ms / 1000;
        ts.tv_nsec += (timeout_ms % 1000) * 1000000;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec += ts.tv_nsec / 1000000000L;
            ts.tv_nsec = ts.tv_nsec % 1000000000L;
        }
        pthread_mutex_lock(&_lock);
        while (_count > 0) {
            int ret = pthread_cond_timedwait(&_cond, &_lock, &ts);
            if (ret == ETIMEDOUT) {
                pthread_mutex_unlock(&_lock);
                return -1;
            }
        }
        pthread_mutex_unlock(&_lock);
        return 0;
    }

private:
    int _count;
    pthread_mutex_t _lock;
    pthread_cond_t _cond;
};

}

#endif

