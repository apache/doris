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

#include <cstdlib>
#include <sys/time.h>
#include <thread>
#include "common/thread_fuzzy.h"

namespace doris {

int ThreadFuzzer::setup() {
    struct sigaction sa;

    sa.sa_flags = SA_RESTART;
    sa.sa_handler = prof_handler;

#if defined(OS_LINUX)
    if (sigemptyset(&sa.sa_mask)) {
        LOG(WARNING) << "sigempty failed, errno = " << errno;
        return -1;
    }

    if (sigaddset(&sa.sa_mask, SIGPROF)) {
        LOG(WARNING) << "sigaddset failed, errno = " << errno;
        return -1;
    }
#endif

    std::srand(std::time(nullptr));

    struct timeval interval;
    interval.tv_sec = 0;
    interval.tv_usec = 20000; // 20ms

    struct itimerval timer = {.it_interval = interval, .it_value = interval};

    if (0 != setitimer(ITIMER_PROF, &timer, nullptr)) {
        LOG(WARNING) << "setitimer failed, errno = " << errno;
        return -1;
    }

    return 0;
}

void ThreadFuzzer::prof_handler(int) {
    auto saved_errno = errno;

    sched_yield();
    std::this_thread::sleep_for(std::chrono::microseconds(std::rand()%10));

    errno = saved_errno;
}

} // doris
