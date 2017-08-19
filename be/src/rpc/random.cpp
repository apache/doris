// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "compat.h"
#include "random.h"

#include <cassert>
#include <mutex>
#include <random>

#define LOCK_GLOBAL_MUTEX(m) (void)m

namespace palo {

std::mt19937 g_random_engine {1};
std::mutex g_random_mutex;

void Random::seed(unsigned int s) {
    LOCK_GLOBAL_MUTEX(g_random_mutex);
    g_random_engine.seed(s);
}

uint32_t Random::number32(uint32_t maximum) {
    LOCK_GLOBAL_MUTEX(g_random_mutex);
    if (maximum) {
        return std::uniform_int_distribution<uint32_t>(0, maximum-1)(g_random_engine);
    }
    return std::uniform_int_distribution<uint32_t>()(g_random_engine);
}

int64_t Random::number64(int64_t maximum) {
    LOCK_GLOBAL_MUTEX(g_random_mutex);
    if (maximum) {
        assert(maximum > 0);
        return std::uniform_int_distribution<int64_t>(0, maximum-1)(g_random_engine);
    }
    return std::uniform_int_distribution<int64_t>()(g_random_engine);
}

double Random::uniform01() {
    LOCK_GLOBAL_MUTEX(g_random_mutex);
    return std::uniform_real_distribution<>()(g_random_engine);
}

std::chrono::milliseconds Random::duration_millis(uint32_t maximum) {
    LOCK_GLOBAL_MUTEX(g_random_mutex);
    assert(maximum > 0);
    std::uniform_int_distribution<uint32_t> di(0, maximum-1);
    return std::chrono::milliseconds(di(g_random_engine));
}

}
