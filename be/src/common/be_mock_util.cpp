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

#include "be_mock_util.h"

#include <chrono>
#include <random>
#include <thread>
#include <vector>

namespace doris {
#include "common/compile_check_begin.h"
void mock_random_sleep() {
    std::vector<int> sleepDurations = {0, 0, 0, 0, 50};
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, (int)sleepDurations.size() - 1);

    int sleepTime = sleepDurations[dis(gen)];
    std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
}
} // namespace doris
#include "common/compile_check_end.h"