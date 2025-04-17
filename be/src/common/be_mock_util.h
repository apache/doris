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
#include "util/defer_op.h"

namespace doris {

// #define BE_TEST

#ifdef BE_TEST
#define MOCK_FUNCTION virtual
#else
#define MOCK_FUNCTION
#endif

#ifdef BE_TEST
#define MOCK_DEFINE(str) str
#else
#define MOCK_DEFINE(str)
#endif

#ifdef BE_TEST
#define MOCK_REMOVE(str)
#else
#define MOCK_REMOVE(str) str
#endif

void mock_random_sleep();

#ifdef BE_TEST
#define INJECT_MOCK_SLEEP(lock_guard) \
    DEFER(mock_random_sleep());       \
    mock_random_sleep();              \
    lock_guard;
#else
#define INJECT_MOCK_SLEEP(lock_guard) lock_guard
#endif

} // namespace doris

/*
#include "common/be_mock_util.h"
*/
