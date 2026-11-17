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

#ifdef __APPLE__
#include <pthread.h>
#else
#include <sys/prctl.h>
#endif

namespace doris::cloud {

// Like BE's Thread::set_self_name, this must be called from the thread being named.
inline int set_current_thread_name(const char* name) {
#ifdef __APPLE__
    return pthread_setname_np(name);
#else
    return prctl(PR_SET_NAME, name);
#endif
}

} // namespace doris::cloud
