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

#ifndef DORIS_BE_SRC_COMMON_UTIL_LOGGING_H
#define DORIS_BE_SRC_COMMON_UTIL_LOGGING_H

#include <string>
#include "common/logging.h"

namespace doris {

// glog doesn't allow multiple invocations of InitGoogleLogging. This method conditionally
// calls InitGoogleLogging only if it hasn't been called before.
bool init_glog(const char* basename, bool install_signal_handler = false);

// Shuts down the google logging library. Call before exit to ensure that log files are
// flushed. May only be called once.
void shutdown_logging();

}

#endif // DORIS_BE_SRC_COMMON_UTIL_LOGGING_H
