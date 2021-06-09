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

#ifndef DORIS_BE_SRC_COMMON_UTIL_DEFAULT_PATH_HANDLERS_H
#define DORIS_BE_SRC_COMMON_UTIL_DEFAULT_PATH_HANDLERS_H

#include <stdio.h>

#include <memory>

namespace doris {

class MemTracker;
class WebPageHandler;

// Adds a set of default path handlers to the webserver to display
// logs and configuration flags
void add_default_path_handlers(WebPageHandler* web_page_handler,
                               const std::shared_ptr<MemTracker>& process_mem_tracker);
} // namespace doris

#endif // IMPALA_UTIL_DEFAULT_PATH_HANDLERS_H
