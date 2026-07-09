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

#include "util/stack_util.h"

#include "common/stack_trace.h"
#include "util/mem_info.h"
#include "util/pretty_printer.h"

namespace doris {

std::string get_stack_trace(int start_pointers_index, std::string dwarf_location_info_mode) {
#ifndef BE_TEST
    if (!config::enable_stacktrace) {
        return "no enable stacktrace";
    }
#endif
    if (dwarf_location_info_mode.empty()) {
        dwarf_location_info_mode = config::dwarf_location_info_mode;
    }

    return "\n" + StackTrace().toString(start_pointers_index, dwarf_location_info_mode);
}

} // namespace doris
