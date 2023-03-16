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

namespace google {
namespace glog_internal_namespace_ {
void DumpStackTraceToString(std::string* stacktrace);
}
} // namespace google

namespace doris {

// `boost::stacktrace::stacktrace()` has memory leak, so use the glog internal func to print stacktrace.
// The reason for the boost::stacktrace memory leak is that a state is saved in the thread local of each
// thread but is not actively released. Refer to:
// https://github.com/boostorg/stacktrace/issues/118
// https://github.com/boostorg/stacktrace/issues/111
std::string get_stack_trace() {
    std::string s;
    google::glog_internal_namespace_::DumpStackTraceToString(&s);
    return s;
}

} // namespace doris
