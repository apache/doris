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

#include <execinfo.h>
#include <signal.h>
#include <stdio.h>

#include <boost/stacktrace.hpp>

#include "common/stack_trace.h"
#include "util/mem_info.h"
#include "util/pretty_printer.h"

namespace google {
namespace glog_internal_namespace_ {
void DumpStackTraceToString(std::string* stacktrace);
}
} // namespace google

namespace doris {

std::string get_stack_trace() {
#ifdef ENABLE_STACKTRACE
    auto tool = config::get_stack_trace_tool;
    if (tool == "glog") {
        return get_stack_trace_by_glog();
    } else if (tool == "boost") {
        return get_stack_trace_by_boost();
    } else if (tool == "glibc") {
        return get_stack_trace_by_glibc();
    } else if (tool == "libunwind") {
#if defined(__APPLE__) // TODO
        return get_stack_trace_by_glog();
#endif
        return get_stack_trace_by_libunwind();
    } else {
        return "no stack";
    }
#endif
    return "no enable stack";
}

std::string get_stack_trace_by_glog() {
    std::string s;
    google::glog_internal_namespace_::DumpStackTraceToString(&s);
    return s;
}

std::string get_stack_trace_by_boost() {
    return boost::stacktrace::to_string(boost::stacktrace::stacktrace());
}

std::string get_stack_trace_by_glibc() {
    void* trace[16];
    char** messages = (char**)nullptr;
    int i, trace_size = 0;

    trace_size = backtrace(trace, 16);
    messages = backtrace_symbols(trace, trace_size);
    std::stringstream out;
    for (i = 1; i < trace_size; ++i) {
        out << messages[i] << "\n";
    }
    return out.str();
}

std::string get_stack_trace_by_libunwind() {
    return "\n" + StackTrace().toString();
}

} // namespace doris
