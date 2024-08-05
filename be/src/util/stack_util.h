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

#include <string>

namespace doris {

/** Returns the stack trace as a string from the current location.
  */

// Select a stack trace tool according to config::get_stack_trace_tool
// glog: 1000 times cost 8min, no line numbers.
// boost: 1000 times cost 1min, has line numbers, but has memory leak.
// glibc: 1000 times cost 1min, no line numbers, unresolved backtrace symbol.
// libunwind: cost is negligible, has line numbers.
std::string get_stack_trace(int start_pointers_index = 0,
                            std::string dwarf_location_info_mode = "");

// Note: there is a libc bug that causes this not to work on 64 bit machines
// for recursive calls.
std::string get_stack_trace_by_glog();

// `boost::stacktrace::stacktrace()` has memory leak, reason for the boost::stacktrace memory leak
// is that a state is saved in the thread local of each thread but is not actively released. Refer to:
// https://github.com/boostorg/stacktrace/issues/118
// https://github.com/boostorg/stacktrace/issues/111
std::string get_stack_trace_by_boost();

// backtrace symbol no parsing with Addr2Line, this is slower and often make mistakes, requiring manual parsing
// https://stackoverflow.com/questions/3151779/best-way-to-invoke-gdb-from-inside-program-to-print-its-stacktrace/4611112#4611112
// https://stackoverflow.com/questions/55450932/how-ro-resolve-cpp-symbols-from-backtrace-symbols-in-the-offset-during-runtime
std::string get_stack_trace_by_glibc();

// use StackTraceCache, PHDRCache speed up, is customizable and has some optimizations.
// TODO:
//  1. currently support linux __x86_64__, __arm__, __powerpc__, not supported __FreeBSD__, APPLE
//     Note: __arm__, __powerpc__ not been verified
//  2. Support signal handle
//  3. libunwid support unw_backtrace for jemalloc
//  4. Use of undefined compile option USE_MUSL for later
std::string get_stack_trace_by_libunwind(int start_pointers_index,
                                         const std::string& dwarf_location_info_mode);

} // namespace doris
