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

#ifndef IMPALA_COMMON_LOGGING_H
#define IMPALA_COMMON_LOGGING_H

// This is a wrapper around the glog header.  When we are compiling to IR,
// we don't want to pull in the glog headers.  Pulling them in causes linking
// issues when we try to dynamically link the codegen'd functions.
#ifdef IR_COMPILE
#include <iostream>
#define DCHECK(condition) \
    while (false) std::cout
#define DCHECK_EQ(a, b) \
    while (false) std::cout
#define DCHECK_NE(a, b) \
    while (false) std::cout
#define DCHECK_GT(a, b) \
    while (false) std::cout
#define DCHECK_LT(a, b) \
    while (false) std::cout
#define DCHECK_GE(a, b) \
    while (false) std::cout
#define DCHECK_LE(a, b) \
    while (false) std::cout
// Similar to how glog defines DCHECK for release.
#define LOG(level) \
    while (false) std::cout
#define VLOG(level) \
    while (false) std::cout
#else
// GLOG defines this based on the system but doesn't check if it's already
// been defined.  undef it first to avoid warnings.
// glog MUST be included before gflags.  Instead of including them,
// our files should include this file instead.
#undef _XOPEN_SOURCE
// This is including a glog internal file.  We want this to expose the
// function to get the stack trace.
#include <glog/logging.h>
#undef MutexLock
#endif

// Define VLOG levels.  We want display per-row info less than per-file which
// is less than per-query.  For now per-connection is the same as per-query.
#define VLOG_CONNECTION VLOG(1)
#define VLOG_RPC VLOG(8)
#define VLOG_QUERY VLOG(1)
#define VLOG_FILE VLOG(2)
#define VLOG_ROW VLOG(10)
#define VLOG_PROGRESS VLOG(2)
#define VLOG_TRACE VLOG(10)
#define VLOG_DEBUG VLOG(7)
#define VLOG_NOTICE VLOG(3)
#define VLOG_CRITICAL VLOG(1)


#define VLOG_CONNECTION_IS_ON VLOG_IS_ON(1)
#define VLOG_RPC_IS_ON VLOG_IS_ON(8)
#define VLOG_QUERY_IS_ON VLOG_IS_ON(1)
#define VLOG_FILE_IS_ON VLOG_IS_ON(2)
#define VLOG_ROW_IS_ON VLOG_IS_ON(10)
#define VLOG_TRACE_IS_ON VLOG_IS_ON(10)
#define VLOG_DEBUG_IS_ON VLOG_IS_ON(7)
#define VLOG_NOTICE_IS_ON VLOG_IS_ON(3)
#define VLOG_CRITICAL_IS_ON VLOG_IS_ON(1)


/// Define a wrapper around DCHECK for strongly typed enums that print a useful error
/// message on failure.
#define DCHECK_ENUM_EQ(a, b)                                                 \
    DCHECK(a == b) << "[ " #a " = " << static_cast<int>(a) << " , " #b " = " \
                   << static_cast<int>(b) << " ]"

#include "fmt/format.h"
#endif
