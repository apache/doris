/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef GTEST_WRAPPER_H
#define GTEST_WRAPPER_H

#include "Adaptor.hh"

// we need to disable a whole set of warnings as we include gtest.h
// restore most of the warnings after the file is loaded.

#if defined(__GNUC__) || defined(__clang__)
DIAGNOSTIC_IGNORE("-Wsign-compare")
#endif

#ifdef __clang__
DIAGNOSTIC_IGNORE("-Wconversion-null")
DIAGNOSTIC_IGNORE("-Wexit-time-destructors")
DIAGNOSTIC_IGNORE("-Wglobal-constructors")
DIAGNOSTIC_IGNORE("-Wunknown-warning-option")
DIAGNOSTIC_IGNORE("-Wused-but-marked-unused")
DIAGNOSTIC_IGNORE("-Wzero-as-null-pointer-constant")
#endif

DIAGNOSTIC_PUSH

#if defined(__GNUC__) || defined(__clang__)
DIAGNOSTIC_IGNORE("-Wdeprecated")
DIAGNOSTIC_IGNORE("-Wmissing-noreturn")
DIAGNOSTIC_IGNORE("-Wpadded")
DIAGNOSTIC_IGNORE("-Wsign-compare")
DIAGNOSTIC_IGNORE("-Wundef")
#endif

#ifdef __clang__
DIAGNOSTIC_IGNORE("-Wshift-sign-overflow")
DIAGNOSTIC_IGNORE("-Wused-but-marked-unused")
DIAGNOSTIC_IGNORE("-Wweak-vtables")
#endif

#ifdef _MSC_VER
DIAGNOSTIC_IGNORE(4146)  // unary minus operator applied to unsigned type, result still unsigned
DIAGNOSTIC_IGNORE(
    4805)  // '==': unsafe mix of type 'const bool' and type 'const int64_t' in operation
#endif

#include "gtest/gtest.h"

DIAGNOSTIC_POP

#endif
