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

#ifndef GMOCK_WRAPPER_H
#define GMOCK_WRAPPER_H

// we need to disable a whole set of warnings as we include gtest.h
// restore most of the warnings after the file is loaded.

#include "Adaptor.hh"

DIAGNOSTIC_PUSH

#if defined(__GNUC__) || defined(__clang__)
DIAGNOSTIC_IGNORE("-Wdeprecated")
DIAGNOSTIC_IGNORE("-Wmissing-noreturn")
DIAGNOSTIC_IGNORE("-Wpadded")
DIAGNOSTIC_IGNORE("-Wsign-compare")
DIAGNOSTIC_IGNORE("-Wsign-conversion")
DIAGNOSTIC_IGNORE("-Wundef")
#endif

#ifdef __clang__
DIAGNOSTIC_IGNORE("-Wnull-dereference")
DIAGNOSTIC_IGNORE("-Wshift-sign-overflow")
DIAGNOSTIC_IGNORE("-Wused-but-marked-unused")
DIAGNOSTIC_IGNORE("-Wweak-vtables")
#endif

#include "gmock/gmock.h"

DIAGNOSTIC_POP

#endif
