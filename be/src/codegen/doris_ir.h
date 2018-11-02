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

#ifndef DORIS_BE_SRC_QUERY_CODGEN_DORIS_IR_H
#define DORIS_BE_SRC_QUERY_CODGEN_DORIS_IR_H
#ifdef IR_COMPILE
// For cross compiling to IR, we need functions decorated in specific ways.  For
// functions that we will replace with codegen, we need them not inlined (otherwise
// we can't find the function by name.  For functions where the non-codegen'd version
// is too long for the compiler to inline, we might still want to inline it since
// the codegen'd version is suitable for inling.
// In the non-ir case (g++), we will just default to whatever the compiler thought
// best at that optimization setting.
#define IR_NO_INLINE __attribute__((noinline))
#define IR_ALWAYS_INLINE __attribute__((always_inline))
#else
#define IR_NO_INLINE
#define IR_ALWAYS_INLINE
#endif
#endif
