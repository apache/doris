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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/static-asserts.cpp
// and modified by Doris

#include <stddef.h>

#include "vec/common/string_ref.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
// This class is unused.  It contains static (compile time) asserts.
// This is useful to validate struct sizes and other similar things
// at compile time.  If these assertions fail, the compiling will fail.
class UnusedClass {
private:
    static_assert(sizeof(StringRef) == 16);
    static_assert(offsetof(StringRef, size) == 8);
    static_assert(sizeof(doris::VecDateTimeValue) == 8);
};

} // namespace doris
