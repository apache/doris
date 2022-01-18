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

#include "runtime/datetime_value.h"
#include "runtime/string_value.h"
#include "vec/runtime/vdatetime_value.h"
namespace doris {
// This class is unused.  It contains static (compile time) asserts.
// This is useful to validate struct sizes and other similar things
// at compile time.  If these asserts fail, the compile will fail.
class UnusedClass {
private:
    static_assert(sizeof(StringValue) == 16);
    static_assert(offsetof(StringValue, len) == 8);
    // Datetime value
    static_assert(sizeof(DateTimeValue) == 16);
    static_assert(sizeof(doris::vectorized::VecDateTimeValue) == 8);
    // static_assert(offsetof(DateTimeValue, _year) == 8);
};

} // namespace doris
