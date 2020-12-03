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

#ifdef IR_COMPILE
#include "codegen/doris_ir.h"
#include "runtime/string_value.hpp"

namespace doris {
int ir_string_compare(const char* s1, int n1, const char* s2, int n2) {
    return string_compare(s1, n1, s2, n2, std::min(n1, n2));
}
} // namespace doris
#else
#error "This file should only be used for cross compiling to IR."
#endif
