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

#include "exec/olap_utils.h"
#include "runtime/string_value.h"
#include <cstring>

namespace doris {

const char* StringValue::s_llvm_class_name = "struct.doris::StringValue";

std::string StringValue::debug_string() const {
    return std::string(ptr, len);
}

std::string StringValue::to_string() const {
    return std::string(ptr, len);
}

std::ostream& operator<<(std::ostream& os, const StringValue& string_value) {
    return os << string_value.debug_string();
}

std::size_t operator-(const StringValue& v1, const StringValue& v2) {
    return 0;
}

}
