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

#include "vec/common/string_utils/string_utils.h"

namespace detail {

bool starts_with(const std::string& s, const char* prefix, size_t prefix_size) {
    return s.size() >= prefix_size && 0 == memcmp(s.data(), prefix, prefix_size);
}

bool ends_with(const std::string& s, const char* suffix, size_t suffix_size) {
    return s.size() >= suffix_size &&
           0 == memcmp(s.data() + s.size() - suffix_size, suffix, suffix_size);
}

} // namespace detail
