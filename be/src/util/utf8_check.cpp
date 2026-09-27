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

#include "util/utf8_check.h"

#include <simdutf.h>

namespace doris {

bool validate_utf8(const char* src, size_t len) {
    return simdutf::validate_utf8(src, len);
}

bool validate_utf8(const TFileScanRangeParams& params, const char* src, size_t len) {
    if (params.__isset.file_attributes && !params.file_attributes.enable_text_validate_utf8) {
        return true;
    }
    return validate_utf8(src, len);
}

} // namespace doris
