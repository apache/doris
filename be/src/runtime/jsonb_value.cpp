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

#include "runtime/jsonb_value.h"

#include <fmt/format.h>

#include <string_view>

#include "util/jsonb_error.h"
#include "util/jsonb_stream.h"
#include "util/jsonb_utils.h"
#include "util/jsonb_writer.h"

namespace doris {

Status JsonBinaryValue::from_json_string(const char* s, int length) {
    JsonbErrType error = JsonbErrType::E_NONE;
    if (!parser.parse(s, length)) {
        error = parser.getErrorCode();
        auto msg = fmt::format("json parse error: {} for value: {}", JsonbErrMsg::getErrMsg(error),
                               std::string_view(s, length));
        VLOG_DEBUG << msg;
        return Status::InvalidArgument(msg);
    }

    ptr = parser.getWriter().getOutput()->getBuffer();
    len = (unsigned)parser.getWriter().getOutput()->getSize();
    DCHECK_LE(len, MAX_LENGTH);
    return Status::OK();
}

std::string JsonBinaryValue::to_json_string() const {
    return JsonbToJson::jsonb_to_json_string(ptr, len);
}

std::ostream& operator<<(std::ostream& os, const JsonBinaryValue& json_value) {
    return os << json_value.to_json_string();
}
} // namespace doris
