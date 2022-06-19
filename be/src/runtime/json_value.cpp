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

#include "runtime/json_value.h"

#include <cstring>

#include "util/jsonb_error.h"

namespace doris {

JsonbErrType JsonValue::from_json_str(std::string s) {
    JsonbErrType error = JsonbErrType::E_NONE;
    if (!parser.parse(s)) {
        error = parser.getErrorCode();
        // TODO(wzy): document must be an object or an array,
        // rune, pure-string, numeirc are valid JSON but get parse error here
        // should return error gracefully to client
        LOG(FATAL) << "invalid json value: " << JsonbErrMsg::getErrMsg(error);
    }
    ptr = parser.getWriter().getOutput()->getBuffer();
    len = (unsigned)parser.getWriter().getOutput()->getSize();
    DCHECK_LE(len, MAX_LENGTH);
    return error;
}

std::string JsonValue::to_string() const {
    JsonbToJson toStr;
    return toStr.json(JsonbDocument::createDocument(ptr, len)->getValue());
}

std::ostream& operator<<(std::ostream& os, const JsonValue& json_value) {
    return os << json_value.to_string();
}

std::size_t operator-(const JsonValue& v1, const JsonValue& v2) {
    return 0;
}

} // namespace doris
