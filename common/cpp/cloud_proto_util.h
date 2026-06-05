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

#pragma once

#include <gen_cpp/cloud.pb.h>

namespace doris::cloud {

// Reads the exact status code when this binary knows the enum value. If aux_code contains a
// future enum value unknown to this binary, fall back to the legacy-compatible code field.
inline MetaServiceCode get_response_code(const MetaServiceResponseStatus& status) {
    if (status.has_aux_code() && MetaServiceCode_IsValid(status.aux_code())) {
        return static_cast<MetaServiceCode>(status.aux_code());
    }
    return status.code();
}

} // namespace doris::cloud
