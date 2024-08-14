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

#include "runtime/query_type.h"

namespace doris {
const std::string toString(QUERY_SOURCE queryType) {
    switch (queryType) {
    case QUERY_SOURCE::INTERNAL_FRONTEND:
        return "INTERNAL_FRONTEND";
    case QUERY_SOURCE::STREAM_LOAD:
        return "STREAM_LOAD";
    case QUERY_SOURCE::GROUP_COMMIT_LOAD:
        return "EXTERNAL_QUERY";
    case QUERY_SOURCE::ROUTINE_LOAD:
        return "ROUTINE_LOAD";
    default:
        return "UNKNOWN";
    }
}
} // namespace doris
