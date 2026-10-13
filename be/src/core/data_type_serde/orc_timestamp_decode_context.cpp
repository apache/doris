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

#include "core/data_type_serde/orc_timestamp_decode_context.h"

#include <orc/Type.hh>

#include "common/check.h"

namespace doris {

const cctz::time_zone& OrcTimestampDecodeContext::timezone_for(const orc::Type& file_type) const {
    const auto kind = file_type.getKind();
    DORIS_CHECK(kind == orc::TIMESTAMP || kind == orc::TIMESTAMP_INSTANT);
    // Plain timestamps reconstruct the SDK reader's civil value. Instant-to-DATETIME conversion
    // uses the session zone, which must not follow a plain sibling's stripe writer timezone.
    return kind == orc::TIMESTAMP ? reader_timezone : session_timezone;
}

} // namespace doris
