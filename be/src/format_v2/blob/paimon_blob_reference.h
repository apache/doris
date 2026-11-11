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

#include <cstdint>
#include <string_view>

#include "common/status.h"

namespace doris::format::paimon {

// A decoded payload location, obtained from either descriptor bytes or a .blob file index.
struct PaimonBlobReference {
    // Borrows source descriptor bytes or split-owned metadata through append().
    // The output column and payload-reader cache copy any URI they retain.
    std::string_view uri;
    int64_t offset = 0;
    int64_t length = 0;

    // Decode the SDK's version 1/2 descriptor. Ranges address payload bytes,
    // not record ordinals, framing, or the size of the containing pack.
    // The version-specific byte layouts are documented beside the implementation.
    static Status decode(std::string_view descriptor_bytes, PaimonBlobReference* decoded_reference);
};

} // namespace doris::format::paimon
