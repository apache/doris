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

#include "common/status.h"
#include "snii/common/slice.h"
#include "snii/encoding/byte_sink.h"
#include "snii/encoding/byte_source.h"

namespace snii {

// A framed section: type + payload view.
struct FramedSection {
    uint8_t type = 0;
    Slice payload;
};

// Unified section framing: [u8 type][varint64 len][payload][fixed32 crc32c(type+len+payload)].
// All full-format sections reuse this encode/checksum path to avoid ad-hoc hand-assembly.
// Unknown optional sections are dispatched by the caller based on type; read still verifies the CRC and skips the payload.
class SectionFramer {
public:
    static void write(ByteSink& sink, uint8_t section_type, Slice payload);
    static doris::Status read(ByteSource& src, FramedSection* out);
};

} // namespace snii
