#pragma once

#include <cstdint>

#include "snii/common/slice.h"
#include "snii/common/status.h"
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
    static Status read(ByteSource& src, FramedSection* out);
};

} // namespace snii
