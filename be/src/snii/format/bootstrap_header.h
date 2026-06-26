#pragma once

#include <cstdint>

#include "snii/common/slice.h"
#include "snii/common/status.h"
#include "snii/encoding/byte_sink.h"
#include "snii/format/format_constants.h"

namespace snii::format {

// Fixed container header at the very start of a {rowset_id}_{seg_id}.idx file.
// Identifies the SNII container and carries basic compatibility info so a
// reader can fail fast before touching any streamed section or the tail meta
// region.
//
// On-disk layout (all multi-byte fields little-endian, fixed width; NOT framed
// by SectionFramer because it must be parseable without prior knowledge of the
// file):
//   u32 magic              == kContainerMagic
//   u16 format_version      == kFormatVersion
//   u16 min_reader_version  readers with kFormatVersion < this MUST refuse to
//   read u32 flags               container-level feature flags u32
//   header_length       total bytes of this header including the checksum u8
//   tail_pointer_size   size of the fixed tail pointer at EOF (hint for the
//   reader) u32 header_checksum     crc32c over all preceding header bytes
struct BootstrapHeader {
    uint32_t magic = kContainerMagic;
    uint16_t format_version = kFormatVersion;
    uint16_t min_reader_version = kMinReaderVersion;
    uint32_t flags = 0;
    uint32_t header_length = 0;
    uint8_t tail_pointer_size = 0;
};

// Total fixed on-disk size of the header, including the trailing crc32c.
inline constexpr uint32_t kBootstrapHeaderSize =
        4 /*magic*/ + 2 /*format_version*/ + 2 /*min_reader_version*/ + 4 /*flags*/ +
        4 /*header_length*/ + 1 /*tail_pointer_size*/ + 4 /*header_checksum*/;

// Serializes the header to sink: writes header_length = kBootstrapHeaderSize
// and appends a crc32c over all preceding bytes. The caller's header_length
// field is ignored on input (it is always derived). Returns OK.
Status encode_bootstrap_header(const BootstrapHeader& header, ByteSink* sink);

// Parses and validates a bootstrap header from the front of data.
//   - too short / trailing bytes beyond the fixed header -> kCorruption
//   - magic != kContainerMagic                           -> kCorruption
//   - checksum mismatch                                  -> kCorruption
//   - format_version != kFormatVersion                   -> kUnsupported
//   - min_reader_version > kFormatVersion                -> kUnsupported
Status decode_bootstrap_header(Slice data, BootstrapHeader* out);

} // namespace snii::format
