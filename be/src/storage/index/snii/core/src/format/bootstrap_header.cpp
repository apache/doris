#include "snii/format/bootstrap_header.h"

#include "snii/encoding/byte_source.h"
#include "snii/encoding/crc32c.h"

namespace snii::format {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

namespace {

// Number of bytes covered by header_checksum: everything except the trailing
// crc32c.
constexpr size_t kChecksumCoverage = kBootstrapHeaderSize - 4;

// Writes all fixed fields except the trailing checksum. Field order is the
// on-disk contract; reuse ByteSink fixed-width primitives, never hand-assemble
// bytes.
void encode_fields(const BootstrapHeader& header, ByteSink* sink) {
    sink->put_fixed32(header.magic);
    sink->put_fixed32((static_cast<uint32_t>(header.min_reader_version) << 16) |
                      header.format_version);
    sink->put_fixed32(header.flags);
    sink->put_fixed32(kBootstrapHeaderSize); // header_length is always derived
    sink->put_u8(header.tail_pointer_size);
}

} // namespace

doris::Status encode_bootstrap_header(const BootstrapHeader& header, ByteSink* sink) {
    if (sink == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("bootstrap_header: null sink");
    }
    ByteSink fields;
    encode_fields(header, &fields);
    const uint32_t checksum = crc32c(fields.view());
    sink->put_bytes(fields.view());
    sink->put_fixed32(checksum);
    return doris::Status::OK();
}

doris::Status decode_bootstrap_header(Slice data, BootstrapHeader* out) {
    if (out == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("bootstrap_header: null out");
    }
    // Reject any size other than the exact fixed header: short input is
    // truncation, longer input means stray trailing bytes the parser would
    // otherwise ignore.
    if (data.size() != kBootstrapHeaderSize) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("bootstrap_header: wrong header size");
    }

    ByteSource src(data);
    uint32_t magic = 0;
    uint32_t version_pair = 0;
    uint32_t flags = 0;
    uint32_t header_length = 0;
    uint8_t tail_pointer_size = 0;
    uint32_t stored_checksum = 0;
    RETURN_IF_ERROR(src.get_fixed32(&magic));
    RETURN_IF_ERROR(src.get_fixed32(&version_pair));
    RETURN_IF_ERROR(src.get_fixed32(&flags));
    RETURN_IF_ERROR(src.get_fixed32(&header_length));
    RETURN_IF_ERROR(src.get_u8(&tail_pointer_size));
    RETURN_IF_ERROR(src.get_fixed32(&stored_checksum));

    if (magic != kContainerMagic) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("bootstrap_header: bad container magic");
    }
    const uint32_t computed = crc32c(data.subslice(0, kChecksumCoverage));
    if (computed != stored_checksum) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("bootstrap_header: checksum mismatch");
    }

    const auto min_reader_version = static_cast<uint16_t>((version_pair >> 16) & 0xFFFFu);
    const auto format_version = static_cast<uint16_t>(version_pair & 0xFFFFu);
    if (format_version != kFormatVersion) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>("bootstrap_header: unsupported container format_version");
    }
    if (min_reader_version > kFormatVersion) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>("bootstrap_header: container requires a newer reader version");
    }

    out->magic = magic;
    out->format_version = format_version;
    out->min_reader_version = min_reader_version;
    out->flags = flags;
    out->header_length = header_length;
    out->tail_pointer_size = tail_pointer_size;
    return doris::Status::OK();
}

} // namespace snii::format
