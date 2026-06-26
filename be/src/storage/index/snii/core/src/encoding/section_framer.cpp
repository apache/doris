#include "snii/encoding/section_framer.h"

#include "snii/encoding/crc32c.h"

namespace snii {

void SectionFramer::write(ByteSink& sink, uint8_t section_type, Slice payload) {
    // Assemble type+len+payload in a temporary sink, compute crc over the whole thing, then write it all out.
    ByteSink framed;
    framed.put_u8(section_type);
    framed.put_varint64(payload.size());
    framed.put_bytes(payload);
    uint32_t crc = crc32c(framed.view());
    sink.put_bytes(framed.view());
    sink.put_fixed32(crc);
}

Status SectionFramer::read(ByteSource& src, FramedSection* out) {
    size_t start = src.position();
    uint8_t type;
    SNII_RETURN_IF_ERROR(src.get_u8(&type));
    uint64_t len;
    SNII_RETURN_IF_ERROR(src.get_varint64(&len));
    Slice payload;
    SNII_RETURN_IF_ERROR(src.get_bytes(static_cast<size_t>(len), &payload));
    size_t framed_len = src.position() - start;
    uint32_t stored;
    SNII_RETURN_IF_ERROR(src.get_fixed32(&stored));
    if (crc32c(src.slice_from(start, framed_len)) != stored) {
        return Status::Corruption("section crc mismatch");
    }
    out->type = type;
    out->payload = payload;
    return Status::OK();
}

} // namespace snii
