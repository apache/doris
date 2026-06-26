#include "snii/encoding/byte_source.h"

#include "snii/encoding/varint.h"

namespace snii {

Status ByteSource::get_u8(uint8_t* v) {
    if (remaining() < 1) return Status::Corruption("get_u8 overrun");
    *v = s_[pos_++];
    return Status::OK();
}

Status ByteSource::get_fixed16(uint16_t* v) {
    if (remaining() < 2) return Status::Corruption("get_fixed16 overrun");
    uint16_t r = 0;
    for (int i = 0; i < 2; ++i) r |= static_cast<uint16_t>(s_[pos_ + i]) << (8 * i);
    pos_ += 2;
    *v = r;
    return Status::OK();
}

Status ByteSource::get_fixed32(uint32_t* v) {
    if (remaining() < 4) return Status::Corruption("get_fixed32 overrun");
    uint32_t r = 0;
    for (int i = 0; i < 4; ++i) r |= static_cast<uint32_t>(s_[pos_ + i]) << (8 * i);
    pos_ += 4;
    *v = r;
    return Status::OK();
}

Status ByteSource::get_fixed64(uint64_t* v) {
    if (remaining() < 8) return Status::Corruption("get_fixed64 overrun");
    uint64_t r = 0;
    for (int i = 0; i < 8; ++i) r |= static_cast<uint64_t>(s_[pos_ + i]) << (8 * i);
    pos_ += 8;
    *v = r;
    return Status::OK();
}

Status ByteSource::get_varint64(uint64_t* v) {
    const uint8_t* p = s_.data() + pos_;
    const uint8_t* next = nullptr;
    SNII_RETURN_IF_ERROR(decode_varint64(p, s_.data() + s_.size(), v, &next));
    pos_ = static_cast<size_t>(next - s_.data());
    return Status::OK();
}

Status ByteSource::get_varint32(uint32_t* v) {
    uint64_t tmp;
    SNII_RETURN_IF_ERROR(get_varint64(&tmp));
    if (tmp > 0xFFFFFFFFu) return Status::Corruption("varint32 overflow");
    *v = static_cast<uint32_t>(tmp);
    return Status::OK();
}

Status ByteSource::get_zigzag(int64_t* v) {
    uint64_t tmp;
    SNII_RETURN_IF_ERROR(get_varint64(&tmp));
    *v = zigzag_decode(tmp);
    return Status::OK();
}

Status ByteSource::get_bytes(size_t n, Slice* out) {
    if (remaining() < n) return Status::Corruption("get_bytes overrun");
    *out = s_.subslice(pos_, n);
    pos_ += n;
    return Status::OK();
}

} // namespace snii
