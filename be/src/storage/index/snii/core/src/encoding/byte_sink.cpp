#include "snii/encoding/byte_sink.h"

#include "snii/encoding/varint.h"

namespace snii {

void ByteSink::put_fixed16(uint16_t v) {
    for (int i = 0; i < 2; ++i) buf_.push_back(static_cast<uint8_t>(v >> (8 * i)));
}

void ByteSink::put_fixed32(uint32_t v) {
    for (int i = 0; i < 4; ++i) buf_.push_back(static_cast<uint8_t>(v >> (8 * i)));
}

void ByteSink::put_fixed64(uint64_t v) {
    for (int i = 0; i < 8; ++i) buf_.push_back(static_cast<uint8_t>(v >> (8 * i)));
}

void ByteSink::put_varint32(uint32_t v) {
    uint8_t tmp[5];
    size_t n = encode_varint32(v, tmp);
    buf_.insert(buf_.end(), tmp, tmp + n);
}

void ByteSink::put_varint64(uint64_t v) {
    uint8_t tmp[10];
    size_t n = encode_varint64(v, tmp);
    buf_.insert(buf_.end(), tmp, tmp + n);
}

void ByteSink::put_zigzag(int64_t v) {
    put_varint64(zigzag_encode(v));
}

void ByteSink::put_bytes(Slice s) {
    buf_.insert(buf_.end(), s.data(), s.data() + s.size());
}

} // namespace snii
