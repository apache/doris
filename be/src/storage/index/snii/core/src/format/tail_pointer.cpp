#include "snii/format/tail_pointer.h"

#include "snii/encoding/byte_source.h"
#include "snii/encoding/crc32c.h"
#include "snii/format/format_constants.h"

namespace snii::format {

namespace {

// Byte widths of every fixed field, used to derive the constant on-disk size:
// u32 magic + u16 version + 3*u64 + 2*u32 + u8 size + u32 tail_checksum.
constexpr size_t kMagicBytes = 4;
constexpr size_t kVersionBytes = 2;
constexpr size_t kU64Bytes = 8;
constexpr size_t kU32Bytes = 4;
constexpr size_t kSizeByteBytes = 1;

constexpr size_t kFixedSize =
        kMagicBytes + kVersionBytes + 3 * kU64Bytes + 2 * kU32Bytes + kSizeByteBytes + kU32Bytes;
// tail_checksum is the trailing u32 and covers every byte before it.
constexpr size_t kChecksumCoverage = kFixedSize - kU32Bytes;

// Serializes the checksum-covered region in fixed field order into covered.
void serialize_covered(const TailPointer& tp, ByteSink* covered) {
    covered->put_fixed32(kTailMagic);
    covered->put_fixed16(kFormatVersion);
    covered->put_fixed64(tp.meta_region_offset);
    covered->put_fixed64(tp.meta_region_length);
    covered->put_fixed64(tp.hot_off);
    covered->put_fixed32(tp.meta_region_checksum);
    covered->put_fixed32(tp.bootstrap_header_checksum);
    covered->put_u8(static_cast<uint8_t>(kFixedSize));
}

} // namespace

size_t tail_pointer_size() {
    return kFixedSize;
}

Status encode_tail_pointer(const TailPointer& tp, ByteSink* sink) {
    ByteSink covered;
    serialize_covered(tp, &covered);
    if (covered.size() != kChecksumCoverage) {
        return Status::Internal("tail_pointer: covered size mismatch");
    }
    const uint32_t tail_checksum = crc32c(covered.view());
    sink->put_bytes(covered.view());
    sink->put_fixed32(tail_checksum);
    return Status::OK();
}

Status decode_tail_pointer(Slice last_bytes, TailPointer* out) {
    // Anti-DoS / framing: the tail pointer is a fixed-size footer, so reject any
    // input that is not exactly the fixed size before touching its contents.
    if (last_bytes.size() != kFixedSize) {
        return Status::Corruption("tail_pointer: input is not the fixed size");
    }
    // Verify the trailing tail_checksum over the covered region first; a mismatch
    // means any parsed field would be untrustworthy.
    const Slice covered = last_bytes.subslice(0, kChecksumCoverage);
    ByteSource src(last_bytes);

    uint32_t magic = 0;
    SNII_RETURN_IF_ERROR(src.get_fixed32(&magic));
    if (magic != kTailMagic) {
        return Status::Corruption("tail_pointer: bad magic");
    }

    uint16_t format_version = 0;
    SNII_RETURN_IF_ERROR(src.get_fixed16(&format_version));
    (void)format_version; // Read to advance the cursor; version policy lives in
                          // the bootstrap header, not here.
    SNII_RETURN_IF_ERROR(src.get_fixed64(&out->meta_region_offset));
    SNII_RETURN_IF_ERROR(src.get_fixed64(&out->meta_region_length));
    SNII_RETURN_IF_ERROR(src.get_fixed64(&out->hot_off));
    SNII_RETURN_IF_ERROR(src.get_fixed32(&out->meta_region_checksum));
    SNII_RETURN_IF_ERROR(src.get_fixed32(&out->bootstrap_header_checksum));

    uint8_t on_disk_size = 0;
    SNII_RETURN_IF_ERROR(src.get_u8(&on_disk_size));
    if (on_disk_size != kFixedSize) {
        return Status::Corruption("tail_pointer: embedded size mismatch");
    }

    uint32_t tail_checksum = 0;
    SNII_RETURN_IF_ERROR(src.get_fixed32(&tail_checksum));
    if (tail_checksum != crc32c(covered)) {
        return Status::Corruption("tail_pointer: tail_checksum mismatch");
    }
    return Status::OK();
}

} // namespace snii::format
