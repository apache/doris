#include "snii/format/dict_block_directory.h"

#include "snii/encoding/byte_source.h"
#include "snii/encoding/section_framer.h"
#include "snii/format/format_constants.h"

namespace snii::format {

namespace {

// Each block_ref has a fixed field order; reuse ByteSink varint/fixed primitives — do not hand-craft bytes manually.
// uncomp_len trails only when the kZstd flag is set, so uncompressed-block
// directories keep their compact (v1-identical) per-ref byte layout.
void encode_ref(const BlockRef& ref, ByteSink* payload) {
    payload->put_varint64(ref.offset);
    payload->put_varint64(ref.length);
    payload->put_varint32(ref.n_entries);
    payload->put_u8(ref.flags);
    payload->put_fixed32(ref.checksum);
    if (ref.flags & block_ref_flags::kZstd) payload->put_varint64(ref.uncomp_len);
}

Status decode_ref(ByteSource* ps, BlockRef* ref) {
    SNII_RETURN_IF_ERROR(ps->get_varint64(&ref->offset));
    SNII_RETURN_IF_ERROR(ps->get_varint64(&ref->length));
    SNII_RETURN_IF_ERROR(ps->get_varint32(&ref->n_entries));
    SNII_RETURN_IF_ERROR(ps->get_u8(&ref->flags));
    SNII_RETURN_IF_ERROR(ps->get_fixed32(&ref->checksum));
    if (ref->flags & block_ref_flags::kZstd) {
        SNII_RETURN_IF_ERROR(ps->get_varint64(&ref->uncomp_len));
    }
    return Status::OK();
}

Status decode_payload(Slice payload, std::vector<BlockRef>* refs) {
    ByteSource ps(payload);
    uint32_t n_blocks = 0;
    SNII_RETURN_IF_ERROR(ps.get_varint32(&n_blocks));
    // Guard against a corrupted, inflated count from untrusted bytes: each BlockRef
    // needs >= 8 bytes (flags u8 + checksum u32 + >= 1 byte for each of 3 varints),
    // so cap before reserve to avoid a huge allocation.
    constexpr size_t kMinRefBytes = 8;
    if (n_blocks > ps.remaining() / kMinRefBytes) {
        return Status::Corruption("dict_block_directory: n_blocks exceeds payload capacity");
    }
    refs->clear();
    refs->reserve(n_blocks);
    for (uint32_t i = 0; i < n_blocks; ++i) {
        BlockRef ref {};
        SNII_RETURN_IF_ERROR(decode_ref(&ps, &ref));
        refs->push_back(ref);
    }
    if (!ps.eof()) {
        return Status::Corruption("dict_block_directory: trailing bytes in payload");
    }
    return Status::OK();
}

} // namespace

void DictBlockDirectoryBuilder::finish(ByteSink* sink) const {
    ByteSink payload;
    payload.put_varint32(static_cast<uint32_t>(refs_.size()));
    for (const auto& ref : refs_) {
        encode_ref(ref, &payload);
    }
    SectionFramer::write(*sink, static_cast<uint8_t>(SectionType::kDictBlockDirectory),
                         payload.view());
}

Status DictBlockDirectoryReader::open(Slice section, DictBlockDirectoryReader* out) {
    ByteSource src(section);
    FramedSection sec;
    SNII_RETURN_IF_ERROR(SectionFramer::read(src, &sec));
    if (sec.type != static_cast<uint8_t>(SectionType::kDictBlockDirectory)) {
        return Status::InvalidArgument("dict_block_directory: unexpected section type");
    }
    return decode_payload(sec.payload, &out->refs_);
}

Status DictBlockDirectoryReader::get(uint32_t ordinal, BlockRef* out) const {
    if (ordinal >= refs_.size()) {
        return Status::NotFound("dict_block_directory: ordinal out of range");
    }
    *out = refs_[ordinal];
    return Status::OK();
}

} // namespace snii::format
