#include "snii/format/logical_index_directory.h"

#include "snii/encoding/byte_source.h"
#include "snii/encoding/section_framer.h"
#include "snii/format/format_constants.h"

namespace snii::format {

namespace {

// Minimum payload bytes any entry can occupy: index_id (>=1) + suffix_len (>=1, value 0) +
// meta_off (>=1) + meta_len (>=1). Used as an anti-DoS lower bound before reserving.
constexpr size_t kMinEntryBytes = 4;

// Encode one directory entry. Fixed field order; reuse ByteSink varint/bytes primitives.
void encode_entry(const LogicalIndexRef& ref, ByteSink* payload) {
    payload->put_varint64(ref.index_id);
    payload->put_varint32(static_cast<uint32_t>(ref.index_suffix.size()));
    payload->put_bytes(Slice(std::string_view(ref.index_suffix)));
    payload->put_varint64(ref.meta_off);
    payload->put_varint64(ref.meta_len);
}

// Decode one directory entry, validating suffix_len against the remaining payload before copying.
Status decode_entry(ByteSource* ps, LogicalIndexRef* ref) {
    SNII_RETURN_IF_ERROR(ps->get_varint64(&ref->index_id));
    uint32_t suffix_len = 0;
    SNII_RETURN_IF_ERROR(ps->get_varint32(&suffix_len));
    // Anti-DoS: reject a suffix_len that cannot fit in the remaining bytes before allocating.
    if (suffix_len > ps->remaining()) {
        return Status::Corruption("logical_index_directory: suffix_len exceeds payload");
    }
    Slice suffix;
    SNII_RETURN_IF_ERROR(ps->get_bytes(suffix_len, &suffix));
    ref->index_suffix.assign(reinterpret_cast<const char*>(suffix.data()), suffix.size());
    SNII_RETURN_IF_ERROR(ps->get_varint64(&ref->meta_off));
    SNII_RETURN_IF_ERROR(ps->get_varint64(&ref->meta_len));
    return Status::OK();
}

Status decode_payload(Slice payload, std::vector<LogicalIndexRef>* refs) {
    ByteSource ps(payload);
    uint32_t n_entries = 0;
    SNII_RETURN_IF_ERROR(ps.get_varint32(&n_entries));
    // Anti-DoS: cap n_entries against the remaining payload before reserving, so a corrupted
    // inflated count cannot trigger a huge allocation.
    if (n_entries > ps.remaining() / kMinEntryBytes) {
        return Status::Corruption("logical_index_directory: n_entries exceeds payload capacity");
    }
    refs->clear();
    refs->reserve(n_entries);
    for (uint32_t i = 0; i < n_entries; ++i) {
        LogicalIndexRef ref {};
        SNII_RETURN_IF_ERROR(decode_entry(&ps, &ref));
        refs->push_back(std::move(ref));
    }
    if (!ps.eof()) {
        return Status::Corruption("logical_index_directory: trailing bytes in payload");
    }
    return Status::OK();
}

} // namespace

void LogicalIndexDirectoryBuilder::finish(ByteSink* sink) const {
    ByteSink payload;
    payload.put_varint32(static_cast<uint32_t>(refs_.size()));
    for (const auto& ref : refs_) {
        encode_entry(ref, &payload);
    }
    SectionFramer::write(*sink, static_cast<uint8_t>(SectionType::kLogicalIndexDirectory),
                         payload.view());
}

Status LogicalIndexDirectoryReader::open(Slice framed, LogicalIndexDirectoryReader* out) {
    if (out == nullptr) {
        return Status::InvalidArgument("logical_index_directory: out is null");
    }
    ByteSource src(framed);
    FramedSection sec;
    SNII_RETURN_IF_ERROR(SectionFramer::read(src, &sec));
    if (sec.type != static_cast<uint8_t>(SectionType::kLogicalIndexDirectory)) {
        return Status::InvalidArgument("logical_index_directory: unexpected section type");
    }
    return decode_payload(sec.payload, &out->refs_);
}

Status LogicalIndexDirectoryReader::get(uint32_t i, LogicalIndexRef* out) const {
    if (out == nullptr) {
        return Status::InvalidArgument("logical_index_directory: out is null");
    }
    if (i >= refs_.size()) {
        return Status::NotFound("logical_index_directory: index out of range");
    }
    *out = refs_[i];
    return Status::OK();
}

Status LogicalIndexDirectoryReader::find(uint64_t index_id, std::string_view suffix, bool* found,
                                         LogicalIndexRef* out) const {
    if (found == nullptr || out == nullptr) {
        return Status::InvalidArgument("logical_index_directory: output pointer is null");
    }
    *found = false;
    for (const auto& ref : refs_) {
        if (ref.index_id != index_id || std::string_view(ref.index_suffix) != suffix) {
            continue;
        }
        *out = ref;
        *found = true;
        return Status::OK();
    }
    return Status::OK();
}

} // namespace snii::format
