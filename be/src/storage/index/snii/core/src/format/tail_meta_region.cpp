#include "snii/format/tail_meta_region.h"

#include "snii/encoding/byte_source.h"
#include "snii/encoding/crc32c.h"
#include "snii/format/format_constants.h"

namespace snii::format {
namespace {

// Header field bytes (before header_crc): u32 ver + u32 flags + u64 meta_region_len
// + u32 n + u64 directory_offset + u64 directory_length.
constexpr size_t kHeaderFields = 4 + 4 + 8 + 4 + 8 + 8; // 36
constexpr size_t kHeaderSize = kHeaderFields + 4;       // + header_crc32c
constexpr size_t kRegionChecksumSize = 4;

} // namespace

size_t tail_meta_header_size() {
    return kHeaderSize;
}

void TailMetaRegionBuilder::add_index(uint64_t index_id, std::string index_suffix,
                                      Slice per_index_meta_bytes) {
    Entry e;
    e.index_id = index_id;
    e.suffix = std::move(index_suffix);
    e.bytes.assign(per_index_meta_bytes.data(),
                   per_index_meta_bytes.data() + per_index_meta_bytes.size());
    entries_.push_back(std::move(e));
}

void TailMetaRegionBuilder::finish(ByteSink* sink) const {
    // Lay out per-index meta blocks right after the header; build the directory
    // with each block's in-region offset/length.
    LogicalIndexDirectoryBuilder dir;
    uint64_t offset = kHeaderSize;
    for (const Entry& e : entries_) {
        LogicalIndexRef ref;
        ref.index_id = e.index_id;
        ref.index_suffix = e.suffix;
        ref.meta_off = offset;
        ref.meta_len = e.bytes.size();
        dir.add(ref);
        offset += e.bytes.size();
    }
    const uint64_t directory_offset = offset;
    ByteSink dir_bytes;
    dir.finish(&dir_bytes);
    const uint64_t directory_length = dir_bytes.size();
    const uint64_t meta_region_len = directory_offset + directory_length + kRegionChecksumSize;

    ByteSink fields;
    fields.put_fixed32(kMetaFormatVersion);
    fields.put_fixed32(0); // flags
    fields.put_fixed64(meta_region_len);
    fields.put_fixed32(static_cast<uint32_t>(entries_.size()));
    fields.put_fixed64(directory_offset);
    fields.put_fixed64(directory_length);

    ByteSink region;
    region.put_bytes(fields.view());
    region.put_fixed32(crc32c(fields.view())); // header_crc32c
    for (const Entry& e : entries_) region.put_bytes(Slice(e.bytes));
    region.put_bytes(dir_bytes.view());
    region.put_fixed32(crc32c(region.view())); // meta_region_checksum

    sink->put_bytes(region.view());
}

Status TailMetaRegionReader::parse_header(Slice header, TailMetaRegionHeader* const out) {
    if (out == nullptr) {
        return Status::InvalidArgument("tail_meta_region: null header out");
    }
    if (header.size() != kHeaderSize) {
        return Status::Corruption("tail_meta_region: header size mismatch");
    }
    ByteSource hs(header.subslice(0, kHeaderFields));
    uint32_t ver = 0, flags = 0, n = 0;
    uint64_t meta_region_len = 0, directory_offset = 0, directory_length = 0;
    SNII_RETURN_IF_ERROR(hs.get_fixed32(&ver));
    SNII_RETURN_IF_ERROR(hs.get_fixed32(&flags));
    SNII_RETURN_IF_ERROR(hs.get_fixed64(&meta_region_len));
    SNII_RETURN_IF_ERROR(hs.get_fixed32(&n));
    SNII_RETURN_IF_ERROR(hs.get_fixed64(&directory_offset));
    SNII_RETURN_IF_ERROR(hs.get_fixed64(&directory_length));
    ByteSource hc(header.subslice(kHeaderFields, 4));
    uint32_t header_crc = 0;
    SNII_RETURN_IF_ERROR(hc.get_fixed32(&header_crc));
    if (crc32c(header.subslice(0, kHeaderFields)) != header_crc) {
        return Status::Corruption("tail_meta_region: header crc mismatch");
    }
    if (ver != kMetaFormatVersion) {
        return Status::Unsupported("tail_meta_region: unsupported meta_format_version");
    }
    if (flags != 0) {
        return Status::Unsupported("tail_meta_region: unsupported flags");
    }
    if (meta_region_len < kHeaderSize + kRegionChecksumSize) {
        return Status::Corruption("tail_meta_region: declared length too small");
    }
    if (directory_offset < kHeaderSize || directory_offset > meta_region_len ||
        directory_length > meta_region_len - directory_offset) {
        return Status::Corruption("tail_meta_region: directory out of range");
    }
    out->meta_region_len = meta_region_len;
    out->directory_offset = directory_offset;
    out->directory_length = directory_length;
    out->n_logical_indexes = n;
    return Status::OK();
}

Status TailMetaRegionReader::open_directory(const TailMetaRegionHeader& header, Slice directory,
                                            TailMetaRegionReader* const out) {
    if (out == nullptr) {
        return Status::InvalidArgument("tail_meta_region: null out");
    }
    if (directory.size() != header.directory_length) {
        return Status::Corruption("tail_meta_region: directory length mismatch");
    }

    SNII_RETURN_IF_ERROR(LogicalIndexDirectoryReader::open(directory, &out->dir_));
    if (out->dir_.size() != header.n_logical_indexes) {
        return Status::Corruption("tail_meta_region: directory size mismatch");
    }
    out->region_ = Slice {};
    out->meta_region_len_ = header.meta_region_len;
    out->n_ = header.n_logical_indexes;
    return Status::OK();
}

Status TailMetaRegionReader::open(Slice region, TailMetaRegionReader* const out) {
    if (out == nullptr) {
        return Status::InvalidArgument("tail_meta_region: null out");
    }
    if (region.size() < kHeaderSize + kRegionChecksumSize) {
        return Status::Corruption("tail_meta_region: region too short");
    }

    // Verify the trailing region checksum.
    const size_t covered = region.size() - kRegionChecksumSize;
    ByteSource cs(region.subslice(covered, kRegionChecksumSize));
    uint32_t region_crc = 0;
    SNII_RETURN_IF_ERROR(cs.get_fixed32(&region_crc));
    if (crc32c(region.subslice(0, covered)) != region_crc) {
        return Status::Corruption("tail_meta_region: meta_region_checksum mismatch");
    }

    TailMetaRegionHeader header;
    SNII_RETURN_IF_ERROR(parse_header(region.subslice(0, kHeaderSize), &header));
    if (header.meta_region_len != region.size()) {
        return Status::Corruption("tail_meta_region: declared length mismatch");
    }
    SNII_RETURN_IF_ERROR(open_directory(
            header, region.subslice(header.directory_offset, header.directory_length), out));
    out->region_ = region;
    return Status::OK();
}

Status TailMetaRegionReader::find_ref(uint64_t index_id, std::string_view suffix, bool* const found,
                                      LogicalIndexRef* const ref) const {
    return dir_.find(index_id, suffix, found, ref);
}

Status TailMetaRegionReader::find(uint64_t index_id, std::string_view suffix, bool* const found,
                                  Slice* const per_index_meta_bytes) const {
    LogicalIndexRef ref;
    SNII_RETURN_IF_ERROR(find_ref(index_id, suffix, found, &ref));
    if (!*found) {
        return Status::OK();
    }
    if (region_.empty()) {
        return Status::InvalidArgument("tail_meta_region: region bytes not resident");
    }
    if (ref.meta_off > region_.size() || ref.meta_len > region_.size() - ref.meta_off ||
        ref.meta_off > meta_region_len_ || ref.meta_len > meta_region_len_ - ref.meta_off) {
        return Status::Corruption("tail_meta_region: meta block out of range");
    }
    *per_index_meta_bytes = region_.subslice(ref.meta_off, ref.meta_len);
    return Status::OK();
}

} // namespace snii::format
