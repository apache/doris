#include "snii/format/norms_pod.h"

#include <limits>

#include "snii/common/slice.h"
#include "snii/encoding/byte_source.h"
#include "snii/encoding/section_framer.h"
#include "snii/format/format_constants.h"

namespace snii::format {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

void NormsPodWriter::finish(ByteSink* sink) const {
    // Build inner payload: [varint64 doc_count][raw norm bytes].
    ByteSink payload;
    payload.put_varint64(norms_.size());
    payload.put_bytes(Slice(norms_));
    // Delegate outer framing to SectionFramer to append type+len+crc32c, avoiding manual checksum assembly.
    SectionFramer::write(*sink, static_cast<uint8_t>(SectionType::kStatsBlock), payload.view());
}

doris::Status NormsPodReader::open(Slice framed, NormsPodReader* out) {
    // framer handles CRC verify, truncation detection, and payload slicing.
    ByteSource src(framed);
    FramedSection sec;
    RETURN_IF_ERROR(SectionFramer::read(src, &sec));

    // Parse inner payload: [varint64 doc_count][bytes].
    ByteSource payload(sec.payload);
    uint64_t doc_count = 0;
    RETURN_IF_ERROR(payload.get_varint64(&doc_count));
    if (doc_count > std::numeric_limits<uint32_t>::max()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("norms POD doc_count overflows uint32");
    }
    // doc_count must exactly equal the remaining byte count (1 byte per doc).
    if (payload.remaining() != doc_count) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("norms POD length mismatch");
    }

    Slice bytes;
    RETURN_IF_ERROR(payload.get_bytes(static_cast<size_t>(doc_count), &bytes));
    out->doc_count_ = static_cast<uint32_t>(doc_count);
    out->norms_ = bytes.data();
    return doris::Status::OK();
}

} // namespace snii::format
