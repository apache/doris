#include "snii/format/stats_block.h"

namespace snii::format {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

namespace {

// Field order within payload is fixed; reuse ByteSink varint primitives — do not hand-assemble bytes.
void encode_payload(const StatsBlock& sb, ByteSink* payload) {
    payload->put_varint64(sb.doc_count);
    payload->put_varint64(sb.indexed_doc_count);
    payload->put_varint64(sb.term_count);
    payload->put_varint64(sb.sum_total_term_freq);
    payload->put_varint64(sb.null_count);
}

doris::Status decode_payload(Slice payload, StatsBlock* out) {
    ByteSource ps(payload);
    RETURN_IF_ERROR(ps.get_varint64(&out->doc_count));
    RETURN_IF_ERROR(ps.get_varint64(&out->indexed_doc_count));
    RETURN_IF_ERROR(ps.get_varint64(&out->term_count));
    RETURN_IF_ERROR(ps.get_varint64(&out->sum_total_term_freq));
    RETURN_IF_ERROR(ps.get_varint64(&out->null_count));
    if (!ps.eof()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("stats_block: trailing bytes in payload");
    }
    return doris::Status::OK();
}

} // namespace

void encode_stats_block(const StatsBlock& sb, ByteSink* sink) {
    ByteSink payload;
    encode_payload(sb, &payload);
    SectionFramer::write(*sink, static_cast<uint8_t>(SectionType::kStatsBlock), payload.view());
}

doris::Status decode_stats_block(ByteSource* src, StatsBlock* out) {
    FramedSection sec;
    RETURN_IF_ERROR(SectionFramer::read(*src, &sec));
    if (sec.type != static_cast<uint8_t>(SectionType::kStatsBlock)) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("stats_block: unexpected section type");
    }
    return decode_payload(sec.payload, out);
}

} // namespace snii::format
