#include "snii/format/stats_block.h"

namespace snii::format {

namespace {

// Field order within payload is fixed; reuse ByteSink varint primitives — do not hand-assemble bytes.
void encode_payload(const StatsBlock& sb, ByteSink* payload) {
    payload->put_varint64(sb.doc_count);
    payload->put_varint64(sb.indexed_doc_count);
    payload->put_varint64(sb.term_count);
    payload->put_varint64(sb.sum_total_term_freq);
    payload->put_varint64(sb.null_count);
}

Status decode_payload(Slice payload, StatsBlock* out) {
    ByteSource ps(payload);
    SNII_RETURN_IF_ERROR(ps.get_varint64(&out->doc_count));
    SNII_RETURN_IF_ERROR(ps.get_varint64(&out->indexed_doc_count));
    SNII_RETURN_IF_ERROR(ps.get_varint64(&out->term_count));
    SNII_RETURN_IF_ERROR(ps.get_varint64(&out->sum_total_term_freq));
    SNII_RETURN_IF_ERROR(ps.get_varint64(&out->null_count));
    if (!ps.eof()) {
        return Status::Corruption("stats_block: trailing bytes in payload");
    }
    return Status::OK();
}

} // namespace

void encode_stats_block(const StatsBlock& sb, ByteSink* sink) {
    ByteSink payload;
    encode_payload(sb, &payload);
    SectionFramer::write(*sink, static_cast<uint8_t>(SectionType::kStatsBlock), payload.view());
}

Status decode_stats_block(ByteSource* src, StatsBlock* out) {
    FramedSection sec;
    SNII_RETURN_IF_ERROR(SectionFramer::read(*src, &sec));
    if (sec.type != static_cast<uint8_t>(SectionType::kStatsBlock)) {
        return Status::InvalidArgument("stats_block: unexpected section type");
    }
    return decode_payload(sec.payload, out);
}

} // namespace snii::format
