#include "snii/io/metered_file_reader.h"

#include <algorithm>

namespace snii::io {
using doris::Status; // RETURN_IF_ERROR expands to bare Status
namespace {

// Inclusive [first, last] block ids touched by a validated [offset, offset+len).
// Empty len touches no block (callers guard len==0 before calling this).
void block_range(uint64_t offset, size_t len, size_t block_size, uint64_t* first, uint64_t* last) {
    *first = offset / block_size;
    *last = (offset + len - 1) / block_size;
}

} // namespace

MeteredFileReader::MeteredFileReader(FileReader* inner, size_t block_size)
        : inner_(inner), block_size_(block_size) {}

void MeteredFileReader::reset_metrics() {
    resident_.clear();
    metrics_ = IoMetrics {};
}

doris::Status MeteredFileReader::validate_range(uint64_t offset, size_t len) const {
    if (inner_ == nullptr) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("metered: null inner reader");
    if (block_size_ == 0) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("metered: zero block size");
    const uint64_t total = inner_->size();
    if (offset > total || len > total - offset) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("metered: read range past end");
    }
    return doris::Status::OK();
}

// Accounts the FileCache effect of touching [offset, offset+len): newly missed
// blocks become coalesced remote GETs and remote bytes. Returns true iff any
// block missed. (Single contiguous span -> at most one coalesced run.)
bool MeteredFileReader::account_blocks(uint64_t offset, size_t len) {
    if (len == 0) return false;
    uint64_t first = 0, last = 0;
    block_range(offset, len, block_size_, &first, &last);

    bool any_miss = false;
    bool in_run = false; // currently inside a contiguous run of missing blocks
    const uint64_t total = inner_->size();
    for (uint64_t b = first; b <= last; ++b) {
        if (resident_.count(b)) {
            in_run = false;
            continue;
        }
        resident_.insert(b);
        any_miss = true;
        const uint64_t block_start = b * block_size_;
        metrics_.remote_bytes += std::min<uint64_t>(block_size_, total - block_start);
        if (!in_run) {
            ++metrics_.range_gets; // start of a new coalesced GET
            in_run = true;
        }
    }
    return any_miss;
}

doris::Status MeteredFileReader::read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) {
    if (out == nullptr) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("metered: null out");
    RETURN_IF_ERROR(validate_range(offset, len));
    ++metrics_.read_at_calls;
    metrics_.total_request_bytes += len;
    // A single blocking read: any miss forces one serial round (the next offset is
    // not known until these bytes return).
    if (account_blocks(offset, len)) ++metrics_.serial_rounds;
    return inner_->read_at(offset, len, out);
}

doris::Status MeteredFileReader::read_batch(const std::vector<Range>& ranges,
                                     std::vector<std::vector<uint8_t>>* outs) {
    if (outs == nullptr) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("metered: null batch out");
    for (const Range& r : ranges) {
        RETURN_IF_ERROR(validate_range(r.offset, r.len));
    }

    // Gather the union of touched blocks so coalescing spans the whole batch, and
    // the entire batch counts as at most one serial round.
    std::vector<uint64_t> blocks;
    for (const Range& r : ranges) {
        metrics_.total_request_bytes += r.len;
        if (r.len == 0) continue;
        uint64_t first = 0, last = 0;
        block_range(r.offset, r.len, block_size_, &first, &last);
        for (uint64_t b = first; b <= last; ++b) blocks.push_back(b);
    }
    metrics_.read_at_calls += ranges.size();

    std::sort(blocks.begin(), blocks.end());
    blocks.erase(std::unique(blocks.begin(), blocks.end()), blocks.end());

    bool any_miss = false;
    const uint64_t total = inner_->size();
    uint64_t prev_miss = 0;
    bool have_prev = false;
    for (uint64_t b : blocks) {
        if (resident_.count(b)) continue;
        resident_.insert(b);
        any_miss = true;
        metrics_.remote_bytes += std::min<uint64_t>(block_size_, total - b * block_size_);
        if (!have_prev || b != prev_miss + 1) ++metrics_.range_gets; // new run
        prev_miss = b;
        have_prev = true;
    }
    if (any_miss) ++metrics_.serial_rounds;

    // Delegate the actual byte fetch to the inner reader's batch path, so a backend
    // that fetches a batch concurrently (e.g. S3FileReader) realizes the planned
    // round as parallel GETs (matching the single serial round accounted above).
    return inner_->read_batch(ranges, outs);
}

} // namespace snii::io
