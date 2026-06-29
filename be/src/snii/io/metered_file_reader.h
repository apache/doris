#pragma once

#include <cstddef>
#include <cstdint>
#include <unordered_set>
#include <vector>

#include "snii/io/file_reader.h"
#include "snii/io/io_metrics.h"

namespace snii::io {

// A FileReader decorator that models an object-storage FileCache: reads are
// aligned to fixed (default 1MiB) blocks; only not-yet-resident blocks become
// remote range GETs (adjacent misses are coalesced). It is the single shared
// "yardstick" through which both single blocking reads and batched concurrent
// reads are measured.
//
//   - read_at(): a single blocking read. Any cache miss => +1 serial round
//     (the cursor must wait for bytes before the next offset is known).
//   - read_batch(): all ranges submitted concurrently => the whole batch is at
//     most one serial round (+1 iff any range misses).
class MeteredFileReader : public FileReader {
public:
    explicit MeteredFileReader(FileReader* inner, size_t block_size = (1u << 20));

    doris::Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override;
    doris::Status read_batch(const std::vector<Range>& ranges,
                      std::vector<std::vector<uint8_t>>* outs) override;
    uint64_t size() const override { return inner_->size(); }

    const IoMetrics& metrics() const { return metrics_; }
    const IoMetrics* io_metrics() const override { return &metrics_; }
    // Clears counters AND the resident block set, modelling a cold (cache-empty) query.
    void reset_metrics();

private:
    doris::Status validate_range(uint64_t offset, size_t len) const;

    // Accounts the cache effect of touching [offset, offset+len): records misses,
    // coalesced GETs, and remote bytes. Returns true iff at least one block missed.
    bool account_blocks(uint64_t offset, size_t len);

    FileReader* inner_;
    size_t block_size_;
    std::unordered_set<uint64_t> resident_;
    IoMetrics metrics_;
};

} // namespace snii::io
