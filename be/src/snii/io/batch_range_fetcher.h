#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "snii/common/slice.h"
#include "snii/common/status.h"
#include "snii/io/file_reader.h"

namespace snii::io {

// Collects the byte ranges a query plan needs, coalesces overlapping/adjacent
// ranges into physical reads, and fetches them in a single batch (one serial
// I/O round on a MeteredFileReader). Callers retrieve each requested range by
// the handle returned from add(). This is the SNII read path's batching layer:
// it front-loads range planning so reads are issued concurrently rather than
// cursor-by-cursor.
class BatchRangeFetcher {
public:
    // coalesce_gap: requests separated by a gap <= this many bytes are merged into
    // one physical read (reads a few extra bytes to save a request). 0 merges only
    // overlapping/adjacent ranges.
    explicit BatchRangeFetcher(FileReader* reader, uint64_t coalesce_gap = 0);

    // Registers a desired range; returns a handle usable with get() after fetch().
    size_t add(uint64_t offset, uint64_t len);

    // Coalesces and issues one batched read; fills internal buffers.
    Status fetch();

    // Bytes for handle h (valid only after a successful fetch(), until clear()).
    Slice get(size_t h) const;

    size_t pending() const { return reqs_.size(); }
    void clear();

private:
    struct Req {
        uint64_t offset;
        uint64_t len;
        size_t len_size = 0;   // validated size_t length after successful fetch()
        size_t phys_idx = 0;   // index into phys_ after fetch
        size_t sub_offset = 0; // byte offset of this req within its physical read
    };

    FileReader* reader_;
    uint64_t coalesce_gap_;
    std::vector<Req> reqs_;
    std::vector<std::vector<uint8_t>> phys_; // physical read buffers after fetch
};

} // namespace snii::io
