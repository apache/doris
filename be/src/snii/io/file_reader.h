#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "snii/common/slice.h"
#include "common/status.h"
#include "snii/io/io_metrics.h"

namespace snii::io {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

// One logical read request (offset, length).
struct Range {
    uint64_t offset = 0;
    size_t len = 0;
};

// The single physical-read primitive (a BE-internal read_at). All higher layers
// route reads through this so I/O can be accounted and backed by local files or
// object storage interchangeably.
class FileReader {
public:
    virtual ~FileReader() = default;

    // Reads exactly len bytes starting at offset into *out (which is resized to
    // len). Reading past EOF is an error (Corruption/IoError).
    virtual doris::Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) = 0;

    // Reads a batch of ranges that may be served concurrently. The default is a
    // sequential loop; backends that model concurrency (MeteredFileReader) or
    // perform real parallel fetches (object storage) override this.
    virtual doris::Status read_batch(const std::vector<Range>& ranges,
                              std::vector<std::vector<uint8_t>>* outs) {
        outs->resize(ranges.size());
        for (size_t i = 0; i < ranges.size(); ++i) {
            RETURN_IF_ERROR(read_at(ranges[i].offset, ranges[i].len, &(*outs)[i]));
        }
        return doris::Status::OK();
    }

    // Total size of the underlying object in bytes.
    virtual uint64_t size() const = 0;

    // Optional live metrics. Readers that do not account I/O return nullptr.
    virtual const IoMetrics* io_metrics() const { return nullptr; }
};

} // namespace snii::io
