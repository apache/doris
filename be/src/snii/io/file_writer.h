#pragma once

#include <cstdint>

#include "snii/common/slice.h"
#include "snii/common/status.h"

namespace snii::io {

// Append-only writer (no seek-back), so the format can be produced in a single
// streaming pass compatible with S3FileWriter / StreamSinkFileWriter / packed
// writer. All container bytes are written front-to-back; back-references are
// resolved by writing metadata last.
class FileWriter {
public:
    virtual ~FileWriter() = default;

    virtual Status append(Slice data) = 0;
    virtual Status finalize() = 0;
    virtual uint64_t bytes_written() const = 0;
};

} // namespace snii::io
