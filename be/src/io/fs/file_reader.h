#pragma once

#include "common/status.h"
#include "gutil/macros.h"
#include "io/fs/path.h"

namespace doris {
namespace io {

class FileReader {
public:
    FileReader() = default;
    virtual ~FileReader() = default;

    DISALLOW_COPY_AND_ASSIGN(FileReader);

    virtual Status close() = 0;

    virtual Status read_at(size_t offset, Slice result, size_t* bytes_read) = 0;

    virtual const Path& path() const = 0;

    virtual size_t size() const = 0;
};

} // namespace io
} // namespace doris
