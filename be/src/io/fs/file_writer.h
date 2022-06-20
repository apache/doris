#pragma once

#include "common/status.h"
#include "gutil/macros.h"
#include "io/fs/path.h"

namespace doris {
namespace io {

class FileWriter {
public:
    FileWriter(Path&& path) : _path(std::move(path)) {}
    virtual ~FileWriter() = default;

    DISALLOW_COPY_AND_ASSIGN(FileWriter);

    // Normal close. Wait for all data to persist before returning.
    virtual Status close() = 0;

    // Abnormal close and remove this file.
    virtual Status abort() = 0;

    virtual Status append(const Slice& data) = 0;

    virtual Status appendv(const Slice* data, size_t data_cnt) = 0;

    virtual Status write_at(size_t offset, const Slice& data) = 0;

    // Call this method when there is no more data to write.
    // FIXME(cyx): Does not seem to be an appropriate interface for file system?
    virtual Status finalize() = 0;

    virtual size_t bytes_appended() const = 0;

    const Path& path() const { return _path; }

protected:
    Path _path;
};

} // namespace io
} // namespace doris
