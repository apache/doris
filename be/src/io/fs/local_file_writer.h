#pragma once

#include <cstddef>

#include "io/fs/file_writer.h"

namespace doris {
namespace io {

class LocalFileWriter final : public FileWriter {
public:
    LocalFileWriter(Path path, int fd);
    ~LocalFileWriter() override;

    Status close() override;

    Status abort() override;

    Status append(const Slice& data) override;

    Status appendv(const Slice* data, size_t data_cnt) override;

    Status write_at(size_t offset, const Slice& data) override;

    Status finalize() override;

    size_t bytes_appended() const override { return _bytes_appended; }

private:
    Status _close(bool sync);

private:
    int _fd; // owned

    size_t _bytes_appended = 0;
    bool _dirty = false;
    bool _closed = false;
};

} // namespace io
} // namespace doris
