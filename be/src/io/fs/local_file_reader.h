#pragma once

#include "io/fs/file_reader.h"
#include "io/fs/path.h"
#include "util/file_cache.h"

namespace doris {
namespace io {

class LocalFileReader final : public FileReader {
public:
    LocalFileReader(Path path, size_t file_size,
                    std::shared_ptr<OpenedFileHandle<int>> file_handle);

    ~LocalFileReader() override;

    Status close() override;

    Status read_at(size_t offset, Slice result, size_t* bytes_read) override;

    const Path& path() const override { return _path; }

    size_t size() const override { return _file_size; }

private:
    std::shared_ptr<OpenedFileHandle<int>> _file_handle;
    int _fd; // ref
    Path _path;
    size_t _file_size;

    std::atomic_bool _closed;
};

} // namespace io
} // namespace doris
