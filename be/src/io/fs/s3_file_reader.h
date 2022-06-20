#pragma once

#include "gutil/macros.h"
#include "io/fs/file_reader.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_system.h"

namespace doris {
namespace io {

class S3FileReader final : public FileReader {
public:
    S3FileReader(Path path, size_t file_size, std::string key, std::string bucket,
                 S3FileSystem* fs);

    ~S3FileReader() override;

    Status close() override;

    Status read_at(size_t offset, Slice result, size_t* bytes_read) override;

    const Path& path() const override { return _path; }

    size_t size() const override { return _file_size; }

private:
    Path _path;
    size_t _file_size;
    S3FileSystem* _fs;

    std::string _bucket;
    std::string _key;
};

} // namespace io
} // namespace doris
