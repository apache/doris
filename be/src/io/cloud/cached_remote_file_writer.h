#pragma once

#include "io/cloud/cloud_file_cache.h"
#include "io/fs/file_writer.h"

namespace doris {
namespace io {

class CachedRemoteFileWriter final : public FileWriter {
public:
    CachedRemoteFileWriter(FileWriterPtr remote_file_writer);
    ~CachedRemoteFileWriter() override { close(); }

    Status open() override;

    Status close(bool sync = true) override { return _remote_file_writer->close(sync); }

    Status abort() override { return _remote_file_writer->abort(); }

    Status append(const Slice& data) override;

    Status appendv(const Slice* data, size_t data_cnt) override;

    Status write_at(size_t offset, const Slice& data) override;

    Status finalize() override;

    size_t bytes_appended() const override { return _remote_file_writer->bytes_appended(); }

    // From now, the datas are appended will be cached in file cache
    void cache_data_from_current_offset() {
        _buffer.clear();
        _need_buffer = true;
        _buffer_start_offset = bytes_appended();
    }

    // cache_data_from_current_offset and put_buffer_to_cache are a pair.
    // call this method, the datas in buffer will be flush in file cache.
    // it should call after cache_data_from_current_offset
    Status put_buffer_to_cache();

    const Path& path() const override { return _remote_file_writer->path(); }

private:
    FileWriterPtr _remote_file_writer;

    std::string _buffer;
    size_t _buffer_start_offset;
    bool _need_buffer = false;
    IFileCache::Key _cache_key;
    CloudFileCachePtr _cache;
};

} // namespace io
} // namespace doris
