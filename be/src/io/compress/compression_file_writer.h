#pragma once
#include <memory>

#include "io/fs/file_writer.h"
namespace doris::io {
class CompressionFileWriter : public FileWriter {
public:
    CompressionFileWriter(std::unique_ptr<FileWriter> file_writer)
            : _file_writer(std::move(file_writer)) {}

    // It is assumed that the destructor of the implementation class of FileWriter
    // has already handled the status issue when the file is closed.
    ~CompressionFileWriter() override = default;

    Status appendv(const Slice* data, size_t data_cnt) override;
    const Path& path() const override { return _file_writer->path(); }
    size_t bytes_appended() const override { return _file_writer->bytes_appended(); }
    State state() const override { return _file_writer->state(); }
    FileCacheAllocatorBuilder* cache_builder() const override {
        return _file_writer->cache_builder();
    }
    Status close(bool non_block = false) override {
        RETURN_IF_ERROR(finish());
        // It doesn't matter even if it is not closed, because it is assumed
        // to be handled in the destructor of the implementation class of FileWriter
        return _file_writer->close(non_block);
    }

    virtual Status finish() = 0;
    virtual Status reset() = 0;

protected:
    const std::unique_ptr<FileWriter> _file_writer;
};
}; // namespace doris::io