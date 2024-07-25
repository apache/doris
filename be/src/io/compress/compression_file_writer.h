#pragma once
#include <memory>

#include "io/compress/compressor.h"
#include "io/fs/file_writer.h"
namespace doris::io {
class CompressionFileWriter : public FileWriter {
public:
    CompressionFileWriter(std::unique_ptr<FileWriter> file_writer,
                          std::unique_ptr<Compressor> compressor)
            : _file_writer(std::move(file_writer)), _compressor(std::move(compressor)) {}

    // It is assumed that the destructor of the implementation class of FileWriter
    // has already handled the status issue when the file is closed.
    ~CompressionFileWriter() override = default;

    Status appendv(const Slice* data, size_t data_cnt) override = 0;
    const Path& path() const override { return _file_writer->path(); }
    size_t bytes_appended() const override { return _file_writer->bytes_appended(); }
    State state() const override { return _file_writer->state(); }
    FileCacheAllocatorBuilder* cache_builder() const override {
        return _file_writer->cache_builder();
    }
    Status close(bool non_block = false) override {
        if (!_closed) {
            _closed = true;
            RETURN_IF_ERROR(finish());
            // It doesn't matter even if it is not closed, because it is assumed
            // to be handled in the destructor of the implementation class of FileWriter
            return _file_writer->close(non_block);
        }
    }

    // for reporting error status
    virtual Status init() {
        if (_file_writer == nullptr) {
            return Status::InternalError("file writer is null");
        } else if (_compressor == nullptr) {
            return Status::InternalError("compressor is null");
        } else {
            return Status::OK();
        }
    }
    virtual Status finish() = 0;
    void reset() { _compressor->reset(); }

protected:
    bool _closed = false;
    const std::unique_ptr<FileWriter> _file_writer;
    const std::unique_ptr<Compressor> _compressor;
};
}; // namespace doris::io