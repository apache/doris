// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "format/new_parquet/parquet_file_context.h"

#include <arrow/buffer.h>
#include <arrow/result.h>
#include <parquet/exception.h>

#include <exception>
#include <utility>

#include "io/fs/file_reader.h"
#include "util/slice.h"

namespace doris::parquet {
namespace {

class DorisRandomAccessFile final : public arrow::io::RandomAccessFile {
public:
    DorisRandomAccessFile(io::FileReaderSPtr file_reader, io::IOContext* io_ctx)
            : _file_reader(std::move(file_reader)), _io_ctx(io_ctx) {
        set_mode(arrow::io::FileMode::READ);
    }

    arrow::Status Close() override {
        _closed = true;
        return arrow::Status::OK();
    }

    bool closed() const override { return _closed; }

    arrow::Result<int64_t> Tell() const override { return _pos; }

    arrow::Status Seek(int64_t position) override {
        if (position < 0) {
            return arrow::Status::Invalid("negative seek position");
        }
        _pos = position;
        return arrow::Status::OK();
    }

    arrow::Result<int64_t> GetSize() override {
        if (!_file_reader) {
            return arrow::Status::IOError("Doris file reader is not open");
        }
        return static_cast<int64_t>(_file_reader->size());
    }

    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
        ARROW_ASSIGN_OR_RAISE(auto bytes_read, ReadAt(_pos, nbytes, out));
        _pos += bytes_read;
        return bytes_read;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
        ARROW_ASSIGN_OR_RAISE(auto bytes_read, Read(nbytes, buffer->mutable_data()));
        ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read, false));
        buffer->ZeroPadding();
        return buffer;
    }

    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
        if (!_file_reader) {
            return arrow::Status::IOError("Doris file reader is not open");
        }
        if (position < 0 || nbytes < 0) {
            return arrow::Status::Invalid("negative read position or length");
        }
        size_t bytes_read = 0;
        Status st = _file_reader->read_at(
                static_cast<size_t>(position),
                Slice(static_cast<uint8_t*>(out), static_cast<size_t>(nbytes)), &bytes_read,
                _io_ctx);
        if (!st.ok()) {
            return arrow::Status::IOError(st.to_string_no_stack());
        }
        return static_cast<int64_t>(bytes_read);
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position,
                                                         int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
        ARROW_ASSIGN_OR_RAISE(auto bytes_read, ReadAt(position, nbytes, buffer->mutable_data()));
        ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read, false));
        buffer->ZeroPadding();
        return buffer;
    }

private:
    io::FileReaderSPtr _file_reader;
    io::IOContext* _io_ctx = nullptr;
    int64_t _pos = 0;
    bool _closed = false;
};

} // namespace

Status arrow_status_to_doris_status(const arrow::Status& status) {
    if (status.ok()) {
        return Status::OK();
    }
    if (status.IsIOError()) {
        return Status::IOError(status.ToString());
    }
    if (status.IsInvalid()) {
        return Status::InvalidArgument(status.ToString());
    }
    return Status::InternalError(status.ToString());
}

Status ParquetFileContext::open(io::FileReaderSPtr file_reader, io::IOContext* io_ctx) {
    arrow_file = std::make_shared<DorisRandomAccessFile>(std::move(file_reader), io_ctx);
    try {
        this->file_reader = ::parquet::ParquetFileReader::Open(
                arrow_file, ::parquet::default_reader_properties());
        metadata = this->file_reader->metadata();
        schema = metadata != nullptr ? metadata->schema() : nullptr;
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption("Failed to open parquet file: {}", e.what());
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to open parquet file: {}", e.what());
    }

    if (metadata == nullptr || schema == nullptr) {
        return Status::Corruption("Failed to read parquet metadata");
    }
    return Status::OK();
}

Status ParquetFileContext::close() {
    if (file_reader != nullptr) {
        try {
            file_reader->Close();
        } catch (const std::exception&) {
            // close 需要保持幂等；这里不覆盖此前 scan 路径上的真实错误。
        }
    }
    if (arrow_file != nullptr) {
        static_cast<void>(arrow_status_to_doris_status(arrow_file->Close()));
    }
    file_reader.reset();
    arrow_file.reset();
    metadata.reset();
    schema = nullptr;
    return Status::OK();
}

} // namespace doris::parquet
