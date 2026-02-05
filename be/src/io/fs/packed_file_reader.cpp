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

#include "io/fs/packed_file_reader.h"

#include <algorithm>
#include <utility>

#include "common/logging.h"
#include "common/status.h"
#include "io/fs/file_reader.h"

namespace doris::io {

PackedFileReader::PackedFileReader(FileReaderSPtr inner_reader, Path path, int64_t offset,
                                   int64_t size)
        : _inner_reader(std::move(inner_reader)),
          _path(std::move(path)),
          _packed_file_offset(offset),
          _file_size(size) {
    DCHECK(_inner_reader != nullptr);
}

PackedFileReader::~PackedFileReader() {
    if (!_closed) {
        static_cast<void>(close());
    }
}

Status PackedFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                      const IOContext* io_ctx) {
    if (_closed) {
        return Status::InternalError("FileReader is already closed");
    }

    // Calculate the actual offset in packed file
    size_t actual_offset = _packed_file_offset + offset;

    // Calculate the maximum bytes we can read
    size_t max_read = std::min(result.get_size(), static_cast<size_t>(_file_size - offset));

    // Adjust result slice to the actual size we can read
    Slice adjusted_result(result.get_data(), max_read);

    // Read from packed file at the adjusted offset
    auto s = _inner_reader->read_at(actual_offset, adjusted_result, bytes_read, io_ctx);
    if (!s.ok()) {
        LOG(WARNING) << "failed to read packed file: " << _path.native() << ", offset: " << offset
                     << ", actual offset: " << actual_offset
                     << ", result size: " << adjusted_result.get_size()
                     << ", error: " << s.to_string()
                     << ", packed file offset: " << _packed_file_offset
                     << ", file size: " << _file_size;
        return s;
    }

    return Status::OK();
}

Status PackedFileReader::close() {
    if (_closed) {
        return Status::OK();
    }

    if (_inner_reader) {
        RETURN_IF_ERROR(_inner_reader->close());
    }

    _closed = true;
    return Status::OK();
}

} // namespace doris::io
