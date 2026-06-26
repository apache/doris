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

#include "storage/index/snii/snii_doris_adapter.h"

#include <fmt/format.h>

namespace doris::segment_v2::snii_doris {

Status to_doris_status(const ::snii::Status& status) {
    if (status.ok()) {
        return Status::OK();
    }
    switch (status.code()) {
    case ::snii::StatusCode::kNotFound:
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>("SNII: {}",
                                                                       status.message());
    case ::snii::StatusCode::kUnsupported:
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>("SNII: {}", status.message());
    case ::snii::StatusCode::kInvalidArgument:
        return Status::Error<ErrorCode::INVALID_ARGUMENT>("SNII: {}", status.message());
    case ::snii::StatusCode::kCorruption:
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>("SNII: {}",
                                                                       status.message());
    case ::snii::StatusCode::kIoError:
        return Status::IOError("SNII: {}", status.message());
    case ::snii::StatusCode::kInternal:
        return Status::InternalError("SNII: {}", status.message());
    case ::snii::StatusCode::kOk:
        break;
    }
    return Status::InternalError("SNII: {}", status.message());
}

::snii::Status to_snii_status(const Status& status) {
    if (status.ok()) {
        return ::snii::Status::OK();
    }
    return ::snii::Status::IoError(status.to_string_no_stack());
}

::snii::Status DorisSniiFileWriter::append(::snii::Slice data) {
    if (_writer == nullptr) {
        return ::snii::Status::InvalidArgument("doris writer is null");
    }
    return to_snii_status(
            _writer->append(Slice(reinterpret_cast<const char*>(data.data()), data.size())));
}

::snii::Status DorisSniiFileWriter::finalize() {
    if (_writer == nullptr) {
        return ::snii::Status::InvalidArgument("doris writer is null");
    }
    return ::snii::Status::OK();
}

uint64_t DorisSniiFileWriter::bytes_written() const {
    return _writer == nullptr ? 0 : _writer->bytes_appended();
}

::snii::Status DorisSniiFileReader::read_at(uint64_t offset, size_t len,
                                            std::vector<uint8_t>* out) {
    if (_reader == nullptr) {
        return ::snii::Status::InvalidArgument("doris reader is null");
    }
    if (out == nullptr) {
        return ::snii::Status::InvalidArgument("output buffer is null");
    }
    out->resize(len);
    size_t bytes_read = 0;
    auto status = _reader->read_at(offset, Slice(out->data(), len), &bytes_read, _io_ctx);
    if (!status.ok()) {
        return to_snii_status(status);
    }
    if (bytes_read != len) {
        return ::snii::Status::IoError(
                fmt::format("short read at offset {}, expect {}, got {}", offset, len, bytes_read));
    }
    return ::snii::Status::OK();
}

uint64_t DorisSniiFileReader::size() const {
    return _reader == nullptr ? 0 : _reader->size();
}

} // namespace doris::segment_v2::snii_doris
