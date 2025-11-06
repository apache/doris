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

#include "io/fs/merge_file_writer.h"

#include <glog/logging.h>

#include "cloud/config.h"
#include "common/config.h"
#include "common/status.h"
#include "runtime/exec_env.h"
#include "util/slice.h"

namespace doris::io {

MergeFileWriter::MergeFileWriter(FileWriterPtr inner_writer, Path path)
        : _inner_writer(std::move(inner_writer)),
          _file_path(path.native()),
          _merge_file_manager(MergeFileManager::instance()) {
    DCHECK(_inner_writer != nullptr);
    DCHECK(!_file_path.empty());
}

MergeFileWriter::~MergeFileWriter() {
    if (_state == State::OPENED) {
        LOG(WARNING) << "MergeFileWriter destroyed without being closed, file: " << _file_path;
    }
}

Status MergeFileWriter::appendv(const Slice* data, size_t data_cnt) {
    if (_state != State::OPENED) {
        return Status::InternalError("Cannot append to closed or closing writer for file: " +
                                     _file_path);
    }

    // Calculate total size to append
    size_t total_size = 0;
    for (size_t i = 0; i < data_cnt; ++i) {
        total_size += data[i].size;
    }

    if (total_size == 0) {
        return Status::OK();
    }

    // Check if we should switch to direct write mode
    if (!_is_direct_write && _bytes_appended + total_size > config::small_file_threshold_bytes) {
        RETURN_IF_ERROR(_switch_to_direct_write());
        _is_direct_write = true;
    }

    // Write data based on current mode
    if (_is_direct_write) {
        RETURN_IF_ERROR(_inner_writer->appendv(data, data_cnt));
    } else {
        // Buffer small file data
        for (size_t i = 0; i < data_cnt; ++i) {
            _buffer.append(data[i].data, data[i].size);
        }
    }

    _bytes_appended += total_size;
    return Status::OK();
}

Status MergeFileWriter::close(bool non_block) {
    if (_state == State::CLOSED) {
        return Status::OK();
    }

    if (_state == State::ASYNC_CLOSING) {
        if (non_block) {
            return Status::InternalError("Don't submit async close multi times");
        }
        if (!_is_direct_write) {
            RETURN_IF_ERROR(_wait_merge_upload());
        } else {
            RETURN_IF_ERROR(_inner_writer->close(false));
        }
        _state = State::CLOSED;
        return Status::OK();
    }

    if (non_block) {
        return _close_async();
    } else {
        return _close_sync();
    }
}

Status MergeFileWriter::_close_async() {
    // If merge file is disabled, just use direct write
    if (!config::enable_merge_file) {
        RETURN_IF_ERROR(_inner_writer->close(true));
        _state = State::ASYNC_CLOSING;
        return Status::OK();
    }

    if (!_is_direct_write) {
        // Send small file data to merge manager
        RETURN_IF_ERROR(_send_to_merge_manager());
    } else {
        // For large files, just close the inner writer asynchronously
        RETURN_IF_ERROR(_inner_writer->close(true));
    }
    _state = State::ASYNC_CLOSING;
    return Status::OK();
}

Status MergeFileWriter::_close_sync() {
    // If merge file is disabled, just use direct write
    if (!config::enable_merge_file) {
        RETURN_IF_ERROR(_inner_writer->close(false));
        _state = State::CLOSED;
        return Status::OK();
    }

    if (!_is_direct_write) {
        // Send small file data to merge manager and wait for upload
        RETURN_IF_ERROR(_send_to_merge_manager());
        RETURN_IF_ERROR(_wait_merge_upload());
    } else {
        // For large files, close the inner writer synchronously
        RETURN_IF_ERROR(_inner_writer->close(false));
    }
    _state = State::CLOSED;
    return Status::OK();
}

Status MergeFileWriter::_wait_merge_upload() {
    DCHECK(!_is_direct_write);
    // Only wait if we have data that was sent to merge manager
    if (_bytes_appended > 0 && _merge_file_manager != nullptr) {
        return _merge_file_manager->wait_write_done(_file_path);
    }
    return Status::OK();
}

Status MergeFileWriter::_switch_to_direct_write() {
    DCHECK(!_is_direct_write);

    // If we have buffered data, write it to inner writer first
    if (_buffer.size() > 0) {
        Slice buffer_slice(_buffer.data(), _buffer.size());
        RETURN_IF_ERROR(_inner_writer->appendv(&buffer_slice, 1));
        _buffer.clear();
    }

    return Status::OK();
}

Status MergeFileWriter::_send_to_merge_manager() {
    DCHECK(!_is_direct_write);

    if (_buffer.size() == 0) {
        return Status::OK();
    }

    if (_merge_file_manager == nullptr) {
        return Status::InternalError("MergeFileManager is not available");
    }

    Slice data_slice(_buffer.data(), _buffer.size());
    RETURN_IF_ERROR(_merge_file_manager->append(_file_path, data_slice));
    _buffer.clear();
    return Status::OK();
}

Status MergeFileWriter::get_merge_file_index(MergeFileSegmentIndex* index) const {
    DCHECK(_state == State::CLOSED);
    if (_is_direct_write) {
        *index = MergeFileSegmentIndex {};
        return Status::OK();
    }
    RETURN_IF_ERROR(_merge_file_manager->get_merge_file_index(_file_path, index));
    return Status::OK();
}
} // namespace doris::io
