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

#include "io/fs/packed_file_writer.h"

#include <bvar/recorder.h>
#include <bvar/window.h>
#include <glog/logging.h>

#include "cloud/config.h"
#include "common/config.h"
#include "common/status.h"
#include "runtime/exec_env.h"
#include "util/slice.h"

namespace doris::io {

bvar::IntRecorder packed_file_writer_first_append_to_close_ms_recorder;
bvar::Window<bvar::IntRecorder> packed_file_writer_first_append_to_close_ms_window(
        "packed_file_writer_first_append_to_close_ms",
        &packed_file_writer_first_append_to_close_ms_recorder, /*window_size=*/10);

PackedFileWriter::PackedFileWriter(FileWriterPtr inner_writer, Path path,
                                   PackedAppendContext append_info)
        : _inner_writer(std::move(inner_writer)),
          _file_path(path.native()),
          _packed_file_manager(PackedFileManager::instance()),
          _append_info(std::move(append_info)) {
    DCHECK(_inner_writer != nullptr);
    DCHECK(!_file_path.empty());
}

PackedFileWriter::~PackedFileWriter() {
    if (_state == State::OPENED) {
        LOG(WARNING) << "PackedFileWriter destroyed without being closed, file: " << _file_path;
    }
}

Status PackedFileWriter::appendv(const Slice* data, size_t data_cnt) {
    if (_state != State::OPENED) {
        return Status::InternalError("Cannot append to closed or closing writer for file: " +
                                     _file_path);
    }

    if (!_first_append_timestamp.has_value()) {
        _first_append_timestamp = std::chrono::steady_clock::now();
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

Status PackedFileWriter::close(bool non_block) {
    if (_state == State::CLOSED) {
        return Status::OK();
    }

    auto record_close_latency = [this]() {
        if (_close_latency_recorded || !_first_append_timestamp.has_value()) {
            return;
        }
        auto now = std::chrono::steady_clock::now();
        auto latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  now - *_first_append_timestamp)
                                  .count();
        packed_file_writer_first_append_to_close_ms_recorder << latency_ms;
        if (auto* sampler = packed_file_writer_first_append_to_close_ms_recorder.get_sampler()) {
            sampler->take_sample();
        }
        _close_latency_recorded = true;
    };

    if (_state == State::ASYNC_CLOSING) {
        if (non_block) {
            return Status::InternalError("Don't submit async close multi times");
        }
        if (!_is_direct_write) {
            RETURN_IF_ERROR(_wait_packed_upload());
        } else {
            RETURN_IF_ERROR(_inner_writer->close(false));
        }
        _state = State::CLOSED;
        if (!non_block) {
            record_close_latency();
        }
        return Status::OK();
    }

    if (non_block) {
        return _close_async();
    } else {
        return _close_sync();
    }
}

Status PackedFileWriter::_close_async() {
    if (!_is_direct_write) {
        // Send small file data to packed manager
        RETURN_IF_ERROR(_send_to_packed_manager());
    } else {
        // For large files, just close the inner writer asynchronously
        RETURN_IF_ERROR(_inner_writer->close(true));
    }
    _state = State::ASYNC_CLOSING;
    return Status::OK();
}

Status PackedFileWriter::_close_sync() {
    if (!_is_direct_write) {
        // Send small file data to pack manager and wait for upload
        RETURN_IF_ERROR(_send_to_packed_manager());
        RETURN_IF_ERROR(_wait_packed_upload());
    } else {
        // For large files, close the inner writer synchronously
        RETURN_IF_ERROR(_inner_writer->close(false));
    }
    _state = State::CLOSED;
    if (!_close_latency_recorded) {
        auto now = std::chrono::steady_clock::now();
        if (_first_append_timestamp.has_value()) {
            auto latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                      now - *_first_append_timestamp)
                                      .count();
            packed_file_writer_first_append_to_close_ms_recorder << latency_ms;
            if (auto* sampler =
                        packed_file_writer_first_append_to_close_ms_recorder.get_sampler()) {
                sampler->take_sample();
            }
            _close_latency_recorded = true;
        }
    }
    return Status::OK();
}

Status PackedFileWriter::_wait_packed_upload() {
    DCHECK(!_is_direct_write);
    // Only wait if we have data that was sent to packed manager
    if (_bytes_appended > 0 && _packed_file_manager != nullptr) {
        return _packed_file_manager->wait_upload_done(_file_path);
    }
    return Status::OK();
}

Status PackedFileWriter::_switch_to_direct_write() {
    DCHECK(!_is_direct_write);

    // If we have buffered data, write it to inner writer first
    if (_buffer.size() > 0) {
        Slice buffer_slice(_buffer.data(), _buffer.size());
        RETURN_IF_ERROR(_inner_writer->appendv(&buffer_slice, 1));
        _buffer.clear();
    }

    return Status::OK();
}

Status PackedFileWriter::_send_to_packed_manager() {
    DCHECK(!_is_direct_write);

    if (_packed_file_manager == nullptr) {
        return Status::InternalError("PackedFileManager is not available");
    }
    LOG(INFO) << "send_to_packed_manager: " << _file_path << " buffer size: " << _buffer.size();

    if (_append_info.resource_id.empty()) {
        return Status::InternalError("Missing resource id for packed file append");
    }

    if (_append_info.txn_id <= 0) {
        return Status::InvalidArgument("Missing valid txn id for packed file append: " +
                                       _file_path);
    }

    Slice data_slice(_buffer.data(), _buffer.size());
    RETURN_IF_ERROR(_packed_file_manager->append_small_file(_file_path, data_slice, _append_info));
    _buffer.clear();
    return Status::OK();
}

Status PackedFileWriter::get_packed_slice_location(PackedSliceLocation* location) const {
    DCHECK(_state == State::CLOSED)
            << " file_path: " << _file_path << " bytes_appended: " << _bytes_appended;
    if (_is_direct_write) {
        *location = PackedSliceLocation {};
        return Status::OK();
    }
    RETURN_IF_ERROR(_packed_file_manager->get_packed_slice_location(_file_path, location));
    LOG(INFO) << "get_packed_slice_location: " << _file_path
              << " packed_path: " << location->packed_file_path << " " << location->offset << " "
              << location->size;
    return Status::OK();
}
} // namespace doris::io
