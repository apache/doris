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

#pragma once

#include <chrono>
#include <optional>
#include <string>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_writer.h"
#include "io/fs/packed_file_manager.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris::io {

// FileWriter wrapper that buffers small files and writes them to packed file
// If file size exceeds threshold, it directly writes to inner writer
// Otherwise, it buffers data and writes to PackedFileManager on close
class PackedFileWriter final : public FileWriter {
public:
    PackedFileWriter(FileWriterPtr inner_writer, Path path,
                     PackedAppendContext append_info = PackedAppendContext {});
    ~PackedFileWriter() override;

    PackedFileWriter(const PackedFileWriter&) = delete;
    const PackedFileWriter& operator=(const PackedFileWriter&) = delete;

    Status appendv(const Slice* data, size_t data_cnt) override;

    const Path& path() const override { return _inner_writer->path(); }

    size_t bytes_appended() const override { return _bytes_appended; }

    State state() const override { return _state; }

    Status close(bool non_block = false) override;

    // Get merge file segment index information
    // This method should be called after close(true) to get the index information
    // Returns empty index if file is not in merge file
    Status get_packed_slice_location(PackedSliceLocation* location) const;

private:
    // Async close: submit data without waiting
    Status _close_async();

    // Sync close: submit data and wait for completion
    Status _close_sync();

    // Switch from merge file mode to direct write mode
    Status _switch_to_direct_write();

    // Send buffered data to merge file manager
    Status _send_to_packed_manager();

    // Wait for packed file to be uploaded to S3
    Status _wait_packed_upload();

    FileWriterPtr _inner_writer;
    std::string _file_path; // Store file path as string for packed file manager

    // Buffer for small files
    std::string _buffer;
    size_t _bytes_appended = 0;
    State _state = State::OPENED;
    bool _is_direct_write = false;                      // Whether to use direct write mode
    PackedFileManager* _packed_file_manager = nullptr;  // Packed file manager instance
    mutable PackedSliceLocation _packed_slice_location; // Packed slice info (mutable for lazy init)
    PackedAppendContext _append_info;
    std::optional<std::chrono::steady_clock::time_point> _first_append_timestamp;
    bool _close_latency_recorded = false;
};

} // namespace doris::io
