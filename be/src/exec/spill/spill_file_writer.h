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

#include <atomic>
#include <memory>
#include <string>

#include "core/block/block.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "runtime/runtime_profile.h"
#include "runtime/workload_management/resource_context.h"
namespace doris {
class RuntimeState;

class SpillDataDir;
class SpillFile;

/// SpillFileWriter writes blocks to a SpillFile, automatically managing
/// part-file rotation when a part exceeds the configured size threshold
/// (config::spill_file_part_size_bytes).
///
/// Usage:
///   SpillFileWriterSPtr writer;
///   RETURN_IF_ERROR(spill_file->create_writer(state, profile, writer));
///   RETURN_IF_ERROR(writer->write_block(state, block));
///   RETURN_IF_ERROR(writer->close());
///
/// Part rotation is fully internal. Each part file has its own footer with
/// block offset metadata. Parts are named 0, 1, 2, ... within the SpillFile's
/// directory. A part is closed synchronously when it is rotated out or when close() is called.
/// If the SpillFile is destroyed before the writer is closed, the unfinished part is discarded.
///
/// Files are created on the file system pinned in the SpillFile. This base class handles
/// common part lifecycle and local writes; SpillS3FileWriter provides object-storage writes.
class SpillFileWriter {
public:
    SpillFileWriter(const std::shared_ptr<SpillFile>& spill_file, RuntimeState* state,
                    RuntimeProfile* profile, SpillDataDir* data_dir, io::FileSystemSPtr fs,
                    std::string spill_dir);

    virtual ~SpillFileWriter();

    /// Write a block. Automatically opens the first part, splits large blocks,
    /// and rotates to a new part when the current one exceeds max_part_size.
    Status write_block(RuntimeState* state, const Block& block);

    /// Finalize: close the current part, record cumulative stats in SpillFile. After close(),
    /// no more writes are allowed. After a failed write_block() the file cannot be completed:
    /// close() drops the current part and returns that failure.
    Status close();

    /// Largest serialized record and source Block written to this file, across all parts.
    /// Analytic replay uses these values before opening its readers.
    size_t max_record_bytes() const { return _max_record_bytes; }
    size_t max_block_bytes() const { return _max_block_bytes; }
    size_t max_footer_bytes() const { return _max_footer_bytes; }

protected:
    /// Preserve the first append failure for the whole part, including its footer.
    virtual Status _append_to_part(const Slice& data);

    /// Record bytes in the storage-specific query I/O context.
    virtual void _record_spill_write_bytes(int64_t bytes);

    /// Account a closed or discarded part after its writer has drained on failure.
    virtual void _on_part_closed(const std::shared_ptr<SpillFile>& spill_file, bool closed,
                                 int64_t close_ns);

    const std::string& _spill_directory() const { return _spill_dir; }

private:
    Status _close_impl(const std::shared_ptr<SpillFile>& spill_file);

    friend class SpillFile;

    Status _write_block(RuntimeState* state, const Block& block);

    /// Open the next part file (spill_dir/{_current_part_index}).
    Status _open_next_part(const std::shared_ptr<SpillFile>& spill_file);

    /// Close the current part: write the footer, close the file writer synchronously
    /// (draining in-flight uploads on failure), account its statistics, register
    /// the successful part with the SpillFile, and advance to the next part index.
    /// Statistics are accounted whether it succeeds or not.
    Status _close_current_part(const std::shared_ptr<SpillFile>& spill_file);

    /// Reset the per-part state and move on to the next part index.
    void _advance_part();

    /// Called by SpillFile::gc() when the SpillFile goes away before this writer was closed
    /// (e.g. an error unwound the owner of the file first). Drops the current part without
    /// a footer and without publishing it, and unregisters from the SpillFile, so that nothing
    /// is charged to or written under a SpillFile that no longer exists.
    void _discard(SpillFile* spill_file);

    /// If current part size >= _max_part_size, close it.
    Status _rotate_if_needed(const std::shared_ptr<SpillFile>& spill_file);

    /// Serialize and write a single block to the current part.
    Status _write_internal(const Block& block, const std::shared_ptr<SpillFile>& spill_file);

    // ── Back-reference ──
    std::weak_ptr<SpillFile> _spill_file_wptr; // weak ref; use lock() in close()

    // ── Configuration ──
    SpillDataDir* _data_dir = nullptr;
    io::FileSystemSPtr _fs;
    std::string _spill_dir;
    int64_t _max_part_size;

    // ── Current part state (reset on rotation) ──
    size_t _current_part_index = 0;
    std::string _current_part_path;
    size_t _part_written_blocks = 0;
    size_t _part_max_sub_block_size = 0;
    std::string _part_meta;

protected:
    std::unique_ptr<doris::io::FileWriter> _file_writer;
    int64_t _part_written_bytes = 0;
    Status _part_append_status;
    std::shared_ptr<ResourceContext> _resource_ctx = nullptr;

private:
    bool _closed = false;
    // First failed write_block(); every later write and close() returns it.
    Status _write_status;
    size_t _max_record_bytes = 0;
    size_t _max_block_bytes = 0;
    size_t _max_footer_bytes = 0;

    // ── Counters ──
    RuntimeProfile::Counter* _write_file_timer = nullptr;
    RuntimeProfile::Counter* _serialize_timer = nullptr;
    RuntimeProfile::Counter* _write_block_counter = nullptr;
    RuntimeProfile::Counter* _write_block_bytes_counter = nullptr;
    RuntimeProfile::Counter* _write_file_total_size = nullptr;
    RuntimeProfile::Counter* _write_file_current_size = nullptr;
    RuntimeProfile::Counter* _write_rows_counter = nullptr;
    RuntimeProfile::Counter* _memory_used_counter = nullptr;
    RuntimeProfile::Counter* _total_file_count = nullptr;
};
using SpillFileWriterSPtr = std::shared_ptr<SpillFileWriter>;
} // namespace doris
