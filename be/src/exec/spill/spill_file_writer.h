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
#include "io/fs/file_writer.h"
#include "runtime/runtime_profile.h"
#include "runtime/workload_management/resource_context.h"
namespace doris {
#include "common/compile_check_begin.h"
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
/// directory.
class SpillFileWriter {
public:
    SpillFileWriter(const std::shared_ptr<SpillFile>& spill_file, RuntimeState* state,
                    RuntimeProfile* profile, SpillDataDir* data_dir, const std::string& spill_dir);

    ~SpillFileWriter();

    /// Write a block. Automatically opens the first part, splits large blocks,
    /// and rotates to a new part when the current one exceeds max_part_size.
    Status write_block(RuntimeState* state, const Block& block);

    /// Finalize: close the current part, record cumulative stats in SpillFile.
    /// After close(), no more writes are allowed.
    Status close();

private:
    /// Open the next part file (spill_dir/{_current_part_index}).
    Status _open_next_part();

    /// Close the current part: write footer, close FileWriter, update stats.
    Status _close_current_part(const std::shared_ptr<SpillFile>& spill_file);

    /// If current part size >= _max_part_size, close it.
    Status _rotate_if_needed(const std::shared_ptr<SpillFile>& spill_file);

    /// Serialize and write a single block to the current part.
    Status _write_internal(const Block& block, const std::shared_ptr<SpillFile>& spill_file);

    // ── Back-reference ──
    std::weak_ptr<SpillFile> _spill_file_wptr; // weak ref; use lock() in close()

    // ── Configuration ──
    SpillDataDir* _data_dir = nullptr;
    std::string _spill_dir;
    int64_t _max_part_size;

    // ── Current part state (reset on rotation) ──
    size_t _current_part_index = 0;
    std::string _current_part_path;
    std::unique_ptr<doris::io::FileWriter> _file_writer;
    size_t _part_written_blocks = 0;
    int64_t _part_written_bytes = 0;
    size_t _part_max_sub_block_size = 0;
    std::string _part_meta;

    // ── Cumulative state ──
    int64_t _total_written_bytes = 0;
    size_t _total_parts = 0;
    bool _closed = false;

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

    std::shared_ptr<ResourceContext> _resource_ctx = nullptr;
};
using SpillFileWriterSPtr = std::shared_ptr<SpillFileWriter>;
} // namespace doris

#include "common/compile_check_end.h"
