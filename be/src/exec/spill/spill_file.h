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
#include <memory>
#include <string>

#include "common/status.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;

class Block;
class SpillDataDir;
class SpillFileWriter;
class SpillFileReader;
using SpillFileWriterSPtr = std::shared_ptr<SpillFileWriter>;
using SpillFileReaderSPtr = std::shared_ptr<SpillFileReader>;

/// SpillFile represents a logical spill file that may consist of multiple
/// physical "part" files on disk. Parts are managed automatically by
/// SpillFileWriter when a part exceeds the configured size threshold.
///
/// On-disk layout:
///   spill_dir/         (created lazily by SpillFileWriter on first write)
///   +-- 0              (part 0)
///   +-- 1              (part 1)
///   +-- ...
///
/// Writing workflow:
///   SpillFileWriterSPtr writer;
///   RETURN_IF_ERROR(spill_file->create_writer(state, profile, writer));
///   RETURN_IF_ERROR(writer->write_block(state, block)); // auto-rotates parts
///   RETURN_IF_ERROR(writer->close());                   // finalizes all parts
///
/// Reading workflow:
///   auto reader = spill_file->create_reader(state, profile);
///   RETURN_IF_ERROR(reader->open());
///   while (!eos) { RETURN_IF_ERROR(reader->read(&block, &eos)); }
class SpillFile : public std::enable_shared_from_this<SpillFile> {
public:
    // to avoid too many small file writes
    static constexpr size_t MIN_SPILL_WRITE_BATCH_MEM = 512 * 1024;
    static constexpr size_t MAX_SPILL_WRITE_BATCH_MEM = 32 * 1024 * 1024;

    /// @param data_dir       The spill storage directory (disk) selected by SpillFileManager.
    /// @param relative_path  Relative path under the spill root, formatted by the operator.
    ///                       e.g. "query_id/sort-node_id-task_id-unique_id"
    SpillFile(SpillDataDir* data_dir, std::string relative_path);

    SpillFile() = delete;
    SpillFile(const SpillFile&) = delete;
    SpillFile& operator=(const SpillFile&) = delete;

    ~SpillFile();

    void gc();

    /// Returns true after the writer has been closed (all data flushed).
    bool ready_for_reading() const { return _ready_for_reading; }

    /// Create a SpillFileWriter that automatically manages multi-part rotation.
    /// Only one writer should exist per SpillFile at a time.
    /// Part size threshold is read from config::spill_file_part_size_bytes.
    Status create_writer(RuntimeState* state, RuntimeProfile* profile, SpillFileWriterSPtr& writer);

    /// Create a SpillFileReader that reads sequentially across all parts.
    /// The caller should call reader->open() before reading.
    SpillFileReaderSPtr create_reader(RuntimeState* state, RuntimeProfile* profile) const;

private:
    friend class SpillFileWriter;
    friend class SpillFileManager;

    /// Called by SpillFileWriter::close() to mark writing as complete.
    void finish_writing();

    /// Called by SpillFileWriter to incrementally track bytes written to disk.
    /// This ensures SpillFile always knows the correct _total_written_bytes for
    /// gc() accounting, even if the writer's close() is never properly called.
    void update_written_bytes(int64_t delta_bytes);

    /// Called by SpillFileWriter when a part file is completed.
    void increment_part_count();

    SpillDataDir* _data_dir = nullptr;
    // Absolute path: data_dir->get_spill_data_path() + "/" + relative_path
    std::string _spill_dir;
    int64_t _total_written_bytes = 0;
    size_t _part_count = 0;
    bool _ready_for_reading = false;
    // Pointer to the currently-active writer. Mutable to allow checks from const
    // methods like create_reader(). Only one writer may be active at a time.
    mutable SpillFileWriter* _active_writer = nullptr;
};
using SpillFileSPtr = std::shared_ptr<SpillFile>;
} // namespace doris
