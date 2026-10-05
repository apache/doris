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

#include <gen_cpp/data.pb.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/pod_array.h"
#include "core/pod_array_fwd.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "runtime/runtime_profile.h"
#include "runtime/workload_management/resource_context.h"

namespace doris {
class RuntimeState;
class Block;

/// SpillFileReader reads blocks sequentially across all parts of a SpillFile.
///
/// Usage:
///   auto reader = spill_file->create_reader(state, profile);
///   RETURN_IF_ERROR(reader->open());
///   bool eos = false;
///   while (!eos) { RETURN_IF_ERROR(reader->read(&block, &eos)); }
///
/// Part boundaries are transparent to the caller. When the current part is
/// exhausted, the reader automatically opens the next part.
class SpillFileReader {
public:
    SpillFileReader(RuntimeState* state, RuntimeProfile* profile, std::string spill_dir,
                    size_t part_count);

    ~SpillFileReader() { (void)close(); }

    /// Open the first part and read its footer metadata.
    Status open();

    /// Read the next block. Automatically advances across part boundaries.
    /// Sets *eos = true when all parts are exhausted.
    Status read(Block* block, bool* eos);

    /// Seek to a global block index within the whole spill file.
    /// block_index is 0-based across all parts.
    /// If block_index is out of range, the reader is positioned at EOS.
    Status seek(size_t block_index);

    Status close();

private:
    /// Open a specific part file and read its footer.
    Status _open_part(size_t part_index);

    /// Seek implementation with status propagation.
    Status _seek_to_block(size_t block_index);

    /// Close the current part's file reader.
    void _close_current_part();

    // ── Configuration ──
    std::string _spill_dir;
    size_t _part_count;

    // ── Current part state ──
    size_t _current_part_index = 0;
    bool _is_open = false;
    bool _part_opened = false;
    io::FileReaderSPtr _file_reader;
    size_t _part_block_count = 0;
    size_t _part_read_block_index = 0;
    size_t _part_max_sub_block_size = 0;
    PaddedPODArray<char> _read_buff;
    std::vector<size_t> _block_start_offsets;

    PBlock _pb_block;

    // ── Counters ──
    RuntimeProfile::Counter* _read_file_timer = nullptr;
    RuntimeProfile::Counter* _deserialize_timer = nullptr;
    RuntimeProfile::Counter* _read_block_count = nullptr;
    RuntimeProfile::Counter* _read_block_data_size = nullptr;
    RuntimeProfile::Counter* _read_file_size = nullptr;
    RuntimeProfile::Counter* _read_rows_count = nullptr;
    RuntimeProfile::Counter* _read_file_count = nullptr;

    std::shared_ptr<ResourceContext> _resource_ctx = nullptr;
};

using SpillFileReaderSPtr = std::shared_ptr<SpillFileReader>;

} // namespace doris
