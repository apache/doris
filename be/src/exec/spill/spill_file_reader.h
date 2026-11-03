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
#include "io/fs/file_system.h"
#include "runtime/runtime_profile.h"
#include "runtime/workload_management/resource_context.h"
#include "util/slice.h"

namespace doris {
class RuntimeState;
class Block;
class SpillDataDir;

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
///
/// Parts are opened on the file system pinned in the SpillFile (local disk or object storage).
/// Part sizes are known from the writer, so no size lookup is needed on open.
class SpillFileReader {
public:
    SpillFileReader(RuntimeState* state, RuntimeProfile* profile, SpillDataDir* data_dir,
                    io::FileSystemSPtr fs, std::string spill_dir, std::vector<int64_t> part_sizes);

    virtual ~SpillFileReader() { (void)close(); }

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

protected:
    /// Storage-specific read sizing. Local files read the footer tail and one block at a time.
    virtual size_t _footer_probe_size(size_t file_size, bool fetch_small_part) const;
    virtual size_t _read_window_end(size_t index) const;

    /// Account a successful store read in the storage-specific counters.
    virtual void _record_spill_read(size_t bytes_read);

private:
    /// Open a specific part file and read its footer. `fetch_small_part` allows the storage
    /// policy to fetch the whole part when it fits in one read.
    Status _open_part(size_t part_index, bool fetch_small_part);

    /// Read the footer (block offsets, max sub block size, block count) of the current part.
    Status _read_footer(size_t file_size, bool fetch_small_part);

    /// Point `out` at the serialized bytes of block `index` of the current part, reading
    /// them (and any following blocks selected by the storage policy) if not buffered.
    Status _block_slice(size_t index, Slice* out);

    /// Read exactly `len` bytes at `offset` of the current part into `data`.
    Status _read_exact(size_t offset, char* data, size_t len);

    /// Seek implementation with status propagation.
    Status _seek_to_block(size_t block_index);

    /// Close the current part's file reader.
    void _close_current_part();

    /// Make the read buffer hold at least `size` bytes.
    void _ensure_read_buff(size_t size);

    /// Account bytes read from the store (local or remote) as one request.
    void _record_read(size_t bytes_read);

    // ── Configuration ──
    SpillDataDir* _data_dir = nullptr;
    io::FileSystemSPtr _fs;
    std::string _spill_dir;
    std::vector<int64_t> _part_sizes;
    size_t _part_count;

    // ── Current part state ──
    size_t _current_part_index = 0;
    bool _is_open = false;
    bool _part_opened = false;
    io::FileReaderSPtr _file_reader;
    size_t _part_block_count = 0;
    size_t _part_read_block_index = 0;
    size_t _part_max_sub_block_size = 0;
    // Holds the bytes of [_window_begin, _window_end) of the current part.
    PaddedPODArray<char> _read_buff;
    size_t _window_begin = 0;
    size_t _window_end = 0;

    PBlock _pb_block;

    // ── Counters ──
    RuntimeProfile::Counter* _read_file_timer = nullptr;
    RuntimeProfile::Counter* _deserialize_timer = nullptr;
    RuntimeProfile::Counter* _read_block_count = nullptr;
    RuntimeProfile::Counter* _read_block_data_size = nullptr;
    RuntimeProfile::Counter* _read_file_size = nullptr;
    RuntimeProfile::Counter* _read_rows_count = nullptr;
    RuntimeProfile::Counter* _read_file_count = nullptr;

protected:
    // Includes the footer start as the final offset, so every block has an end offset.
    std::vector<size_t> _block_start_offsets;
    std::shared_ptr<ResourceContext> _resource_ctx = nullptr;
};

using SpillFileReaderSPtr = std::shared_ptr<SpillFileReader>;

} // namespace doris
