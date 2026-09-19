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
/// Parts are opened on the SpillDataDir's file system (local disk or object storage).
/// Part sizes are known from the writer, so no size lookup is needed on open.
///
/// On object storage every read is a GET, so the reader keeps the request count low:
/// the part footer is fetched with one tail read, adjacent blocks are coalesced into one
/// read of at most `_coalesce_bytes`, and a part no larger than that is fetched whole.
class SpillFileReader {
public:
    SpillFileReader(RuntimeState* state, RuntimeProfile* profile, SpillDataDir* data_dir,
                    std::string spill_dir, std::vector<int64_t> part_sizes);

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
    /// Open a specific part file and read its footer. With `fetch_small_part`, a part that
    /// fits in one coalesced read is fetched whole, so its blocks need no further reads.
    Status _open_part(size_t part_index, bool fetch_small_part);

    /// Read the footer (block offsets, max sub block size, block count) of the current part.
    Status _read_footer(size_t file_size, bool fetch_small_part);

    /// Point `out` at the serialized bytes of block `index` of the current part, reading
    /// them (and the following blocks that fit in the coalesce window) if not buffered.
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
    std::string _spill_dir;
    std::vector<int64_t> _part_sizes;
    size_t _part_count;
    bool _is_remote = false;
    // Upper bound of one coalesced read; 0 reads block by block (local disk).
    size_t _coalesce_bytes = 0;

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
    // Remote only, may be null when the profile does not register it.
    RuntimeProfile::Counter* _remote_read_requests = nullptr;

    std::shared_ptr<ResourceContext> _resource_ctx = nullptr;
};

using SpillFileReaderSPtr = std::shared_ptr<SpillFileReader>;

} // namespace doris
