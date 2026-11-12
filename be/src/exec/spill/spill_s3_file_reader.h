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

#include "exec/spill/spill_file_reader.h"

namespace doris {

/// S3-specific coalesced reads and request accounting for spill parts.
/// The footer is fetched with one tail read (plus one more for offsets that do not fit),
/// adjacent blocks are coalesced, and a part that fits in the read window is fetched whole.
/// A single block or the rest of the offset array larger than the window is read whole.
class SpillS3FileReader final : public SpillFileReader {
public:
    SpillS3FileReader(RuntimeState* state, RuntimeProfile* profile, SpillDataDir* data_dir,
                      io::FileSystemSPtr fs, std::string spill_dir,
                      std::vector<int64_t> part_sizes);

protected:
    size_t _footer_probe_size(size_t file_size, bool fetch_small_part) const override;
    size_t _read_window_end(size_t index) const override;
    void _record_spill_read(size_t bytes_read) override;

private:
    // Upper bound of one coalesced read; 0 reads block by block.
    size_t _coalesce_bytes = 0;
    // Optional: older profiles may not register it.
    RuntimeProfile::Counter* _remote_read_requests = nullptr;
};

} // namespace doris
