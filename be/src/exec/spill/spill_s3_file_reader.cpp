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

#include "exec/spill/spill_s3_file_reader.h"

#include <algorithm>
#include <utility>

#include "common/config.h"
#include "exec/spill/spill_file_manager.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile_counter_names.h"
#include "runtime/runtime_state.h"
#include "runtime/workload_management/io_context.h"

namespace doris {

SpillS3FileReader::SpillS3FileReader(RuntimeState* state, RuntimeProfile* profile,
                                     SpillDataDir* data_dir, io::FileSystemSPtr fs,
                                     std::string spill_dir, std::vector<int64_t> part_sizes)
        : SpillFileReader(state, profile, data_dir, std::move(fs), std::move(spill_dir),
                          std::move(part_sizes)),
          _coalesce_bytes(static_cast<size_t>(
                  std::max<int64_t>(0, std::min(config::spill_s3_read_coalesce_bytes,
                                                state->spill_buffer_size_bytes())))) {
    RuntimeProfile* custom_profile = profile->get_child(profile::CUSTOM_COUNTERS);
    _remote_read_requests = custom_profile->get_counter(profile::SPILL_REMOTE_READ_REQUESTS);
}

size_t SpillS3FileReader::_footer_probe_size(size_t file_size, bool fetch_small_part) const {
    constexpr size_t kFooterTailBytes = 2 * sizeof(size_t);
    // Enough for the offsets of a few thousand blocks, so the footer usually takes one GET.
    constexpr size_t kRemoteFooterProbeBytes = 64 * 1024;
    if (_coalesce_bytes == 0) {
        return kFooterTailBytes;
    }
    return fetch_small_part && file_size <= _coalesce_bytes
                   ? file_size
                   : std::min({file_size, kRemoteFooterProbeBytes,
                               std::max(_coalesce_bytes, kFooterTailBytes)});
}

size_t SpillS3FileReader::_read_window_end(size_t index) const {
    // Take this block and the following ones as long as the read stays within
    // _coalesce_bytes. A block larger than that is still read whole.
    const size_t begin = _block_start_offsets[index];
    size_t last = index + 1;
    while (last + 1 < _block_start_offsets.size() &&
           _block_start_offsets[last + 1] - begin <= _coalesce_bytes) {
        ++last;
    }
    return _block_start_offsets[last];
}

void SpillS3FileReader::_record_spill_read(size_t bytes_read) {
    // One successful read_at() is one logical GET. Retries inside the object storage client
    // and reads that finally failed are not counted.
    if (_remote_read_requests != nullptr) {
        COUNTER_UPDATE(_remote_read_requests, 1);
    }
    if (_resource_ctx) {
        _resource_ctx->io_context()->update_spill_read_bytes_from_remote_storage(bytes_read);
        _resource_ctx->io_context()->update_spill_remote_read_requests(1);
    }
    ExecEnv::GetInstance()->spill_file_mgr()->update_spill_remote_read(bytes_read, 1);
}

} // namespace doris
