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

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/prx_decode_stats.h"

namespace doris::snii::format {

class PrxPositionIterator {
public:
    Status reset(Slice framed_window, uint32_t expected_doc_count,
                 std::span<const uint32_t> selected_doc_ordinals, PrxDecodeContext* context);
    Status seek(uint32_t doc_ordinal);
    [[nodiscard]] uint32_t freq() const { return frequency_; }
    Status next_position(uint32_t* position, bool* available);
    Status finish_doc();
    Status finish_frame();

private:
    void reset_state(PrxDecodeContext* context);
    Status initialize_frame(Slice framed_window, uint32_t expected_doc_count,
                            std::span<const uint32_t> selected_doc_ordinals);
    Status read_frequency(uint32_t* frequency);
    Status skip_positions(uint32_t count);
    Status decode_pfor_counts(uint32_t declared_total_positions);
    Status advance_pfor_cursor(uint32_t target, bool decode_partial_run, bool require_position);
    Status decode_pfor_run(uint32_t run_begin, uint32_t run_length);
    Status skip_pfor_run(uint32_t run_length);
    Status fail(Status status);

    std::vector<uint8_t> decompressed_;
    std::optional<ByteSource> payload_source_;
    PrxDecodeContext* context_ = nullptr;
    PrxDecodeStats frame_stats_;
    std::vector<uint32_t> pfor_counts_;
    std::vector<uint32_t> pfor_offsets_;
    std::array<uint32_t, format::kFrqBaseUnit> pfor_run_ {};
    uint32_t pfor_run_begin_ = 0;
    uint32_t pfor_run_length_ = 0;
    uint32_t pfor_run_index_ = 0;
    uint32_t pfor_stream_index_ = 0;
    alignas(64) std::array<uint32_t, 16> scratch_ {};
    PrxCodec codec_ = PrxCodec::kRaw;
    uint32_t doc_count_ = 0;
    uint32_t next_doc_ordinal_ = 0;
    uint32_t frequency_ = 0;
    uint32_t decoded_from_doc_ = 0;
    uint32_t scratch_position_ = 0;
    uint32_t scratch_size_ = 0;
    uint32_t previous_position_ = 0;
    bool first_position_ = true;
    bool active_doc_ = false;
    bool failed_ = false;
    bool finished_ = false;
};

} // namespace doris::snii::format
