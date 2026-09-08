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

#include <cstdint>
#include <functional>
#include <span>
#include <vector>

#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/writer/posting_byte_buffer.h"

namespace doris::snii::writer {

class TermPostingBuffer;

struct PostingPositionView {
    std::span<const uint32_t> flat;
    TermPostingBuffer* buffer = nullptr;
    uint64_t offset = 0;
    uint64_t count = 0;

    Status read(uint64_t begin, std::span<uint32_t> destination) const;
};

// Compress replayable bytes using an admitted, fixed ZSTD workspace. The frame
// declares its complete input length and uses the existing compression level.
Status compress_posting_bytes(PostingByteBuffer* input, int level, PostingByteBuffer* output);

// Preserves canonical PRX windows and the format layer's codec-selection policy.
// Ordinary small windows use the existing encoder. Large windows build their
// RAW/PFOR/ZSTD candidates through bounded byte buffers and replay passes.
class PostingPrxEncoder {
public:
    explicit PostingPrxEncoder(MemoryReporter* reporter);
    Status build(const PostingPositionView& positions, std::span<const uint32_t> freqs,
                 int zstd_level_or_negative_for_auto, const format::PrxWindowLimits& limits,
                 format::PrxWindowBuildOutcome* outcome);
    uint64_t size() const;
    bool resident() const;
    Slice resident_bytes() const;
    Status stream_into(io::FileWriter* output);
    Status copy_to(PostingByteBuffer* output);
    Status visit_bytes(const std::function<Status(Slice)>& append);
    Status freeze_inline();
    void clear();

private:
    Status build_replayable(const PostingPositionView& positions, std::span<const uint32_t> freqs,
                            int level, const format::PrxWindowLimits& limits,
                            format::PrxWindowBuildOutcome* outcome);

    MemoryReporter* reporter_;
    MemoryReporter::Reservation resident_reservation_;
    std::vector<uint8_t> resident_;
    PostingByteBuffer replayable_;
    bool replayable_result_ = false;
};

} // namespace doris::snii::writer
