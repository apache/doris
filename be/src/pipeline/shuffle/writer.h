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

#include "vec/sink/vdata_stream_sender.h"

namespace doris {
class RuntimeState;
class Status;
namespace vectorized {
class Block;
class Channel;
} // namespace vectorized
namespace pipeline {

#include "common/compile_check_begin.h"
class ExchangeSinkLocalState;

class WriterBase {
public:
    WriterBase() = default;

protected:
    template <typename ChannelPtrType>
    Status _handle_eof_channel(RuntimeState* state, ChannelPtrType channel, Status st) const;
    Status _add_rows_impl(RuntimeState* state,
                          std::vector<std::shared_ptr<vectorized::Channel>>& channels,
                          size_t channel_count, vectorized::Block* block, bool eos);

    // _origin_row_idx[i]: row id in original block for the i-th's data we send.
    vectorized::PaddedPODArray<uint32_t> _origin_row_idx;
    // _channel_rows_histogram[i]: number of rows for channel i in current batch
    vectorized::PaddedPODArray<uint32_t> _channel_rows_histogram;
    // _channel_start_offsets[i]: the start offset of channel i in _row_idx
    // its value equals to prefix sum of _channel_rows_histogram
    // after calculation, it will be end offset for channel i.
    vectorized::PaddedPODArray<uint32_t> _channel_pos_offsets;
};

class TrivialWriter final : public WriterBase {
public:
    TrivialWriter() = default;

    Status write(ExchangeSinkLocalState* local_state, RuntimeState* state, vectorized::Block* block,
                 bool eos);

private:
    Status _channel_add_rows(RuntimeState* state,
                             std::vector<std::shared_ptr<vectorized::Channel>>& channels,
                             size_t channel_count, const uint32_t* __restrict channel_ids,
                             size_t rows, vectorized::Block* block, bool eos);
};

// maybe auto partition
class OlapWriter final : public WriterBase {
public:
    OlapWriter() = default;

    Status write(ExchangeSinkLocalState* local_state, RuntimeState* state, vectorized::Block* block,
                 bool eos);

private:
    Status _write_normal(ExchangeSinkLocalState* local_state, RuntimeState* state,
                         vectorized::Block* block);
    // write batched data(if exists)
    Status _write_last(ExchangeSinkLocalState* local_state, RuntimeState* state,
                       vectorized::Block* block);
    template <bool NeedCheck>
    Status _channel_add_rows(RuntimeState* state,
                             std::vector<std::shared_ptr<vectorized::Channel>>& channels,
                             size_t channel_count, const int64_t* __restrict channel_ids,
                             size_t rows, vectorized::Block* block, bool eos);
};
#include "common/compile_check_end.h"
} // namespace pipeline
} // namespace doris
