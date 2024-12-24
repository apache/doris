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

class Writer {
public:
    Writer() = default;

    Status write(ExchangeSinkLocalState* local_state, RuntimeState* state, vectorized::Block* block,
                 bool eos) const;

private:
    template <typename ChannelIdType>
    Status _channel_add_rows(RuntimeState* state,
                             std::vector<std::shared_ptr<vectorized::Channel>>& channels,
                             size_t partition_count, const ChannelIdType* __restrict channel_ids,
                             size_t rows, vectorized::Block* block, bool eos) const;

    template <typename ChannelPtrType>
    void _handle_eof_channel(RuntimeState* state, ChannelPtrType channel, Status st) const;
};
#include "common/compile_check_end.h"
} // namespace pipeline
} // namespace doris