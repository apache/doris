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

#include "common/status.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

namespace doris::snii::writer {

inline Status materialize_streamed_term(StreamedTermPostings&& streamed, TermPostings* output,
                                        uint32_t window_docs = 1024) {
    if (output == nullptr || streamed.source == nullptr || window_docs == 0) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "test posting materializer: invalid arguments");
    }
    *output = TermPostings();
    output->term = std::move(streamed.term);
    output->retain_positions = streamed.retain_positions;
    TermPostingBuffer buffer(nullptr);
    bool exhausted = false;
    while (!exhausted) {
        buffer.clear_reuse();
        RETURN_IF_ERROR(streamed.source->fill(window_docs, &buffer, &exhausted));
        output->docids.insert(output->docids.end(), buffer.docids().begin(), buffer.docids().end());
        output->freqs.insert(output->freqs.end(), buffer.freqs().begin(), buffer.freqs().end());
        output->positions_flat.insert(output->positions_flat.end(), buffer.positions_flat().begin(),
                                      buffer.positions_flat().end());
    }
    return Status::OK();
}

inline Status consume_streamed_term(StreamedTermPostings&& streamed, uint32_t window_docs = 1024) {
    if (streamed.source == nullptr || window_docs == 0) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "test posting consumer: invalid arguments");
    }
    TermPostingBuffer buffer(nullptr);
    bool exhausted = false;
    while (!exhausted) {
        buffer.clear_reuse();
        RETURN_IF_ERROR(streamed.source->fill(window_docs, &buffer, &exhausted));
    }
    return Status::OK();
}

} // namespace doris::snii::writer
