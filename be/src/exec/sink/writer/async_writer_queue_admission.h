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

#include <cstddef>

namespace doris {

inline constexpr size_t ASYNC_WRITER_QUEUE_SIZE = 3;

class AsyncWriterQueueAdmission {
public:
    void wait_for_processing_before_next_sink() { _wait_for_processing = true; }
    void begin_processing() { _block_being_processed = _wait_for_processing; }
    void finish_processing() { _block_being_processed = false; }

    [[nodiscard]] bool is_available(size_t queued_blocks) const {
        return _wait_for_processing ? queued_blocks == 0 && !_block_being_processed
                                    : queued_blocks < ASYNC_WRITER_QUEUE_SIZE;
    }

    [[nodiscard]] bool waits_for_processing() const { return _wait_for_processing; }

private:
    bool _block_being_processed = false;
    bool _wait_for_processing = false;
};

} // namespace doris
