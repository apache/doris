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

#include <stdint.h>

#include <map>
#include <string>
#include <vector>

#include "exec/file_reader.h"
#include "librdkafka/rdkafka.h"
#include "runtime/message_body_sink.h"
#include "runtime/stream_load/stream_load_pipe.h"

namespace doris {

class KafkaConsumerPipe : public StreamLoadPipe {
public:
    KafkaConsumerPipe(size_t max_buffered_bytes = 1024 * 1024, size_t min_chunk_size = 64 * 1024)
            : StreamLoadPipe(max_buffered_bytes, min_chunk_size) {}

    virtual ~KafkaConsumerPipe() {}

    Status append_with_line_delimiter(const char* data, size_t size) {
        Status st = append(data, size);
        if (!st.ok()) {
            return st;
        }

        // append the line delimiter
        st = append("\n", 1);
        return st;
    }

    Status append_json(const char* data, size_t size) { return append_and_flush(data, size); }
};

} // end namespace doris
