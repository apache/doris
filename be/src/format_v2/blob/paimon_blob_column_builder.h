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

#include <utility>

#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "core/pod_array.h"
#include "format_v2/blob/paimon_blob_reference.h"

namespace doris::format::paimon {

// Input representation and output representation are independent.
enum class PaimonBlobOutputMode { INLINE, DESCRIPTOR };

// Builds one FILE leaf column. Both input sources use this builder for NULL/empty values,
// size limits and FILE construction. The batch buffer is reused and then released.
class PaimonBlobColumnBuilder {
public:
    PaimonBlobColumnBuilder(const DataTypePtr& result_column_type, PaimonBlobOutputMode output_mode,
                            int64_t max_inline_size, size_t row_count);
    Status append_null();

    // Keep physical read policy at the source: indexed records validate framing/CRC;
    // descriptors read a byte range. No virtual call or serialized intermediate column.
    template <typename ReadPayload>
    Status append(const PaimonBlobReference& reference, ReadPayload&& read_payload) {
        if (_output_mode == PaimonBlobOutputMode::DESCRIPTOR) {
            _append_descriptor(reference);
        } else {
            RETURN_IF_ERROR(_check_inline_size(reference.length));
            RETURN_IF_ERROR(read_payload(&_payload_buffer));
            RETURN_IF_ERROR(_append_inline(reference.length));
        }
        return Status::OK();
    }

    MutableColumnPtr finish() { return std::move(_result_column); }

private:
    Status _check_inline_size(int64_t payload_length) const;
    Status _append_inline(int64_t payload_length);
    void _append_descriptor(const PaimonBlobReference& reference);

    PaimonBlobOutputMode _output_mode;
    int64_t _max_inline_size;
    bool _result_nullable;
    MutableColumnPtr _result_column;
    Field _file_value;
    PODArray<char> _payload_buffer;
};

} // namespace doris::format::paimon
