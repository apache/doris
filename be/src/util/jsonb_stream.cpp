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

#include "jsonb_stream.h"

#include "vec/functions/cast/cast_to_string.h"

namespace doris {

void JsonbOutStream::write(double d) {
    if (size_ + MAX_DOUBLE_STR_LENGTH + 2 > capacity_) {
        realloc(MAX_DOUBLE_STR_LENGTH + 2);
    }

    int len = vectorized::CastToString::from_number(d, head_ + size_);
    assert(len > 0);
    size_ += len;
}

void JsonbOutStream::write(float d) {
    if (size_ + MAX_FLOAT_STR_LENGTH + 2 > capacity_) {
        realloc(MAX_FLOAT_STR_LENGTH + 2);
    }

    int len = vectorized::CastToString::from_number(d, head_ + size_);
    assert(len > 0);
    size_ += len;
}

} // namespace doris