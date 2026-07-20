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
#include <cstdint>

#include "core/string_ref.h"

namespace doris {

struct VariantMetadataRef {
    const char* data = nullptr;
    size_t size = 0;

    uint8_t version() const;
    bool sorted_strings() const;
    uint8_t offset_size() const;
    uint32_t dict_size() const;
    StringRef key_at(uint32_t id) const;
    int64_t find_key(StringRef key) const;

    // Checks all structural invariants, including dictionary ordering when advertised.
    void validate() const;

private:
    struct Layout {
        uint8_t offset_width;
        uint32_t num_keys;
        size_t offsets_offset;
        size_t strings_offset;
    };

    Layout _layout() const;
};

} // namespace doris
