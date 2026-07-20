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
#include <span>

#include "core/string_ref.h"
#include "core/value/variant/variant_metadata.h"
#include "core/value/variant/variant_tracked_storage.h"
#include "core/value/variant/variant_value.h"

namespace doris {

class VariantBlockBuilder;

// Owns one immutable two-pass encoding unit. Value refs remain valid until this block is destroyed
// or moved. Moving the owner invalidates every ref obtained from the old owner.
class VariantEncodedBlock {
public:
    VariantEncodedBlock(VariantEncodedBlock&&) noexcept;
    VariantEncodedBlock& operator=(VariantEncodedBlock&&) noexcept;
    ~VariantEncodedBlock();

    VariantEncodedBlock(const VariantEncodedBlock&) = delete;
    VariantEncodedBlock& operator=(const VariantEncodedBlock&) = delete;

    size_t num_rows() const noexcept;
    VariantMetadataRef metadata_ref() const noexcept {
        return {.data = _metadata.data(), .size = _metadata.size()};
    }
    VariantRef value_at(size_t row) const;
    StringRef value_bytes() const noexcept { return {_values.data(), _values.size()}; }
    std::span<const uint32_t> value_offsets() const noexcept { return _offsets; }

private:
    friend class VariantBlockBuilder;

    VariantEncodedBlock(VariantTrackedString metadata, VariantTrackedString values,
                        DorisVector<uint32_t> offsets) noexcept;

    VariantTrackedString _metadata;
    VariantTrackedString _values;
    DorisVector<uint32_t> _offsets;
};

} // namespace doris
