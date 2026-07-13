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
#include <memory>
#include <span>

#include "core/string_ref.h"
#include "util/variant/variant_metadata.h"
#include "util/variant/variant_value.h"

namespace doris {

class VariantBlockBuilder;
struct VariantEncodedBlockStorage;

// Borrows one immutable shared-metadata encoding unit. Offsets are directly compatible with
// ColumnString and always contain num_rows() + 1 entries.
class VariantEncodedBlockView {
public:
    size_t num_rows() const noexcept;
    VariantMetadataRef metadata_ref() const noexcept { return _metadata; }
    VariantValueRef value_at(size_t row) const;
    StringRef value_bytes() const noexcept { return _values; }
    std::span<const uint32_t> value_offsets() const noexcept { return _offsets; }

private:
    friend class VariantEncodedBlock;

    VariantEncodedBlockView(VariantMetadataRef metadata, StringRef values,
                            std::span<const uint32_t> offsets) noexcept
            : _metadata(metadata), _values(values), _offsets(offsets) {}

    VariantMetadataRef _metadata;
    StringRef _values;
    std::span<const uint32_t> _offsets;
};

// Owns one immutable two-pass encoding unit. Views and value refs remain valid until this block is
// destroyed or moved.
class VariantEncodedBlock {
public:
    VariantEncodedBlock(VariantEncodedBlock&&) noexcept;
    VariantEncodedBlock& operator=(VariantEncodedBlock&&) noexcept;
    ~VariantEncodedBlock();

    VariantEncodedBlock(const VariantEncodedBlock&) = delete;
    VariantEncodedBlock& operator=(const VariantEncodedBlock&) = delete;

    VariantEncodedBlockView view() const noexcept;
    size_t num_rows() const noexcept { return view().num_rows(); }
    VariantMetadataRef metadata_ref() const noexcept { return view().metadata_ref(); }
    VariantValueRef value_at(size_t row) const { return view().value_at(row); }
    StringRef value_bytes() const noexcept { return view().value_bytes(); }
    std::span<const uint32_t> value_offsets() const noexcept { return view().value_offsets(); }

private:
    friend class VariantBlockBuilder;

    explicit VariantEncodedBlock(std::unique_ptr<VariantEncodedBlockStorage> storage) noexcept;

    std::unique_ptr<VariantEncodedBlockStorage> _storage;
};

} // namespace doris
