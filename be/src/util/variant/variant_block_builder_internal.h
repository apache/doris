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

#include "core/string_ref.h"
#include "util/variant/variant_metadata.h"

namespace doris {

class VariantCollectionCore;
struct VariantEncodedBlockStorage;

// Internal dictionary owner for one VariantBlockBuilder encoding unit. Rows collect temporary
// key ids first; seal() fixes the sorted dictionary and id remap for the completed block.
class VariantMetadataBuilder {
public:
    VariantMetadataBuilder();
    ~VariantMetadataBuilder();

    VariantMetadataBuilder(const VariantMetadataBuilder&) = delete;
    VariantMetadataBuilder& operator=(const VariantMetadataBuilder&) = delete;
    VariantMetadataBuilder(VariantMetadataBuilder&&) = delete;
    VariantMetadataBuilder& operator=(VariantMetadataBuilder&&) = delete;

    uint32_t register_key(StringRef key);
    void seal();

    bool is_sealed() const noexcept;
    size_t num_keys() const noexcept;
    uint32_t final_id(uint32_t temporary_id) const;
    StringRef encoded_metadata() const;
    VariantMetadataRef metadata_ref() const;

private:
    friend class VariantBlockBuilder;
    friend class VariantCollectionCore;

    void _begin_row();
    void _retain_key(uint32_t temporary_id) noexcept;
    void _complete_row() noexcept;
    void _abort_row(const uint32_t* temporary_ids, size_t count, bool was_collecting) noexcept;
    void _reserve_keys(size_t count);
    void _move_encoded_metadata(VariantEncodedBlockStorage& destination) noexcept;
    StringRef _temporary_key(uint32_t temporary_id) const noexcept;
    size_t _key_capacity() const noexcept;
    size_t _key_capacity_growths() const noexcept;

    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace doris
