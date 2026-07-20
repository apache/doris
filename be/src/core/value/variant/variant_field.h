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
#include <memory>

#include "core/string_ref.h"
#include "core/value/variant/variant_value.h"

namespace doris {

// Validate a complete metadata dictionary, including every UTF-8 key. Call this once before
// validating one or more payloads that reference the same dictionary.
void validate_variant_metadata(VariantMetadataRef metadata);

// Validate exactly one recursive Variant payload. The referenced metadata must already have
// passed validate_variant_metadata().
void validate_variant_payload(VariantRef value);

// Owns one encoded Variant row. The byte layout is
// [u32 little-endian metadata_size][metadata][exactly one value].
class VariantField {
public:
    VariantField() noexcept = default;
    ~VariantField() = default;

    VariantField(const VariantField& other);
    VariantField(VariantField&& other) noexcept;
    VariantField& operator=(const VariantField& other);
    VariantField& operator=(VariantField&& other) noexcept;

    // Both entry points validate the complete row once, then own a byte-for-byte copy;
    // decode never borrows the input buffer.
    static VariantField encode(VariantRef value);
    static VariantField decode(StringRef bytes);

    // The returned view borrows this field and is invalidated by assignment or destruction. It is
    // O(1) and does not revalidate. Default and moved-from fields have empty bytes(), and ref()
    // throws for them.
    VariantRef ref() const;
    StringRef bytes() const noexcept;

    // VariantField has no ordering or equality contract; all six comparisons always throw.
    bool operator<(const VariantField&) const;
    bool operator<=(const VariantField&) const;
    bool operator==(const VariantField&) const;
    bool operator!=(const VariantField&) const;
    bool operator>=(const VariantField&) const;
    bool operator>(const VariantField&) const;

private:
    VariantField(std::unique_ptr<char[]> data, size_t size) noexcept;
    void swap(VariantField& other) noexcept;

    std::unique_ptr<char[]> _data;
    size_t _size = 0;
};

} // namespace doris
