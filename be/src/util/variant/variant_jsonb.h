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
#include <memory>

#include "util/variant/variant_block_builder.h"
#include "util/variant/variant_json.h"
#include "util/variant/variant_value.h"

namespace doris {

class JsonbOutStream;
template <class OS_TYPE>
class JsonbWriterT;
using JsonbWriter = JsonbWriterT<JsonbOutStream>;

// Appends exactly one complete JSONB value to an active block-builder row. This is the storage
// assembler adapter over the same bounded parser; failure aborts the active row.
void jsonb_to_variant(StringRef document, VariantBlockBuilder::Row& row,
                      uint32_t initial_depth = 0);

// Converts a sequence of complete JSONB documents into one shared-metadata Variant block. Input
// bytes only need to remain alive for add_jsonb(). A failed add_jsonb()/finish_block() is terminal,
// as is a successful finish_block().
class JsonbToVariantEncoder {
public:
    JsonbToVariantEncoder();
    explicit JsonbToVariantEncoder(VariantBlockBuilder::ReserveHint hint);
    ~JsonbToVariantEncoder();

    JsonbToVariantEncoder(const JsonbToVariantEncoder&) = delete;
    JsonbToVariantEncoder& operator=(const JsonbToVariantEncoder&) = delete;
    JsonbToVariantEncoder(JsonbToVariantEncoder&&) noexcept;
    JsonbToVariantEncoder& operator=(JsonbToVariantEncoder&&) noexcept;

    void add_null();
    void add_jsonb(StringRef document);
    VariantEncodedBlock finish_block();

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

// Replaces writer output with exactly one JSONB document. Legal non-canonical Variant input is
// accepted. The caller validates shared metadata once at the encoding-unit entry; this per-row API
// intentionally performs only O(1) metadata layout checks and validates keys referenced by the
// row. On failure the writer is reset and contains no usable document.
void variant_to_jsonb(VariantValueRef value, JsonbWriter& writer,
                      const VariantJsonFormatOptions& options = {});

} // namespace doris
