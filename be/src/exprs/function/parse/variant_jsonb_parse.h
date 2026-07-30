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

#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_value.h"
#include "exprs/function/parse/variant_string_parse.h"

namespace doris {

class JsonbOutStream;
template <class OS_TYPE>
class JsonbWriterT;
using JsonbWriter = JsonbWriterT<JsonbOutStream>;

// Appends exactly one complete JSONB value to an active batch-builder row. This is the storage
// assembler adapter over the same bounded parser; failure aborts the active row.
void jsonb_to_variant(StringRef document, VariantBatchBuilder::Row& row,
                      uint32_t initial_depth = 0);

// Converts a sequence of complete JSONB documents into one shared-metadata Variant batch. Input
// bytes only need to remain alive for add_jsonb(). A failed add_jsonb()/finish_batch() is terminal,
// as is a successful finish_batch().
class JsonbToVariantEncoder {
public:
    JsonbToVariantEncoder();
    explicit JsonbToVariantEncoder(VariantBatchBuilder::ReserveHint hint);
    ~JsonbToVariantEncoder();

    JsonbToVariantEncoder(const JsonbToVariantEncoder&) = delete;
    JsonbToVariantEncoder& operator=(const JsonbToVariantEncoder&) = delete;
    JsonbToVariantEncoder(JsonbToVariantEncoder&&) noexcept;
    JsonbToVariantEncoder& operator=(JsonbToVariantEncoder&&) noexcept;

    void add_null();
    void add_jsonb(StringRef document);
    VariantBatchBuilder finish_batch();

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

// Replaces writer output with exactly one JSONB document. Legal non-canonical Variant input is
// accepted. The caller validates shared metadata once at the encoding-unit entry; this per-row API
// intentionally performs only O(1) metadata layout checks and validates keys referenced by the
// row. On failure the writer is reset and contains no usable document.
void variant_to_jsonb(VariantRef value, JsonbWriter& writer,
                      const VariantJsonFormatOptions& options = {});

} // namespace doris
