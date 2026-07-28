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
#include <vector>

#include "common/status.h"

namespace doris::snii {
class ByteSource;
namespace format {
struct PrxDecodeContext;
}
} // namespace doris::snii

namespace doris::snii::query::internal {

// The production seam shared by PosChunkDecoder and focused shape-validation
// tests. A successful format decode commits its stats before caller-level CSR
// validation runs.
Status decode_and_validate_prx_frame(ByteSource* source,
                                     std::span<const uint32_t> selected_doc_ordinals,
                                     bool decode_full, bool all_docs_selected,
                                     uint32_t expected_total_docs, size_t expected_selected_docs,
                                     std::vector<uint32_t>* pos_flat,
                                     std::vector<uint32_t>* pos_offsets,
                                     format::PrxDecodeContext* decode_context);

// Validates the CSR shape expected by phrase execution. Format-level decode
// statistics have already been committed when this function runs.
Status validate_prx_frame(std::span<const uint32_t> pos_flat, std::span<const uint32_t> pos_offsets,
                          uint32_t actual_total_docs, uint32_t expected_total_docs,
                          size_t expected_selected_docs,
                          std::span<const uint32_t> selected_doc_ordinals,
                          bool offsets_by_prx_ordinal, bool all_docs_selected);

} // namespace doris::snii::query::internal
