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
#include <optional>
#include <string>
#include <vector>

#include <lance/lance.h>

namespace doris::lance {

// The saved typed error of one failed lance FFI call, captured in the only trusted
// order: the code first (the typed fact), then a copy of the message, freeing the
// provider string immediately. The message rides along only so the pinned-open
// evidence survives a later observation call; it is never logged, forwarded into a
// result, or inspected to infer anything.
struct SavedLanceError {
    LanceErrorCode code = LANCE_OK;
    std::string message;
};

// Reads and clears the thread-local lance error per the envelope discipline above.
SavedLanceError save_lance_error();

// Schema contract v1: the seven slots of one indexed field, mirroring the durable
// FE representation (Gson keys "scv"/"flds"/"fid"/"nn"/"nt"/"nul"/"fsd"/"vet"/"ven").
// The optional slots are absent from the FE JSON when they are null.
struct ContractField {
    int64_t fid = 0;                // "fid": lance schema field id (0 is a legal id)
    std::string nn;                 // "nn": normalized (case-folded) field name
    std::string nt;                 // "nt": canonical normalized type
    bool nul = false;               // "nul": field nullable
    std::optional<int32_t> fsd;     // "fsd": fixed-size-list dimension
    std::optional<std::string> vet; // "vet": vector element canonical type
    std::optional<bool> ven;        // "ven": vector element nullable

    bool operator==(const ContractField& other) const {
        return fid == other.fid && nn == other.nn && nt == other.nt && nul == other.nul &&
               fsd == other.fsd && vet == other.vet && ven == other.ven;
    }
};

struct SchemaContract {
    static constexpr int32_t SCHEMA_CONTRACT_VERSION_V1 = 1;
    // Mirrors LanceIndexSchemaContract.MAX_INDEXED_FIELDS / MAX_FIELD_STRING_BYTES.
    static constexpr size_t MAX_INDEXED_FIELDS = 64;
    static constexpr size_t MAX_FIELD_STRING_BYTES = 1024;

    int32_t scv = SCHEMA_CONTRACT_VERSION_V1; // "scv"
    std::vector<ContractField> flds;          // "flds": ordered indexed fields

    bool operator==(const SchemaContract& other) const {
        return scv == other.scv && flds == other.flds;
    }
};

// Outcome of a contract parse/recompute. Every non-OK value is a complete
// pre-invocation fact the caller maps to exactly one wire result code; no message
// text is ever produced or consulted here.
enum class ContractStatus {
    OK,
    // The contract payload or the recomputed shape is outside the supported
    // contract v1 matrix (fail-closed). Maps to
    // PRE_INVOCATION_UNSUPPORTED_SCHEMA_CONTRACT.
    UNSUPPORTED,
    // A typed provider failure (I/O or otherwise) prevented a trusted recompute.
    // Maps to PRE_INVOCATION_RESOURCE_REJECTED.
    RESOURCE_REJECTED,
    // No trusted typed fact exists (provider contract violation). The caller must
    // not emit a result frame.
    NO_TRUSTED,
};

// Parses the FE schema_contract_json (Gson form: null slots absent, key order not
// guaranteed) into a SchemaContract. UNSUPPORTED on any malformed input, including
// an empty payload (the legacy-record DROP case) or an out-of-bound representation.
ContractStatus parse_schema_contract(const std::string& json, SchemaContract* out);

// Recomputes contract v1 for the top-level column `column_name` from the pinned
// dataset, following the proven manifest-DFS zip binding model: the ArrowSchema
// tree is walked in DFS pre-order SKIPPING the single item child of every
// fixed-size-list node, and the i-th walk node pairs with the i-th data-statistics
// field id (proven: statistics entries cover every manifest field — struct/list/map
// subtrees included — but never a fixed-size-list item). Guardrails, any failure is
// UNSUPPORTED fail-closed: fragment-count caps before the expensive statistics
// scan; statistics entry count == walked node count; statistics ids strictly
// ascending; top-level ids strictly increasing; child id greater than parent id;
// no dictionary-encoded nodes; every node format inside the proven type set; the
// column resolves to exactly one top-level field with no ASCII-fold duplicate; a
// fixed-size-list column has a positive dimension and exactly one child; the
// element type of the indexed fixed-size list is float16 or float32. The column
// name fold rejects non-ASCII names (Java ROOT-fold is not byte-reproducible).
ContractStatus recompute_contract(LanceDataset* dataset, const std::string& column_name,
                                  SchemaContract* out);

// The product-matrix shape check the worker applies to the AGREED contract field
// before the native build call: a non-nullable fixed-size list of positive
// dimension whose element is float16 or float32, and num_sub_vectors dividing the
// dimension. Pure slot logic over the contract (no dataset access), shared by
// run_index_worker and unit tests.
bool vector_index_shape_supported(const ContractField& field, uint32_t num_sub_vectors);

// ── Unit-test seams (never used by production call sites) ──

// Direct access to the Arrow-format → canonical normalized-type mapping (the
// proven mapping table): unit tests pin every row byte-exactly, including the
// generic-fallback value grammar and the fail-closed rows (nullopt) that no
// creatable dataset can reach. Production code reaches it only through
// recompute_contract.
std::optional<std::string> canonical_type_for_format(const char* raw_format, int64_t flags);

// The production defaults of the fail-closed fragment caps, exposed so tests can
// restore them after forcing small values.
inline constexpr uint64_t RECOMPUTE_MAX_STATS_FRAGMENTS = 4096;
inline constexpr uint64_t RECOMPUTE_MAX_STATS_FRAGMENT_FIELD_PRODUCT = 32768;

// Overrides the fail-closed fragment caps of recompute_contract so unit tests can
// exercise the cap rejection with small values instead of building >4096-fragment
// datasets (the force_*_timeouts_for_test precedent). Values are set verbatim;
// restore the production defaults afterwards by passing the constants above. Not
// thread-safe by design: tests set it before any recompute call and reset it after.
void force_recompute_fragment_caps_for_test(uint64_t max_stats_fragments,
                                            uint64_t max_stats_fragment_field_product);

} // namespace doris::lance
