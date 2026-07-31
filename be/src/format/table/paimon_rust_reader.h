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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cctz/time_zone.h"
#include "common/status.h"
#include "exprs/vexpr_fwd.h"
#include "format/generic_reader.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
class SlotDescriptor;
} // namespace doris

namespace doris {

class Block;

// Reads Paimon data on BE by calling into the paimon-rust C bindings
// (libpaimon_c) through the catalog-based read pipeline:
//
//   catalog_create(warehouse) -> catalog_get_table(db, table)
//     -> read_builder -> projection -> filter
//     -> plan_from_split_bytes(FE split) -> read -> arrow record batch stream
//
// The FE-planned split bytes are deserialized directly into a one-split plan
// via `paimon_plan_from_split_bytes` (wire form is identical to the one
// paimon-cpp consumes), so each scanner reads exactly the split assigned to
// it rather than re-planning the whole table.
class PaimonRustReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(PaimonRustReader);

public:
    PaimonRustReader(const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
                     RuntimeProfile* profile, const TFileRangeDesc& range,
                     const TFileScanRangeParams* range_params);
    ~PaimonRustReader() override;

    Status init_reader();
    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status _get_columns_impl(std::unordered_map<std::string, DataTypePtr>* name_to_type) override;
    Status close() override;

    // Stores the FE push-down conjuncts. They are converted to a paimon-rust
    // filter predicate (via PaimonRustPredicateConverter) inside init_reader,
    // after the table is opened, because the paimon_predicate_* functions need
    // the live table handle to resolve fields from the schema.
    void set_push_down_conjuncts(const VExprContextSPtrs& conjuncts) {
        _push_down_conjuncts = conjuncts;
    }

protected:
    // Captures partition values FE sent via `columns_from_path*` on the range,
    // so `on_after_read_block` can back-fill them into the block. Mirrors the
    // paimon-cpp reader — a Paimon DataSplit read via the C bindings may not
    // emit partition columns in its arrow output, and the engine expects them
    // present in every block.
    Status on_before_init_reader(ReaderInitContext* ctx) override;
    Status on_after_read_block(Block* block, size_t* read_rows) override;
    Status _do_init_reader(ReaderInitContext* /*ctx*/) override { return init_reader(); }

private:
    // Opaque holder for the paimon-rust C handles (defined in the .cpp so that
    // paimon_rust/paimon.h does not leak into other translation units).
    struct PaimonHandles;

    Status _init_paimon_reader();
    // Open the table via the catalog and build the scan plan + arrow reader.
    Status _open_table_and_build_reader();
    std::optional<std::string> _resolve_table_path() const;
    std::optional<std::string> _resolve_db_name() const;
    std::optional<std::string> _resolve_table_name() const;
    // Derive `warehouse` from the table root path by stripping the trailing
    // `/<db>.db/<table>` segments (paimon catalog convention). Returns nullopt
    // when the path shape is unexpected.
    static std::optional<std::string> _derive_warehouse(std::string_view table_path,
                                                        std::string_view db_name,
                                                        std::string_view table_name);
    // Base64-decode the FE-planned split into the raw serialized DataSplit bytes.
    // Kept even though the community API cannot deserialize them yet, so we
    // fail fast on missing / malformed splits without any filesystem IO.
    Status _decode_split_bytes(std::string* out) const;
    std::vector<std::string> _build_read_columns() const;
    std::map<std::string, std::string> _build_options() const;
    // Builds a paimon-rust filter from _push_down_conjuncts and applies it to the
    // read builder. Conjuncts that cannot be represented are dropped silently
    // (the engine still re-applies the full conjunct list), but an actual failure
    // of the paimon_read_builder_with_filter C call is surfaced as an error.
    Status _apply_predicate();

    // Fills partition columns in the block from FE-provided path values,
    // for rows just produced by `_do_get_next_block`.
    Status _fill_partition_columns(Block* block, size_t num_rows);

    const std::vector<SlotDescriptor*>& _file_slot_descs;
    RuntimeState* _state = nullptr;
    [[maybe_unused]] RuntimeProfile* _profile = nullptr;
    const TFileRangeDesc& _range;
    const TFileScanRangeParams* _range_params = nullptr;

    VExprContextSPtrs _push_down_conjuncts;
    std::unique_ptr<PaimonHandles> _handles;

    // Partition columns handed down via `columns_from_path*` on the range.
    // Key: partition column name (matches SlotDescriptor::col_name).
    // Value: (raw path value string, slot descriptor for type conversion).
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            _partition_values;
    std::unordered_map<std::string, bool> _partition_value_is_null;
    std::unordered_map<std::string, uint32_t> _col_name_to_block_idx;
    int64_t _remaining_table_level_row_count = -1;
    bool _reader_eof = false;
    cctz::time_zone _ctzz;
};

} // namespace doris
