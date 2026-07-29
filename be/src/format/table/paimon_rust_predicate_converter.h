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
#include <unordered_set>
#include <vector>

#include "cctz/time_zone.h"
#include "core/data_type/define_primitive_type.h"
#include "exprs/vexpr_fwd.h"

// paimon-rust C bindings. This header (and the paimon_* C API) is only pulled
// into the rust-specific translation units (the converter, the rust reader and
// its test); it is intentionally kept out of file_scanner and other generic BE
// code, mirroring how paimon-cpp's headers are kept isolated.
extern "C" {
#include "paimon_rust/paimon.h"
}

namespace doris {
class RuntimeState;
class SlotDescriptor;
} // namespace doris

namespace doris {

// Converts Doris push-down conjuncts into a paimon-rust filter predicate.
//
// This mirrors PaimonPredicateConverter (the paimon-cpp variant) but targets the
// paimon-rust C bindings. The key API differences drive the design:
//   * paimon-cpp builds predicates standalone via PredicateBuilder using a field
//     index + paimon::FieldType + paimon::Literal.
//   * paimon-rust builds predicates from a live paimon_table handle and a column
//     name (`paimon_predicate_equal(table, column, datum)`), resolving the field
//     index and type from the table schema itself, and takes typed values as a
//     flat tagged `paimon_datum`.
//
// Because the predicate functions need the opened table, the converter is
// constructed with the table handle and must run after the table is open. The
// returned `paimon_predicate*` is owned by the caller, who must either transfer
// it to `paimon_read_builder_with_filter` (which consumes it) or release it via
// `paimon_predicate_free`.
//
// Conversion is best effort: any conjunct that cannot be represented is dropped
// (the engine still re-applies the full conjunct list to the scanned block), so
// a partial filter only ever prunes a superset and never changes results.
class PaimonRustPredicateConverter {
public:
    PaimonRustPredicateConverter(const std::vector<SlotDescriptor*>& file_slot_descs,
                                 RuntimeState* state, const paimon_table* table);

    // Builds an owned predicate AND-combining every convertible conjunct, or
    // nullptr when nothing can be pushed down.
    paimon_predicate* build(const VExprContextSPtrs& conjuncts);

private:
    struct FieldMeta {
        // Original file column name passed verbatim to the paimon_predicate_*
        // functions (rust resolves the field index/type from the schema by name).
        std::string column;
        const SlotDescriptor* slot_desc = nullptr;
    };

    // A datum plus its backing byte storage. String/Bytes datums hold a borrowed
    // pointer into `storage`; the storage must outlive the predicate build call,
    // so callers keep the holder alive and (re)bind str_data after final placement.
    struct DatumHolder {
        // The `{}` value-initializes every paimon_datum field to zero. Conversion
        // code relies on this: each branch only sets the fields its tag uses (e.g.
        // a Timestamp sets int_val but leaves int_val2/nanos at 0), so the rest
        // must already be zeroed. Do not drop the `{}`.
        paimon_datum datum {};
        std::string storage;
    };

    // Each returns an owned paimon_predicate* (transfer or free) or nullptr.
    paimon_predicate* _convert_expr(const VExprSPtr& expr);
    paimon_predicate* _convert_compound(const VExprSPtr& expr);
    paimon_predicate* _convert_in(const VExprSPtr& expr);
    paimon_predicate* _convert_binary(const VExprSPtr& expr);
    paimon_predicate* _convert_is_null(const VExprSPtr& expr, const std::string& fn_name);
    paimon_predicate* _convert_like_prefix(const VExprSPtr& expr);

    std::optional<FieldMeta> _resolve_field(const VExprSPtr& expr) const;
    std::optional<DatumHolder> _convert_literal(const VExprSPtr& expr,
                                                const SlotDescriptor& slot_desc) const;
    std::optional<std::string> _extract_string_literal(const VExprSPtr& expr) const;

    // Consumes a paimon_result_predicate: frees the error (logging it) and
    // returns the predicate, or nullptr on error.
    static paimon_predicate* _take(paimon_result_predicate result);
    // Point a String/Bytes datum's str_data/str_len at the given stable storage.
    static void _bind_datum_storage(paimon_datum* datum, const std::string& storage);

    static std::string _normalize_name(std::string_view name);
    static std::optional<std::string> _next_prefix(const std::string& prefix);
    static int32_t _seconds_to_days(int64_t seconds);
    static bool _is_integer_type(PrimitiveType type);
    static bool _is_string_type(PrimitiveType type);
    static bool _is_decimal_type(PrimitiveType type);
    static bool _is_date_type(PrimitiveType type);
    static bool _is_datetime_type(PrimitiveType type);
    // Whether a slot of this type can be represented as a paimon_datum (mirrors
    // the type coverage of paimon-cpp's _to_paimon_field_type).
    static bool _is_supported_slot_type(PrimitiveType type, uint32_t precision);

    std::unordered_set<std::string> _file_columns; // normalized file column names
    RuntimeState* _state = nullptr;
    const paimon_table* _table = nullptr;
    cctz::time_zone _gmt_tz;
};

} // namespace doris
