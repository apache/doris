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

#include "storage/index/bkd_field_encoding.h"

#include "core/data_type/primitive_type.h"

namespace doris {

Status encode_bkd_field_ascending(FieldType ft, const Field& field, const KeyCoder* coder,
                                  std::string* out) {
    // `actual` is the primitive type of the query Field from the caller; `PrimitiveType::PT` is the
    // scalar type the BKD index stores (e.g. INT for an INT column or ARRAY<INT> index).
    // Normally they match: `int_col = 1` -> both INT; `array_contains(int_arr, 2)` -> both INT.
    // Mismatch happens when the query Field carries a non-scalar while BKD records the inner scalar:
    // `arr = []` reaches here via `FunctionComparison<EqualsOp>` with the entire const ARRAY literal
    // as the query Field, so `actual = TYPE_ARRAY` while PT is the inner scalar -- the predicate
    // cannot be answered by BKD. Return INVERTED_INDEX_EVALUATE_SKIPPED so `_apply_index_expr`
    // downgrades to scalar evaluation instead of crashing on `Field::get<PT>()` DCHECK below.
#define CASE(FT, PT)                                                                 \
    case FieldType::FT: {                                                            \
        const auto actual = field.get_type();                                        \
        if (actual != PrimitiveType::PT && actual != PrimitiveType::TYPE_NULL &&     \
            !(is_string_type(actual) && is_string_type(PrimitiveType::PT))) {        \
            return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED, false>( \
                    "BKD query value type {} does not match index type {}",          \
                    static_cast<int>(actual), static_cast<int>(ft));                 \
        }                                                                            \
        full_encode_field_as_key<PrimitiveType::PT>(field, coder, out);              \
        return Status::OK();                                                         \
    }
    switch (ft) {
        DORIS_APPLY_FOR_KEY_ENCODABLE_NON_STRING_TYPES(CASE)
    default:
        break;
    }
#undef CASE
    // NOT InternalError. Every caller reaches this only with a field type that
    // cannot be encoded, and for the SNII reader that type comes from the index
    // HEADER -- i.e. from disk, i.e. it is reachable by corruption. Doris keys
    // its scalar-evaluation fallback on specific codes (SegmentIterator::
    // _downgrade_without_index and friends); InternalError is not one of them,
    // so a damaged byte would fail the query instead of downgrading it.
    return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
            "unsupported BKD field type {}", static_cast<int>(ft));
}

} // namespace doris
