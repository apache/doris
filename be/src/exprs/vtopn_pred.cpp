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

#include "exprs/vtopn_pred.h"

#include <cstring>

#include "exprs/expr_zonemap_filter.h"
#include "util/simd/parquet_kernels.h"

namespace doris {

namespace {

using simd::RawComparisonOp;

size_t topn_raw_value_size(PrimitiveType type) {
    switch (type) {
#define RETURN_TOPN_RAW_SIZE(TYPE) \
    case TYPE:                     \
        return sizeof(typename PrimitiveTypeTraits<TYPE>::CppType)
        RETURN_TOPN_RAW_SIZE(TYPE_BOOLEAN);
        RETURN_TOPN_RAW_SIZE(TYPE_TINYINT);
        RETURN_TOPN_RAW_SIZE(TYPE_SMALLINT);
        RETURN_TOPN_RAW_SIZE(TYPE_INT);
        RETURN_TOPN_RAW_SIZE(TYPE_BIGINT);
        RETURN_TOPN_RAW_SIZE(TYPE_LARGEINT);
        RETURN_TOPN_RAW_SIZE(TYPE_FLOAT);
        RETURN_TOPN_RAW_SIZE(TYPE_DOUBLE);
        RETURN_TOPN_RAW_SIZE(TYPE_DATE);
        RETURN_TOPN_RAW_SIZE(TYPE_DATETIME);
        RETURN_TOPN_RAW_SIZE(TYPE_DATEV2);
        RETURN_TOPN_RAW_SIZE(TYPE_DATETIMEV2);
        RETURN_TOPN_RAW_SIZE(TYPE_TIMESTAMP_NS);
        RETURN_TOPN_RAW_SIZE(TYPE_TIMESTAMPTZ);
        // Master no longer defines a C++ carrier for legacy TYPE_TIME; Parquet time values use
        // TYPE_TIMEV2, so advertising the deprecated tag would make direct filtering unusable.
        RETURN_TOPN_RAW_SIZE(TYPE_TIMEV2);
        RETURN_TOPN_RAW_SIZE(TYPE_DECIMAL32);
        RETURN_TOPN_RAW_SIZE(TYPE_DECIMAL64);
        RETURN_TOPN_RAW_SIZE(TYPE_DECIMALV2);
        RETURN_TOPN_RAW_SIZE(TYPE_DECIMAL128I);
        RETURN_TOPN_RAW_SIZE(TYPE_DECIMAL256);
        RETURN_TOPN_RAW_SIZE(TYPE_IPV4);
        RETURN_TOPN_RAW_SIZE(TYPE_IPV6);
#undef RETURN_TOPN_RAW_SIZE
    default:
        return 0;
    }
}

bool topn_binary_type(PrimitiveType type) {
    return is_string_type(type) || type == TYPE_VARBINARY;
}

template <typename T, PrimitiveType PT>
void execute_topn_raw_comparison(const uint8_t* values, size_t num_values, const Field& bound,
                                 RawComparisonOp op, uint8_t* matches) {
    simd::raw_compare(values, num_values, bound.get<PT>(), op, matches);
}

template <typename T>
bool topn_raw_scalar_matches(const T& value, const T& bound, RawComparisonOp op) {
    return op == RawComparisonOp::LE ? value <= bound : value >= bound;
}

template <PrimitiveType PT>
void execute_topn_raw_scalar(const uint8_t* values, size_t num_values, const Field& bound,
                             RawComparisonOp op, uint8_t* matches) {
    using T = typename PrimitiveTypeTraits<PT>::CppType;
    const T& typed_bound = bound.get<PT>();
    for (size_t row = 0; row < num_values; ++row) {
        T value;
        std::memcpy(&value, values + row * sizeof(T), sizeof(T));
        matches[row] &= topn_raw_scalar_matches(value, typed_bound, op) ? 1 : 0;
    }
}

bool topn_string_matches(int comparison, RawComparisonOp op) {
    return op == RawComparisonOp::LE ? comparison <= 0 : comparison >= 0;
}

} // namespace

bool VTopNPred::can_execute_on_raw_fixed_values(const DataTypePtr& data_type, int column_id) const {
    if (_predicate == nullptr || data_type == nullptr || _children.size() != 1) {
        return false;
    }
    const auto slot = std::dynamic_pointer_cast<VSlotRef>(_children[0]);
    if (slot == nullptr || slot->column_id() != column_id || slot->data_type() == nullptr) {
        return false;
    }
    const auto raw_type = remove_nullable(data_type);
    return remove_nullable(slot->data_type())->equals(*raw_type) &&
           topn_raw_value_size(raw_type->get_primitive_type()) != 0;
}

Status VTopNPred::execute_on_raw_fixed_values(const uint8_t* values, size_t num_values,
                                              size_t value_width, const DataTypePtr& data_type,
                                              int column_id, uint8_t* matches) const {
    if (!can_execute_on_raw_fixed_values(data_type, column_id)) {
        return Status::NotSupported("TopN predicate cannot evaluate raw fixed-width values");
    }
    DORIS_CHECK(values != nullptr || num_values == 0);
    DORIS_CHECK(matches != nullptr || num_values == 0);
    const auto primitive_type = remove_nullable(data_type)->get_primitive_type();
    const size_t expected_width = topn_raw_value_size(primitive_type);
    if (value_width != expected_width) {
        return Status::Corruption("Raw TopN width {} does not match expected {}", value_width,
                                  expected_width);
    }

    // The bound is mutable and may arrive after reader initialization. Snapshot it once per batch
    // so every row observes one monotonic TopN frontier without invalidating cached capability.
    const Field bound = _predicate->get_value();
    if (bound.is_null()) {
        return Status::OK();
    }
    if (!expr_zonemap::field_types_compatible(bound.get_type(), primitive_type)) {
        return Status::InternalError("TopN bound type {} does not match raw value type {}",
                                     type_to_string(bound.get_type()),
                                     type_to_string(primitive_type));
    }
    const auto op = _predicate->is_asc() ? RawComparisonOp::LE : RawComparisonOp::GE;
    switch (primitive_type) {
    case TYPE_INT:
        execute_topn_raw_comparison<int32_t, TYPE_INT>(values, num_values, bound, op, matches);
        break;
    case TYPE_BIGINT:
        execute_topn_raw_comparison<int64_t, TYPE_BIGINT>(values, num_values, bound, op, matches);
        break;
    case TYPE_FLOAT:
        execute_topn_raw_comparison<float, TYPE_FLOAT>(values, num_values, bound, op, matches);
        break;
    case TYPE_DOUBLE:
        execute_topn_raw_comparison<double, TYPE_DOUBLE>(values, num_values, bound, op, matches);
        break;
#define EXECUTE_TOPN_RAW_SCALAR(TYPE)                                          \
    case TYPE:                                                                 \
        execute_topn_raw_scalar<TYPE>(values, num_values, bound, op, matches); \
        break
        EXECUTE_TOPN_RAW_SCALAR(TYPE_BOOLEAN);
        EXECUTE_TOPN_RAW_SCALAR(TYPE_TINYINT);
        EXECUTE_TOPN_RAW_SCALAR(TYPE_SMALLINT);
        EXECUTE_TOPN_RAW_SCALAR(TYPE_LARGEINT);
        EXECUTE_TOPN_RAW_SCALAR(TYPE_DATE);
        EXECUTE_TOPN_RAW_SCALAR(TYPE_DATETIME);
        EXECUTE_TOPN_RAW_SCALAR(TYPE_DATEV2);
        EXECUTE_TOPN_RAW_SCALAR(TYPE_DATETIMEV2);
        EXECUTE_TOPN_RAW_SCALAR(TYPE_TIMESTAMP_NS);
        EXECUTE_TOPN_RAW_SCALAR(TYPE_TIMESTAMPTZ);
        // Keep dispatch aligned with topn_raw_value_size(): legacy TYPE_TIME has no master carrier.
        EXECUTE_TOPN_RAW_SCALAR(TYPE_TIMEV2);
        EXECUTE_TOPN_RAW_SCALAR(TYPE_DECIMAL32);
        EXECUTE_TOPN_RAW_SCALAR(TYPE_DECIMAL64);
        EXECUTE_TOPN_RAW_SCALAR(TYPE_DECIMALV2);
        EXECUTE_TOPN_RAW_SCALAR(TYPE_DECIMAL128I);
        EXECUTE_TOPN_RAW_SCALAR(TYPE_DECIMAL256);
        EXECUTE_TOPN_RAW_SCALAR(TYPE_IPV4);
        EXECUTE_TOPN_RAW_SCALAR(TYPE_IPV6);
#undef EXECUTE_TOPN_RAW_SCALAR
    default:
        return Status::NotSupported("TopN raw fixed-width type {} is unsupported",
                                    type_to_string(primitive_type));
    }
    return Status::OK();
}

bool VTopNPred::can_execute_on_raw_binary_values(const DataTypePtr& data_type,
                                                 int column_id) const {
    if (_predicate == nullptr || data_type == nullptr || _children.size() != 1) {
        return false;
    }
    const auto raw_type = remove_nullable(data_type);
    const auto raw_primitive_type = raw_type->get_primitive_type();
    if (!topn_binary_type(raw_primitive_type)) {
        return false;
    }
    const auto slot = std::dynamic_pointer_cast<VSlotRef>(_children[0]);
    if (slot == nullptr || slot->column_id() != column_id || slot->data_type() == nullptr) {
        return false;
    }
    const auto slot_primitive_type = remove_nullable(slot->data_type())->get_primitive_type();
    return (is_string_type(raw_primitive_type) && is_string_type(slot_primitive_type)) ||
           (raw_primitive_type == TYPE_VARBINARY && slot_primitive_type == TYPE_VARBINARY);
}

Status VTopNPred::execute_on_raw_binary_values(const StringRef* values, size_t num_values,
                                               const DataTypePtr& data_type, int column_id,
                                               uint8_t* matches) const {
    if (!can_execute_on_raw_binary_values(data_type, column_id)) {
        return Status::NotSupported("TopN predicate cannot evaluate raw binary values");
    }
    DORIS_CHECK(values != nullptr || num_values == 0);
    DORIS_CHECK(matches != nullptr || num_values == 0);
    const Field bound = _predicate->get_value();
    if (bound.is_null()) {
        return Status::OK();
    }
    const auto primitive_type = remove_nullable(data_type)->get_primitive_type();
    StringRef bound_ref;
    if (is_string_type(primitive_type) && is_string_type(bound.get_type())) {
        const auto& value = bound.get<TYPE_STRING>();
        bound_ref = StringRef(value.data(), value.size());
    } else if (primitive_type == TYPE_VARBINARY && bound.get_type() == TYPE_VARBINARY) {
        bound_ref = bound.get<TYPE_VARBINARY>().to_string_ref();
    } else {
        return Status::InternalError("TopN bound type {} does not match raw binary type {}",
                                     type_to_string(bound.get_type()),
                                     type_to_string(primitive_type));
    }
    const auto op = _predicate->is_asc() ? RawComparisonOp::LE : RawComparisonOp::GE;
    for (size_t row = 0; row < num_values; ++row) {
        matches[row] &= topn_string_matches(values[row].compare(bound_ref), op) ? 1 : 0;
    }
    return Status::OK();
}

bool VTopNPred::can_evaluate_dictionary_filter() const {
    if (_predicate == nullptr || _predicate->nulls_first() || _children.size() != 1) {
        return false;
    }
    const auto slot = std::dynamic_pointer_cast<VSlotRef>(_children[0]);
    if (slot == nullptr || slot->data_type() == nullptr) {
        return false;
    }
    const auto primitive_type = remove_nullable(slot->data_type())->get_primitive_type();
    return topn_raw_value_size(primitive_type) != 0 || topn_binary_type(primitive_type);
}

ZoneMapFilterResult VTopNPred::evaluate_dictionary_filter(const DictionaryEvalContext& ctx) const {
    if (!can_evaluate_dictionary_filter()) {
        return ZoneMapFilterResult::kUnsupported;
    }
    const auto slot = std::dynamic_pointer_cast<VSlotRef>(_children[0]);
    DORIS_CHECK(slot != nullptr);
    const auto* dictionary = ctx.slot(slot->column_id());
    if (dictionary == nullptr ||
        !expr_zonemap::data_types_compatible(dictionary->data_type, slot->data_type())) {
        return ZoneMapFilterResult::kUnsupported;
    }
    const Field bound = _predicate->get_value();
    if (bound.is_null()) {
        return ZoneMapFilterResult::kMayMatch;
    }
    const auto primitive_type = remove_nullable(dictionary->data_type)->get_primitive_type();
    if (!expr_zonemap::field_types_compatible(bound.get_type(), primitive_type)) {
        return ZoneMapFilterResult::kUnsupported;
    }
    for (const Field& value : dictionary->values) {
        if (value.is_null()) {
            continue;
        }
        if (!expr_zonemap::field_types_compatible(value.get_type(), primitive_type)) {
            return ZoneMapFilterResult::kUnsupported;
        }
        if ((_predicate->is_asc() && value <= bound) || (!_predicate->is_asc() && value >= bound)) {
            return ZoneMapFilterResult::kMayMatch;
        }
    }
    return ZoneMapFilterResult::kNoMatch;
}

} // namespace doris
