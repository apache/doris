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

#include "exprs/function/cast/variant_v2/cast_variant_v2.h"

#include <utility>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exprs/function/cast/variant_v2/cast_variant_v2_internal.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "runtime/runtime_state.h"

namespace doris::CastWrapper::variant_v2_internal {

VariantJsonFormatOptions variant_json_options(RuntimeState* state) {
    if (state == nullptr) {
        return {};
    }
    return {.timezone = &state->timezone_obj()};
}

} // namespace doris::CastWrapper::variant_v2_internal

namespace doris::CastWrapper {
namespace {

using namespace variant_v2_internal;

ForcedNulls forced_nulls(const NullMap::value_type* null_map, size_t rows) {
    return null_map == nullptr ? ForcedNulls {} : ForcedNulls {null_map, rows};
}

Status require_materialized_source(const ColumnWithTypeAndName& source_column, size_t rows,
                                   const IColumn** source) {
    const ColumnPtr& column = source_column.column;
    if (!column) {
        return Status::InternalError("Variant V2 CAST source column is null");
    }
    if (column->size() != rows) {
        return Status::InternalError("Variant V2 CAST source has {} rows, expected {}",
                                     column->size(), rows);
    }
    if (is_column_const(*column)) {
        return Status::InternalError(
                "Variant V2 CAST kernel requires a materialized source column");
    }
    *source = column.get();
    return Status::OK();
}

Status commit_result(ColumnWithTypeAndName& target_column, size_t rows, ColumnPtr output) {
    if (!output || output->size() != rows) {
        return Status::InternalError("Variant V2 CAST produced {} rows, expected {}",
                                     output ? output->size() : 0, rows);
    }
    target_column.column = std::move(output);
    return Status::OK();
}

Status execute_to_variant(const DataTypePtr& captured_from_type, RuntimeState* state,
                          const ColumnWithTypeAndName& source_column,
                          ColumnWithTypeAndName& target_column, size_t rows,
                          const NullMap::value_type* parent_null_map) {
    static_cast<void>(state);
    const IColumn* source = nullptr;
    RETURN_IF_ERROR(require_materialized_source(source_column, rows, &source));

    const DataTypePtr from_type = remove_nullable(captured_from_type);
    const PrimitiveType primitive = from_type->get_primitive_type();
    const ColumnPtr& source_ptr = source_column.column;
    ColumnPtr output;
    try {
        if (primitive == INVALID_TYPE) {
            VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = rows});
            for (size_t row_index = 0; row_index < rows; ++row_index) {
                auto row = builder.begin_row();
                row.add_null();
                row.finish();
            }
            VariantBatchBuilder encoded_block = builder.finish_batch();
            auto nulls = ColumnVariantV2::create();
            nulls->insert_encoded_batch(encoded_block);
            output = std::move(nulls);
        } else if (primitive == TYPE_VARIANT) {
            if (check_and_get_column<ColumnVariantV2>(source) == nullptr) {
                return Status::InternalError(
                        "ColumnVariantV2 CAST received a legacy Variant "
                        "column in compute-only mode");
            }
            output = source_ptr;
        } else if (primitive == TYPE_JSONB) {
            RETURN_IF_ERROR(cast_jsonb_to_variant(source_ptr, rows,
                                                  forced_nulls(parent_null_map, rows), &output));
        } else if (primitive == TYPE_ARRAY) {
            RETURN_IF_ERROR(cast_array_to_variant(source_ptr, from_type, rows,
                                                  forced_nulls(parent_null_map, rows), &output));
        } else if (is_supported_scalar_source(from_type)) {
            RETURN_IF_ERROR(cast_scalar_to_variant(source_ptr, from_type, rows,
                                                   forced_nulls(parent_null_map, rows), &output));
        } else {
            return Status::InvalidArgument("Conversion from {} to Variant V2 is not supported",
                                           from_type->get_name());
        }
    } catch (const Exception& exception) {
        return exception.to_status();
    }
    return commit_result(target_column, rows, std::move(output));
}

Status execute_from_variant(const DataTypePtr& captured_to_type, RuntimeState* state,
                            const ColumnWithTypeAndName& source_column,
                            ColumnWithTypeAndName& target_column, size_t rows,
                            const NullMap::value_type* parent_null_map) {
    const IColumn* source = nullptr;
    RETURN_IF_ERROR(require_materialized_source(source_column, rows, &source));
    const auto* source_variant = check_and_get_column<ColumnVariantV2>(source);
    if (source_variant == nullptr) {
        return Status::InternalError(
                "ColumnVariantV2 CAST received a legacy Variant column in compute-only mode");
    }

    const DataTypePtr to_type = remove_nullable(captured_to_type);
    const PrimitiveType primitive = to_type->get_primitive_type();
    ColumnPtr output;
    try {
        if (primitive == TYPE_VARIANT) {
            output = source_column.column;
        } else if (primitive == TYPE_STRING || primitive == TYPE_CHAR ||
                   primitive == TYPE_VARCHAR) {
            RETURN_IF_ERROR(cast_variant_to_string(state, *source_variant, rows,
                                                   forced_nulls(parent_null_map, rows), &output));
        } else if (primitive == TYPE_JSONB) {
            RETURN_IF_ERROR(cast_variant_to_jsonb(state, *source_variant, rows,
                                                  forced_nulls(parent_null_map, rows), &output));
        } else if (primitive == TYPE_ARRAY) {
            RETURN_IF_ERROR(cast_variant_to_array(state, *source_variant, to_type, rows,
                                                  forced_nulls(parent_null_map, rows), &output));
        } else if (is_supported_scalar_target(to_type)) {
            if (source_variant->is_typed()) {
                RETURN_IF_ERROR(cast_typed_variant_to_scalar(state, *source_variant, to_type, rows,
                                                             forced_nulls(parent_null_map, rows),
                                                             &output));
            } else {
                RETURN_IF_ERROR(cast_encoded_variant_to_scalar(
                        state, *source_variant, to_type, rows, forced_nulls(parent_null_map, rows),
                        &output));
            }
        } else {
            return Status::InvalidArgument("Conversion from Variant V2 to {} is not supported",
                                           to_type->get_name());
        }
    } catch (const Exception& exception) {
        return exception.to_status();
    }
    return commit_result(target_column, rows, std::move(output));
}

} // namespace

WrapperType create_cast_to_variant_v2_wrapper(const DataTypePtr& from_type) {
    if (!from_type) {
        return create_unsupport_wrapper("Variant V2 CAST source type is null");
    }
    return [from_type](RuntimeState* state, const ColumnWithTypeAndName& source_column,
                       ColumnWithTypeAndName& target_column, size_t rows,
                       const NullMap::value_type* parent_null_map) {
        return execute_to_variant(from_type, state, source_column, target_column, rows,
                                  parent_null_map);
    };
}

WrapperType create_cast_from_variant_v2_wrapper(const DataTypePtr& to_type,
                                                const CastOptions& /*options*/) {
    if (!to_type) {
        return create_unsupport_wrapper("Variant V2 CAST target type is null");
    }
    return [to_type](RuntimeState* state, const ColumnWithTypeAndName& source_column,
                     ColumnWithTypeAndName& target_column, size_t rows,
                     const NullMap::value_type* parent_null_map) {
        return execute_from_variant(to_type, state, source_column, target_column, rows,
                                    parent_null_map);
    };
}

} // namespace doris::CastWrapper
