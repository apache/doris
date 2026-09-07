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

#include "format_v2/parquet/native_schema_desc.h"

#include <ctype.h>

#include <algorithm>
#include <functional>
#include <ostream>
#include <unordered_set>
#include <utility>

#include "common/cast_set.h"
#include "common/exception.h"
#include "common/logging.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/define_primitive_type.h"
#include "util/slice.h"
#include "util/string_util.h"

namespace doris::format::parquet {

static bool is_group_node(const tparquet::SchemaElement& schema) {
    return schema.num_children > 0;
}

static bool is_list_node(const tparquet::SchemaElement& schema) {
    // Writers may emit only the modern logical type, so collection shape must not depend on the
    // deprecated converted_type field being duplicated in the footer.
    return (schema.__isset.converted_type &&
            schema.converted_type == tparquet::ConvertedType::LIST) ||
           (schema.__isset.logicalType && schema.logicalType.__isset.LIST);
}

static bool is_map_node(const tparquet::SchemaElement& schema) {
    return (schema.__isset.converted_type &&
            (schema.converted_type == tparquet::ConvertedType::MAP ||
             schema.converted_type == tparquet::ConvertedType::MAP_KEY_VALUE)) ||
           (schema.__isset.logicalType && schema.logicalType.__isset.MAP);
}

static bool is_variant_node(const tparquet::SchemaElement& schema) {
    return schema.__isset.logicalType && schema.logicalType.__isset.VARIANT;
}

enum class VariantPrimitiveAnnotation : uint8_t {
    NONE,
    INT8,
    INT16,
    INT32,
    INT64,
    DECIMAL,
    DATE,
    TIME_MICROS,
    TIMESTAMP_MICROS,
    TIMESTAMP_NANOS,
    STRING,
    UUID,
    UNSUPPORTED,
};

static VariantPrimitiveAnnotation variant_logical_annotation(
        const tparquet::SchemaElement& schema) {
    if (!schema.__isset.logicalType) {
        return VariantPrimitiveAnnotation::NONE;
    }
    const auto& logical = schema.logicalType;
    if (logical.__isset.INTEGER) {
        if (!logical.INTEGER.isSigned) {
            return VariantPrimitiveAnnotation::UNSUPPORTED;
        }
        if (logical.INTEGER.bitWidth == 8) {
            return VariantPrimitiveAnnotation::INT8;
        }
        if (logical.INTEGER.bitWidth == 16) {
            return VariantPrimitiveAnnotation::INT16;
        }
        // Iceberg 1.11 writes full-width signed INTEGER annotations even though the Variant
        // specification represents these widths without an annotation. Keep the widths distinct
        // so validation only accepts them with the matching physical type.
        if (logical.INTEGER.bitWidth == 32) {
            return VariantPrimitiveAnnotation::INT32;
        }
        if (logical.INTEGER.bitWidth == 64) {
            return VariantPrimitiveAnnotation::INT64;
        }
        return VariantPrimitiveAnnotation::UNSUPPORTED;
    }
    if (logical.__isset.DECIMAL) {
        return VariantPrimitiveAnnotation::DECIMAL;
    }
    if (logical.__isset.DATE) {
        return VariantPrimitiveAnnotation::DATE;
    }
    if (logical.__isset.TIME) {
        return !logical.TIME.isAdjustedToUTC && logical.TIME.unit.__isset.MICROS
                       ? VariantPrimitiveAnnotation::TIME_MICROS
                       : VariantPrimitiveAnnotation::UNSUPPORTED;
    }
    if (logical.__isset.TIMESTAMP) {
        if (logical.TIMESTAMP.unit.__isset.MICROS) {
            return VariantPrimitiveAnnotation::TIMESTAMP_MICROS;
        }
        if (logical.TIMESTAMP.unit.__isset.NANOS) {
            return VariantPrimitiveAnnotation::TIMESTAMP_NANOS;
        }
        return VariantPrimitiveAnnotation::UNSUPPORTED;
    }
    if (logical.__isset.STRING) {
        return VariantPrimitiveAnnotation::STRING;
    }
    if (logical.__isset.UUID) {
        return VariantPrimitiveAnnotation::UUID;
    }
    const bool empty = !logical.__isset.MAP && !logical.__isset.LIST && !logical.__isset.ENUM &&
                       !logical.__isset.UNKNOWN && !logical.__isset.JSON && !logical.__isset.BSON &&
                       !logical.__isset.FLOAT16 && !logical.__isset.GEOMETRY &&
                       !logical.__isset.GEOGRAPHY && !logical.__isset.VARIANT;
    return empty ? VariantPrimitiveAnnotation::NONE : VariantPrimitiveAnnotation::UNSUPPORTED;
}

static VariantPrimitiveAnnotation variant_converted_annotation(
        const tparquet::SchemaElement& schema) {
    if (!schema.__isset.converted_type) {
        return VariantPrimitiveAnnotation::NONE;
    }
    switch (schema.converted_type) {
    case tparquet::ConvertedType::INT_8:
        return VariantPrimitiveAnnotation::INT8;
    case tparquet::ConvertedType::INT_16:
        return VariantPrimitiveAnnotation::INT16;
    // Parquet Java mirrors full-width logical annotations into these legacy converted types.
    case tparquet::ConvertedType::INT_32:
        return VariantPrimitiveAnnotation::INT32;
    case tparquet::ConvertedType::INT_64:
        return VariantPrimitiveAnnotation::INT64;
    case tparquet::ConvertedType::DECIMAL:
        return VariantPrimitiveAnnotation::DECIMAL;
    case tparquet::ConvertedType::DATE:
        return VariantPrimitiveAnnotation::DATE;
    case tparquet::ConvertedType::TIME_MICROS:
        return VariantPrimitiveAnnotation::TIME_MICROS;
    case tparquet::ConvertedType::TIMESTAMP_MICROS:
        return VariantPrimitiveAnnotation::TIMESTAMP_MICROS;
    case tparquet::ConvertedType::UTF8:
        return VariantPrimitiveAnnotation::STRING;
    default:
        return VariantPrimitiveAnnotation::UNSUPPORTED;
    }
}

static Status validate_variant_decimal(const tparquet::SchemaElement& schema,
                                       tparquet::Type::type physical_type) {
    int32_t precision = -1;
    int32_t scale = -1;
    if (schema.__isset.logicalType && schema.logicalType.__isset.DECIMAL) {
        precision = schema.logicalType.DECIMAL.precision;
        scale = schema.logicalType.DECIMAL.scale;
        if ((schema.__isset.precision && schema.precision != precision) ||
            (schema.__isset.scale && schema.scale != scale)) {
            return Status::Corruption(
                    "Parquet Variant DECIMAL logical and converted parameters disagree");
        }
    } else if (schema.__isset.precision && schema.__isset.scale) {
        precision = schema.precision;
        scale = schema.scale;
    }
    if (precision <= 0 || precision > 38 || scale < 0 || scale > precision) {
        return Status::Corruption("Parquet Variant DECIMAL({}, {}) is invalid", precision, scale);
    }

    if ((physical_type == tparquet::Type::INT32 && precision > 9) ||
        (physical_type == tparquet::Type::INT64 && (precision < 10 || precision > 18)) ||
        ((physical_type == tparquet::Type::BYTE_ARRAY ||
          physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) &&
         precision < 19)) {
        return Status::Corruption(
                "Parquet Variant DECIMAL precision {} does not match physical type {}", precision,
                physical_type);
    }
    if (physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
        static constexpr int32_t MAX_PRECISION_BY_LENGTH[] = {2,  4,  6,  9,  11, 14, 16, 18,
                                                              21, 23, 26, 28, 31, 33, 35, 38};
        if (!schema.__isset.type_length || schema.type_length <= 0 || schema.type_length > 16 ||
            precision > MAX_PRECISION_BY_LENGTH[schema.type_length - 1]) {
            return Status::Corruption(
                    "Parquet Variant DECIMAL precision {} does not fit fixed length {}", precision,
                    schema.__isset.type_length ? schema.type_length : -1);
        }
    }
    return Status::OK();
}

static Status validate_variant_primitive_type(const NativeFieldSchema& typed) {
    const auto& schema = typed.parquet_schema;
    auto logical = variant_logical_annotation(schema);
    auto converted = variant_converted_annotation(schema);
    if (logical == VariantPrimitiveAnnotation::UNSUPPORTED ||
        converted == VariantPrimitiveAnnotation::UNSUPPORTED ||
        (logical != VariantPrimitiveAnnotation::NONE &&
         converted != VariantPrimitiveAnnotation::NONE && logical != converted)) {
        return Status::Corruption(
                "Parquet Variant typed value {} has an unsupported logical annotation", typed.name);
    }
    const auto annotation = logical != VariantPrimitiveAnnotation::NONE ? logical : converted;
    const auto physical = schema.type;
    bool valid = false;
    switch (physical) {
    case tparquet::Type::BOOLEAN:
        valid = annotation == VariantPrimitiveAnnotation::NONE;
        break;
    case tparquet::Type::INT32:
        valid = annotation == VariantPrimitiveAnnotation::NONE ||
                annotation == VariantPrimitiveAnnotation::INT8 ||
                annotation == VariantPrimitiveAnnotation::INT16 ||
                annotation == VariantPrimitiveAnnotation::INT32 ||
                annotation == VariantPrimitiveAnnotation::DECIMAL ||
                annotation == VariantPrimitiveAnnotation::DATE;
        break;
    case tparquet::Type::INT64:
        valid = annotation == VariantPrimitiveAnnotation::NONE ||
                annotation == VariantPrimitiveAnnotation::INT64 ||
                annotation == VariantPrimitiveAnnotation::DECIMAL ||
                annotation == VariantPrimitiveAnnotation::TIME_MICROS ||
                annotation == VariantPrimitiveAnnotation::TIMESTAMP_MICROS ||
                annotation == VariantPrimitiveAnnotation::TIMESTAMP_NANOS;
        break;
    case tparquet::Type::FLOAT:
    case tparquet::Type::DOUBLE:
        valid = annotation == VariantPrimitiveAnnotation::NONE;
        break;
    case tparquet::Type::BYTE_ARRAY:
        valid = annotation == VariantPrimitiveAnnotation::NONE ||
                annotation == VariantPrimitiveAnnotation::STRING ||
                annotation == VariantPrimitiveAnnotation::DECIMAL;
        break;
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        valid = annotation == VariantPrimitiveAnnotation::DECIMAL ||
                (annotation == VariantPrimitiveAnnotation::UUID && schema.__isset.type_length &&
                 schema.type_length == 16);
        break;
    default:
        valid = false;
        break;
    }
    if (!valid) {
        return Status::Corruption(
                "Parquet Variant typed value {} has unsupported physical/logical type pair",
                typed.name);
    }
    if (annotation == VariantPrimitiveAnnotation::DECIMAL) {
        RETURN_IF_ERROR(validate_variant_decimal(schema, physical));
    }
    return Status::OK();
}

class ScopedBoolOverride {
public:
    ScopedBoolOverride(bool& target, bool value) : _target(target), _original(target) {
        _target = value;
    }
    ~ScopedBoolOverride() { _target = _original; }

private:
    bool& _target;
    bool _original;
};

Status validate_variant_layout(const NativeFieldSchema& group_field,
                               std::optional<int8_t> specification_version,
                               bool allow_paimon_shredded_layout) {
    if (specification_version.has_value() && *specification_version != 1) {
        return Status::NotSupported("Parquet Variant specification version {} is not supported",
                                    *specification_version);
    }
    if (group_field.parquet_schema.__isset.repetition_type &&
        group_field.parquet_schema.repetition_type == tparquet::FieldRepetitionType::REPEATED) {
        return Status::NotSupported("repeated Parquet Variant group {} is not supported",
                                    group_field.name);
    }
    if (group_field.children.size() < 2 || group_field.children.size() > 3) {
        return Status::Corruption(
                "Parquet Variant {} must contain metadata, value, and optional typed_value",
                group_field.name);
    }

    const NativeFieldSchema* metadata = nullptr;
    const NativeFieldSchema* value = nullptr;
    const NativeFieldSchema* typed_value = nullptr;
    for (const auto& child : group_field.children) {
        const NativeFieldSchema** target = nullptr;
        if (child.name == "metadata") {
            target = &metadata;
        } else if (child.name == "value") {
            target = &value;
        } else if (child.name == "typed_value") {
            target = &typed_value;
        } else {
            return Status::Corruption("Parquet Variant {} has unexpected child {}",
                                      group_field.name, child.name);
        }
        if (*target != nullptr) {
            return Status::Corruption("Parquet Variant {} has duplicate child {}", group_field.name,
                                      child.name);
        }
        *target = &child;
    }
    if (metadata == nullptr || value == nullptr) {
        return Status::Corruption("Parquet Variant {} requires metadata and value children",
                                  group_field.name);
    }
    const auto metadata_repetition = metadata->parquet_schema.repetition_type;
    // Paimon makes every field in its shredded carrier optional. Restrict that compatibility to
    // unannotated overrides; row materialization still rejects null metadata for a non-null value.
    const bool valid_metadata_repetition =
            metadata_repetition == tparquet::FieldRepetitionType::REQUIRED ||
            (allow_paimon_shredded_layout && typed_value != nullptr &&
             metadata_repetition == tparquet::FieldRepetitionType::OPTIONAL);
    if (!metadata->children.empty() || metadata->physical_type != tparquet::Type::BYTE_ARRAY ||
        !valid_metadata_repetition) {
        return Status::Corruption("Parquet Variant {} metadata must be a required BYTE_ARRAY",
                                  group_field.name);
    }
    const auto expected_value_repetition = typed_value == nullptr
                                                   ? tparquet::FieldRepetitionType::REQUIRED
                                                   : tparquet::FieldRepetitionType::OPTIONAL;
    // SQL nullability belongs to the outer Variant group. Only shredding makes value optional,
    // because typed_value may carry all or part of the logical value instead.
    if (!value->children.empty() || value->physical_type != tparquet::Type::BYTE_ARRAY ||
        value->parquet_schema.repetition_type != expected_value_repetition) {
        return Status::Corruption("Parquet Variant {} value must be a {} BYTE_ARRAY",
                                  group_field.name,
                                  typed_value == nullptr ? "required" : "optional");
    }
    if (typed_value != nullptr &&
        typed_value->parquet_schema.repetition_type != tparquet::FieldRepetitionType::OPTIONAL) {
        return Status::Corruption("Parquet Variant {} typed_value must be optional",
                                  group_field.name);
    }

    enum class WrapperContext : uint8_t { OBJECT_FIELD, ARRAY_ELEMENT };
    std::function<Status(const NativeFieldSchema&)> validate_typed_value;
    std::function<Status(const NativeFieldSchema&, WrapperContext)> validate_wrapper;
    validate_wrapper = [&](const NativeFieldSchema& wrapper, WrapperContext context) -> Status {
        const bool valid_wrapper_repetition =
                wrapper.parquet_schema.__isset.repetition_type &&
                (wrapper.parquet_schema.repetition_type ==
                         tparquet::FieldRepetitionType::REQUIRED ||
                 (allow_paimon_shredded_layout && wrapper.parquet_schema.repetition_type ==
                                                          tparquet::FieldRepetitionType::OPTIONAL));
        // The Parquet Variant specification requires wrapper groups. Paimon's unannotated
        // physical carrier makes them optional, so accept that representation only through the
        // table-format override. Materialization still rejects an actually null array element;
        // an absent object wrapper represents a missing key.
        if (!valid_wrapper_repetition) {
            return Status::Corruption("Parquet Variant shredded wrapper {} must be required",
                                      wrapper.name);
        }
        const NativeFieldSchema* fallback = nullptr;
        const NativeFieldSchema* typed = nullptr;
        for (const auto& child : wrapper.children) {
            if (child.name == "value") {
                if (fallback != nullptr) {
                    return Status::Corruption(
                            "Parquet Variant wrapper {} has duplicate value child", wrapper.name);
                }
                fallback = &child;
            } else if (child.name == "typed_value") {
                if (typed != nullptr) {
                    return Status::Corruption(
                            "Parquet Variant wrapper {} has duplicate typed_value child",
                            wrapper.name);
                }
                typed = &child;
            } else {
                return Status::Corruption("Parquet Variant wrapper {} has unexpected child {}",
                                          wrapper.name, child.name);
            }
        }
        if (fallback == nullptr && typed == nullptr) {
            return Status::Corruption(
                    "Parquet Variant shredded wrapper {} requires at least one of value or "
                    "typed_value",
                    wrapper.name);
        }
        // Object fields always retain the fallback value carrier; only typed_value is optional.
        // Array elements may omit either carrier when every element uses the remaining one.
        if (context == WrapperContext::OBJECT_FIELD && fallback == nullptr) {
            return Status::Corruption(
                    "Parquet Variant object wrapper {} requires an optional value child",
                    wrapper.name);
        }
        // Paimon makes this leaf required because a fallback-only array element has no typed
        // carrier; keep the exception scoped to that exact unannotated layout.
        const bool allow_required_fallback = allow_paimon_shredded_layout &&
                                             context == WrapperContext::ARRAY_ELEMENT &&
                                             typed == nullptr;
        const bool valid_fallback_repetition =
                fallback != nullptr && fallback->parquet_schema.__isset.repetition_type &&
                (fallback->parquet_schema.repetition_type ==
                         tparquet::FieldRepetitionType::OPTIONAL ||
                 (allow_required_fallback && fallback->parquet_schema.repetition_type ==
                                                     tparquet::FieldRepetitionType::REQUIRED));
        if (fallback != nullptr &&
            (!fallback->children.empty() || fallback->physical_type != tparquet::Type::BYTE_ARRAY ||
             !valid_fallback_repetition)) {
            return Status::Corruption(
                    "Parquet Variant wrapper {} value must be an {} BYTE_ARRAY", wrapper.name,
                    allow_required_fallback ? "optional or required" : "optional");
        }
        if (typed != nullptr) {
            if (!typed->parquet_schema.__isset.repetition_type ||
                typed->parquet_schema.repetition_type != tparquet::FieldRepetitionType::OPTIONAL) {
                return Status::Corruption("Parquet Variant wrapper {} typed_value must be optional",
                                          wrapper.name);
            }
            return validate_typed_value(*typed);
        }
        return Status::OK();
    };
    validate_typed_value = [&](const NativeFieldSchema& typed) -> Status {
        if (!typed.unsupported_reason.empty()) {
            return Status::NotSupported("Parquet Variant typed value {} is not supported: {}",
                                        typed.name, typed.unsupported_reason);
        }
        if (typed.children.empty()) {
            const auto& physical = typed.parquet_schema;
            if (physical.__isset.logicalType && physical.logicalType.__isset.INTEGER &&
                !physical.logicalType.INTEGER.isSigned) {
                return Status::Corruption(
                        "Parquet Variant unsigned integers are not valid typed values");
            }
            if (physical.__isset.converted_type &&
                (physical.converted_type == tparquet::ConvertedType::UINT_8 ||
                 physical.converted_type == tparquet::ConvertedType::UINT_16 ||
                 physical.converted_type == tparquet::ConvertedType::UINT_32 ||
                 physical.converted_type == tparquet::ConvertedType::UINT_64)) {
                return Status::Corruption(
                        "Parquet Variant unsigned integers are not valid typed values");
            }
            if (physical.__isset.logicalType && physical.logicalType.__isset.TIME) {
                const auto& time = physical.logicalType.TIME;
                // Variant v1 has one canonical TIME representation: local wall-clock MICROS.
                // Accepting adjusted or lower-precision forms would make projection-dependent
                // reconstruction disagree with the canonical Variant value.
                if (time.isAdjustedToUTC) {
                    return Status::Corruption(
                            "Parquet Variant TIME must have isAdjustedToUTC=false");
                }
                if (!time.unit.__isset.MICROS) {
                    return Status::Corruption(
                            "Parquet Variant TIME(MILLIS) is not supported; use TIME(MICROS)");
                }
            }
            if (physical.__isset.converted_type &&
                physical.converted_type == tparquet::ConvertedType::TIME_MILLIS) {
                return Status::Corruption(
                        "Parquet Variant TIME(MILLIS) is not supported; use TIME(MICROS)");
            }
            if (physical.__isset.logicalType && physical.logicalType.__isset.TIMESTAMP &&
                physical.logicalType.TIMESTAMP.unit.__isset.NANOS) {
                // Reject at schema open so full reconstruction and direct typed-leaf access have
                // the same precision contract instead of diverging after projection planning.
                return Status::NotSupported("Parquet Variant TIMESTAMP(NANOS) is not supported");
            }
            // Preserve precise diagnostics above, then enforce the complete Variant matrix before
            // generic Parquet inference can re-encode a value with another logical identity.
            RETURN_IF_ERROR(validate_variant_primitive_type(typed));
            return Status::OK();
        }

        const PrimitiveType primitive = remove_nullable(typed.data_type)->get_primitive_type();
        if (primitive == TYPE_STRUCT) {
            std::unordered_set<std::string> field_names;
            for (const auto& child : typed.children) {
                if (!field_names.insert(child.name).second) {
                    // Name lookup selects one physical wrapper, so duplicates would otherwise make
                    // full reconstruction and leaf projection observe different logical values.
                    return Status::Corruption(
                            "Parquet Variant object has duplicate shredded field {}", child.name);
                }
                RETURN_IF_ERROR(validate_wrapper(child, WrapperContext::OBJECT_FIELD));
            }
            return Status::OK();
        }
        if (primitive == TYPE_ARRAY && typed.children.size() == 1) {
            return validate_wrapper(typed.children[0], WrapperContext::ARRAY_ELEMENT);
        }
        return Status::Corruption("Invalid Parquet Variant typed_value schema {}", typed.name);
    };
    if (typed_value != nullptr) {
        RETURN_IF_ERROR(validate_typed_value(*typed_value));
    }
    return Status::OK();
}

static bool has_primitive_only_annotation(const tparquet::SchemaElement& schema) {
    if (schema.__isset.logicalType) {
        const auto& logical = schema.logicalType;
        if (logical.__isset.STRING || logical.__isset.ENUM || logical.__isset.DECIMAL ||
            logical.__isset.DATE || logical.__isset.TIME || logical.__isset.TIMESTAMP ||
            logical.__isset.INTEGER || logical.__isset.UNKNOWN || logical.__isset.JSON ||
            logical.__isset.BSON || logical.__isset.UUID || logical.__isset.FLOAT16 ||
            logical.__isset.GEOMETRY || logical.__isset.GEOGRAPHY) {
            return true;
        }
    }
    if (!schema.__isset.converted_type) {
        return false;
    }
    return schema.converted_type != tparquet::ConvertedType::MAP &&
           schema.converted_type != tparquet::ConvertedType::MAP_KEY_VALUE &&
           schema.converted_type != tparquet::ConvertedType::LIST;
}

static bool is_repeated_node(const tparquet::SchemaElement& schema) {
    return schema.__isset.repetition_type &&
           schema.repetition_type == tparquet::FieldRepetitionType::REPEATED;
}

static bool is_required_node(const tparquet::SchemaElement& schema) {
    return schema.__isset.repetition_type &&
           schema.repetition_type == tparquet::FieldRepetitionType::REQUIRED;
}

static bool is_optional_node(const tparquet::SchemaElement& schema) {
    return schema.__isset.repetition_type &&
           schema.repetition_type == tparquet::FieldRepetitionType::OPTIONAL;
}

static int num_children_node(const tparquet::SchemaElement& schema) {
    return schema.__isset.num_children ? schema.num_children : 0;
}

static Status validate_native_schema_structure(
        const std::vector<tparquet::SchemaElement>& schemas) {
    if (schemas.empty()) {
        return Status::InvalidArgument("Wrong parquet root schema element");
    }

    const auto& root = schemas[0];
    if (root.__isset.type || !root.__isset.num_children || root.num_children < 0 ||
        (root.__isset.repetition_type &&
         root.repetition_type != tparquet::FieldRepetitionType::REQUIRED)) {
        // Writers may encode the root's implicit REQUIRED repetition explicitly, and a zero-child
        // root is a valid metadata-only schema. Optional or repeated roots remain ambiguous.
        return Status::InvalidArgument("Wrong parquet root schema element");
    }

    struct PendingGroup {
        size_t remaining_children;
        size_t depth;
    };
    const auto root_children = num_children_node(schemas[0]);
    if (root_children < 0 || static_cast<size_t>(root_children) > schemas.size() - 1) {
        return Status::InvalidArgument("Invalid parquet root child count {}", root_children);
    }
    std::vector<PendingGroup> pending {{static_cast<size_t>(root_children), 0}};
    for (size_t pos = 1; pos < schemas.size(); ++pos) {
        while (!pending.empty() && pending.back().remaining_children == 0) {
            pending.pop_back();
        }
        if (pending.empty()) {
            return Status::InvalidArgument("Schema element {} is not reachable from the root", pos);
        }

        const size_t depth = pending.back().depth + 1;
        --pending.back().remaining_children;
        const auto& schema = schemas[pos];
        if (!schema.__isset.repetition_type) {
            return Status::InvalidArgument("Schema element {} has no repetition type", pos);
        }
        const bool has_children = schema.__isset.num_children && schema.num_children > 0;
        if (schema.__isset.type == has_children) {
            // Some legacy parquet-cpp files explicitly encode num_children=0 on primitive nodes.
            // Only a positive count denotes a group, preserving strict rejection of real dual-kind
            // nodes without dropping those otherwise valid files.
            return Status::InvalidArgument("Schema element {} has ambiguous primitive/group kind",
                                           pos);
        }
        if (schema.__isset.num_children && schema.num_children < 0) {
            return Status::InvalidArgument("Schema element {} has a negative child count", pos);
        }
        const int children = num_children_node(schemas[pos]);
        if (children < 0 || static_cast<size_t>(children) > schemas.size() - pos - 1) {
            return Status::InvalidArgument("Invalid child count {} at schema element {}", children,
                                           pos);
        }
        if (children > 0) {
            // Bound the tree before any resize or recursion so an untrusted footer cannot
            // turn a tiny schema into an unbounded allocation or parser stack.
            if (depth > MAX_NATIVE_SCHEMA_DEPTH) {
                return Status::InvalidArgument("Parquet schema depth {} exceeds limit {}", depth,
                                               MAX_NATIVE_SCHEMA_DEPTH);
            }
            pending.push_back({static_cast<size_t>(children), depth});
        }
    }
    while (!pending.empty() && pending.back().remaining_children == 0) {
        pending.pop_back();
    }
    if (!pending.empty()) {
        return Status::InvalidArgument("Parquet schema ended before all children were parsed");
    }
    return Status::OK();
}

/**
 * `repeated_parent_def_level` is the definition level of the first ancestor node whose repetition_type equals REPEATED.
 * Empty array/map values are not stored in doris columns, so have to use `repeated_parent_def_level` to skip the
 * empty or null values in ancestor node.
 *
 * For instance, considering an array of strings with 3 rows like the following:
 * null, [], [a, b, c]
 * We can store four elements in data column: null, a, b, c
 * and the offsets column is: 1, 1, 4
 * and the null map is: 1, 0, 0
 * For the i-th row in array column: range from `offsets[i - 1]` until `offsets[i]` represents the elements in this row,
 * so we can't store empty array/map values in doris data column.
 * As a comparison, spark does not require `repeated_parent_def_level`,
 * because the spark column stores empty array/map values , and use anther length column to indicate empty values.
 * Please reference: https://github.com/apache/spark/blob/master/sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/ParquetColumnVector.java
 *
 * Furthermore, we can also avoid store null array/map values in doris data column.
 * The same three rows as above, We can only store three elements in data column: a, b, c
 * and the offsets column is: 0, 0, 3
 * and the null map is: 1, 0, 0
 *
 * Inherit the repetition and definition level from parent node, if the parent node is repeated,
 * we should set repeated_parent_def_level = definition_level, otherwise as repeated_parent_def_level.
 * @param parent parent node
 * @param repeated_parent_def_level the first ancestor node whose repetition_type equals REPEATED
 */
static void set_child_node_level(NativeFieldSchema* parent, int16_t repeated_parent_def_level) {
    for (auto& child : parent->children) {
        child.repetition_level = parent->repetition_level;
        child.definition_level = parent->definition_level;
        child.repeated_parent_def_level = repeated_parent_def_level;
    }
}

static bool is_struct_list_node(const tparquet::SchemaElement& schema,
                                const std::string& enclosing_list_name) {
    // The legacy Parquet exception is exact: accepting every "*_tuple" wrapper changes a standard
    // one-child LIST wrapper from ARRAY<T> to ARRAY<STRUCT<T>>.
    return schema.name == "array" || schema.name == enclosing_list_name + "_tuple";
}

std::string NativeFieldSchema::debug_string() const {
    std::stringstream ss;
    ss << "NativeFieldSchema(name=" << name << ", R=" << repetition_level
       << ", D=" << definition_level;
    if (children.size() > 0) {
        ss << ", type=" << data_type->get_name() << ", children=[";
        for (int i = 0; i < children.size(); ++i) {
            if (i != 0) {
                ss << ", ";
            }
            ss << children[i].debug_string();
        }
        ss << "]";
    } else {
        ss << ", physical_type=" << physical_type;
        ss << " , doris_type=" << data_type->get_name();
    }
    ss << ")";
    return ss.str();
}

Status NativeFieldDescriptor::parse_from_thrift(
        const std::vector<tparquet::SchemaElement>& t_schemas) {
    _fields.clear();
    _physical_fields.clear();
    _name_to_field.clear();
    RETURN_IF_ERROR(validate_native_schema_structure(t_schemas));
    const auto& root_schema = t_schemas[0];
    _fields.resize(root_schema.num_children);
    _next_schema_pos = 1;

    for (int i = 0; i < root_schema.num_children; ++i) {
        RETURN_IF_ERROR(parse_node_field(t_schemas, _next_schema_pos, &_fields[i]));
        if (_name_to_field.find(_fields[i].name) != _name_to_field.end()) {
            return Status::InvalidArgument("Duplicated field name: {}", _fields[i].name);
        }
        _name_to_field.emplace(_fields[i].name, &_fields[i]);
    }

    if (_next_schema_pos != t_schemas.size()) {
        return Status::InvalidArgument("Remaining {} unparsed schema elements",
                                       t_schemas.size() - _next_schema_pos);
    }

    return Status::OK();
}

Status NativeFieldDescriptor::parse_node_field(
        const std::vector<tparquet::SchemaElement>& t_schemas, size_t curr_pos,
        NativeFieldSchema* node_field) {
    if (curr_pos >= t_schemas.size()) {
        return Status::InvalidArgument("Out-of-bounds index of schema elements");
    }
    auto& t_schema = t_schemas[curr_pos];
    if (is_group_node(t_schema)) {
        // nested structure or nullable list
        return parse_group_field(t_schemas, curr_pos, node_field);
    }
    if (is_variant_node(t_schema)) {
        return Status::InvalidArgument(
                "Parquet Variant logical type requires a group node, got primitive {}",
                t_schema.name);
    }
    if (is_repeated_node(t_schema)) {
        // repeated <primitive-type> <name> (LIST)
        // produce required list<element>
        node_field->repetition_level++;
        node_field->definition_level++;
        node_field->children.resize(1);
        set_child_node_level(node_field, node_field->definition_level);
        auto child = &node_field->children[0];
        parse_physical_field(t_schema, false, child);

        node_field->name = t_schema.name;
        node_field->lower_case_name = to_lower(t_schema.name);
        node_field->data_type = std::make_shared<DataTypeArray>(make_nullable(child->data_type));
        _next_schema_pos = curr_pos + 1;
        node_field->field_id = t_schema.__isset.field_id ? t_schema.field_id : -1;
    } else {
        bool is_optional = is_optional_node(t_schema);
        if (is_optional) {
            node_field->definition_level++;
        }
        parse_physical_field(t_schema, is_optional, node_field);
        _next_schema_pos = curr_pos + 1;
    }
    return Status::OK();
}

void NativeFieldDescriptor::parse_physical_field(const tparquet::SchemaElement& physical_schema,
                                                 bool is_nullable,
                                                 NativeFieldSchema* physical_field) {
    physical_field->name = physical_schema.name;
    physical_field->lower_case_name = to_lower(physical_field->name);
    physical_field->parquet_schema = physical_schema;
    physical_field->physical_type = physical_schema.type;
    physical_field->column_id = NATIVE_UNASSIGNED_COLUMN_ID; // Initialize column_id
    _physical_fields.push_back(physical_field);
    physical_field->physical_column_index = cast_set<int>(_physical_fields.size() - 1);
    std::pair<DataTypePtr, bool> type;
    try {
        type = get_doris_type(physical_schema, is_nullable);
    } catch (const Exception& e) {
        const bool is_decimal =
                (physical_schema.__isset.logicalType &&
                 physical_schema.logicalType.__isset.DECIMAL) ||
                (physical_schema.__isset.converted_type &&
                 physical_schema.converted_type == tparquet::ConvertedType::DECIMAL);
        // DECIMAL conversion failures carry precision/scale semantics that must not be erased by
        // a raw-byte fallback. Other unknown annotations intentionally retain the established
        // physical fallback, including legacy INTERVAL footers and forward-compatible types.
        if (is_decimal) {
            physical_field->unsupported_reason = e.what();
        }
        auto fallback_schema = physical_schema;
        fallback_schema.__isset.logicalType = false;
        fallback_schema.__isset.converted_type = false;
        type = get_doris_type(fallback_schema, is_nullable);
    }
    physical_field->data_type = type.first;
    physical_field->is_type_compatibility = type.second;
    physical_field->field_id = physical_schema.__isset.field_id ? physical_schema.field_id : -1;
}

std::pair<DataTypePtr, bool> NativeFieldDescriptor::get_doris_type(
        const tparquet::SchemaElement& physical_schema, bool nullable) {
    std::pair<DataTypePtr, bool> ans = {std::make_shared<DataTypeNothing>(), false};
    if (physical_schema.__isset.logicalType) {
        ans = convert_to_doris_type(physical_schema.logicalType, nullable);
    } else if (physical_schema.__isset.converted_type) {
        ans = convert_to_doris_type(physical_schema, nullable);
    }
    if (ans.first->get_primitive_type() == PrimitiveType::INVALID_TYPE) {
        switch (physical_schema.type) {
        case tparquet::Type::BOOLEAN:
            ans.first = DataTypeFactory::instance().create_data_type(TYPE_BOOLEAN, nullable);
            break;
        case tparquet::Type::INT32:
            ans.first = DataTypeFactory::instance().create_data_type(TYPE_INT, nullable);
            break;
        case tparquet::Type::INT64:
            ans.first = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, nullable);
            break;
        case tparquet::Type::INT96:
            if (_enable_mapping_timestamp_tz) {
                // treat INT96 as TIMESTAMPTZ
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_TIMESTAMPTZ, nullable,
                                                                         0, 6);
            } else {
                // in most cases, it's a nano timestamp
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, nullable,
                                                                         0, 6);
            }
            break;
        case tparquet::Type::FLOAT:
            ans.first = DataTypeFactory::instance().create_data_type(TYPE_FLOAT, nullable);
            break;
        case tparquet::Type::DOUBLE:
            ans.first = DataTypeFactory::instance().create_data_type(TYPE_DOUBLE, nullable);
            break;
        case tparquet::Type::BYTE_ARRAY:
            if (_enable_mapping_varbinary) {
                // if physical_schema not set logicalType and converted_type,
                // we treat BYTE_ARRAY as VARBINARY by default, so that we can read all data directly.
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_VARBINARY, nullable);
            } else {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_STRING, nullable);
            }
            break;
        case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
            ans.first = DataTypeFactory::instance().create_data_type(TYPE_STRING, nullable);
            break;
        default:
            throw Exception(Status::InternalError("Not supported parquet logicalType{}",
                                                  physical_schema.type));
            break;
        }
    }
    return ans;
}

std::pair<DataTypePtr, bool> NativeFieldDescriptor::convert_to_doris_type(
        tparquet::LogicalType logicalType, bool nullable) {
    std::pair<DataTypePtr, bool> ans = {std::make_shared<DataTypeNothing>(), false};
    bool& is_type_compatibility = ans.second;
    if (logicalType.__isset.STRING || logicalType.__isset.ENUM || logicalType.__isset.JSON ||
        logicalType.__isset.BSON) {
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_STRING, nullable);
    } else if (logicalType.__isset.DECIMAL) {
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_DECIMAL128I, nullable,
                                                                 logicalType.DECIMAL.precision,
                                                                 logicalType.DECIMAL.scale);
    } else if (logicalType.__isset.DATE) {
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_DATEV2, nullable);
    } else if (logicalType.__isset.INTEGER) {
        if (logicalType.INTEGER.isSigned) {
            if (logicalType.INTEGER.bitWidth <= 8) {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_TINYINT, nullable);
            } else if (logicalType.INTEGER.bitWidth <= 16) {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_SMALLINT, nullable);
            } else if (logicalType.INTEGER.bitWidth <= 32) {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_INT, nullable);
            } else {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, nullable);
            }
        } else {
            is_type_compatibility = true;
            if (logicalType.INTEGER.bitWidth <= 8) {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_SMALLINT, nullable);
            } else if (logicalType.INTEGER.bitWidth <= 16) {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_INT, nullable);
            } else if (logicalType.INTEGER.bitWidth <= 32) {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, nullable);
            } else {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_LARGEINT, nullable);
            }
        }
    } else if (logicalType.__isset.TIME) {
        const int scale = logicalType.TIME.unit.__isset.MILLIS ? 3 : 6;
        // TIME stores an integer unit, so its Doris scale must preserve the footer unit or
        // sub-second values are silently truncated by the target SerDe.
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_TIMEV2, nullable, 0, scale);
    } else if (logicalType.__isset.TIMESTAMP) {
        if (_enable_mapping_timestamp_tz) {
            if (logicalType.TIMESTAMP.isAdjustedToUTC) {
                // treat TIMESTAMP with isAdjustedToUTC as TIMESTAMPTZ
                ans.first = DataTypeFactory::instance().create_data_type(
                        TYPE_TIMESTAMPTZ, nullable, 0,
                        logicalType.TIMESTAMP.unit.__isset.MILLIS ? 3 : 6);
                return ans;
            }
        }
        ans.first = DataTypeFactory::instance().create_data_type(
                TYPE_DATETIMEV2, nullable, 0, logicalType.TIMESTAMP.unit.__isset.MILLIS ? 3 : 6);
    } else if (logicalType.__isset.UUID) {
        if (_enable_mapping_varbinary) {
            ans.first = DataTypeFactory::instance().create_data_type(TYPE_VARBINARY, nullable, -1,
                                                                     -1, 16);
        } else {
            ans.first = DataTypeFactory::instance().create_data_type(TYPE_STRING, nullable);
        }
    } else if (logicalType.__isset.FLOAT16) {
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_FLOAT, nullable);
    } else {
        throw Exception(Status::InternalError("Not supported parquet logicalType"));
    }
    return ans;
}

std::pair<DataTypePtr, bool> NativeFieldDescriptor::convert_to_doris_type(
        const tparquet::SchemaElement& physical_schema, bool nullable) {
    std::pair<DataTypePtr, bool> ans = {std::make_shared<DataTypeNothing>(), false};
    bool& is_type_compatibility = ans.second;
    switch (physical_schema.converted_type) {
    case tparquet::ConvertedType::type::UTF8:
    case tparquet::ConvertedType::type::ENUM:
    case tparquet::ConvertedType::type::JSON:
    case tparquet::ConvertedType::type::BSON:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_STRING, nullable);
        break;
    case tparquet::ConvertedType::type::DECIMAL:
        ans.first = DataTypeFactory::instance().create_data_type(
                TYPE_DECIMAL128I, nullable, physical_schema.precision, physical_schema.scale);
        break;
    case tparquet::ConvertedType::type::DATE:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_DATEV2, nullable);
        break;
    case tparquet::ConvertedType::type::TIME_MILLIS:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_TIMEV2, nullable, 0, 3);
        break;
    case tparquet::ConvertedType::type::TIME_MICROS:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_TIMEV2, nullable, 0, 6);
        break;
    case tparquet::ConvertedType::type::TIMESTAMP_MILLIS:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, nullable, 0, 3);
        break;
    case tparquet::ConvertedType::type::TIMESTAMP_MICROS:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, nullable, 0, 6);
        break;
    case tparquet::ConvertedType::type::INT_8:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_TINYINT, nullable);
        break;
    case tparquet::ConvertedType::type::UINT_8:
        is_type_compatibility = true;
        [[fallthrough]];
    case tparquet::ConvertedType::type::INT_16:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_SMALLINT, nullable);
        break;
    case tparquet::ConvertedType::type::UINT_16:
        is_type_compatibility = true;
        [[fallthrough]];
    case tparquet::ConvertedType::type::INT_32:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_INT, nullable);
        break;
    case tparquet::ConvertedType::type::UINT_32:
        is_type_compatibility = true;
        [[fallthrough]];
    case tparquet::ConvertedType::type::INT_64:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, nullable);
        break;
    case tparquet::ConvertedType::type::UINT_64:
        is_type_compatibility = true;
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_LARGEINT, nullable);
        break;
    default:
        throw Exception(Status::InternalError("Not supported parquet ConvertedType: {}",
                                              physical_schema.converted_type));
    }
    return ans;
}

Status NativeFieldDescriptor::parse_group_field(
        const std::vector<tparquet::SchemaElement>& t_schemas, size_t curr_pos,
        NativeFieldSchema* group_field) {
    auto& group_schema = t_schemas[curr_pos];
    group_field->parquet_schema = group_schema;
    if (is_variant_node(group_schema)) {
        if (is_repeated_node(group_schema)) {
            // A repeated annotated group needs an ARRAY carrier and Dremel-level remapping; treating
            // it as a scalar Variant would expose the wrong row shape.
            return Status::NotSupported("repeated Parquet Variant group {} is not supported",
                                        group_schema.name);
        }
        // UTC-adjusted timestamps inside Variant carry an instant, independent of the catalog's
        // presentation mapping. Parsing them as DATETIMEV2 would apply the session timezone and
        // lose that instant when the shredded value is re-encoded into ColumnVariantV2.
        {
            ScopedBoolOverride timestamp_tz_mapping(_enable_mapping_timestamp_tz, true);
            RETURN_IF_ERROR(parse_struct_field(t_schemas, curr_pos, group_field));
        }
        const auto& annotation = group_schema.logicalType.VARIANT;
        const auto specification_version =
                annotation.__isset.specification_version
                        ? std::optional<int8_t>(annotation.specification_version)
                        : std::nullopt;
        RETURN_IF_ERROR(validate_variant_layout(*group_field, specification_version));
        group_field->variant_physical_type = group_field->data_type;
        // Native page readers dispatch groups from data_type, so preserve the physical STRUCT
        // here. The public Parquet schema maps it to logical Variant without losing this shape.
        return Status::OK();
    }
    if ((group_schema.__isset.logicalType && group_schema.logicalType.__isset.ENUM) ||
        (group_schema.__isset.converted_type &&
         group_schema.converted_type == tparquet::ConvertedType::ENUM)) {
        // Preserve the established diagnostic used by compatibility tests and clients while the
        // generic branch below handles every other primitive-only group annotation.
        return Status::InvalidArgument("Logical type Enum cannot be applied to group node {}",
                                       group_schema.name);
    }
    if (has_primitive_only_annotation(group_schema)) {
        // Primitive annotations cannot change a group into a scalar. Reject them here instead of
        // silently interpreting malformed metadata as an ordinary STRUCT.
        return Status::InvalidArgument("Primitive annotation cannot be applied to group node {}",
                                       group_schema.name);
    }
    if (is_map_node(group_schema)) {
        // the map definition:
        // optional group <name> (MAP) {
        //   repeated group map (MAP_KEY_VALUE) {
        //     required <type> key;
        //     optional <type> value;
        //   }
        // }
        return parse_map_field(t_schemas, curr_pos, group_field);
    }
    if (is_list_node(group_schema)) {
        // the list definition:
        // optional group <name> (LIST) {
        //   repeated group [bag | list] { // hive or spark
        //     optional <type> [array_element | element]; // hive or spark
        //   }
        // }
        return parse_list_field(t_schemas, curr_pos, group_field);
    }

    if (is_repeated_node(group_schema)) {
        group_field->repetition_level++;
        group_field->definition_level++;
        group_field->children.resize(1);
        set_child_node_level(group_field, group_field->definition_level);
        auto struct_field = &group_field->children[0];
        // the list of struct:
        // repeated group <name> (LIST) {
        //   optional/required <type> <name>;
        //   ...
        // }
        // produce a non-null list<struct>
        RETURN_IF_ERROR(parse_struct_field(t_schemas, curr_pos, struct_field));

        group_field->name = group_schema.name;
        group_field->lower_case_name = to_lower(group_field->name);
        group_field->column_id = NATIVE_UNASSIGNED_COLUMN_ID; // Initialize column_id
        group_field->data_type =
                std::make_shared<DataTypeArray>(make_nullable(struct_field->data_type));
        group_field->field_id = group_schema.__isset.field_id ? group_schema.field_id : -1;
    } else {
        RETURN_IF_ERROR(parse_struct_field(t_schemas, curr_pos, group_field));
    }

    return Status::OK();
}

Status NativeFieldDescriptor::parse_list_field(
        const std::vector<tparquet::SchemaElement>& t_schemas, size_t curr_pos,
        NativeFieldSchema* list_field, bool repeated_node_is_enclosing_list_element) {
    // the list definition:
    // spark and hive have three level schemas but with different schema name
    // spark: <column-name> - "list" - "element"
    // hive: <column-name> - "bag" - "array_element"
    // parse three level schemas to two level primitive like: LIST<INT>,
    // or nested structure like: LIST<MAP<INT, INT>>
    auto& first_level = t_schemas[curr_pos];
    if (first_level.num_children != 1) {
        return Status::InvalidArgument("List element should have only one child");
    }

    if (curr_pos + 1 >= t_schemas.size()) {
        return Status::InvalidArgument("List element should have the second level schema");
    }

    if (first_level.repetition_type == tparquet::FieldRepetitionType::REPEATED &&
        !repeated_node_is_enclosing_list_element) {
        return Status::InvalidArgument("List element can't be a repeated schema");
    }

    // the repeated schema element
    auto& second_level = t_schemas[curr_pos + 1];
    if (second_level.repetition_type != tparquet::FieldRepetitionType::REPEATED) {
        return Status::InvalidArgument("The second level of list element should be repeated");
    }

    // This indicates if this list is nullable.
    bool is_optional = is_optional_node(first_level);
    if (is_optional) {
        list_field->definition_level++;
    }
    list_field->repetition_level++;
    list_field->definition_level++;
    list_field->children.resize(1);
    NativeFieldSchema* list_child = &list_field->children[0];

    size_t num_children = num_children_node(second_level);
    if (num_children > 0) {
        const bool structural_wrapper = is_struct_list_node(second_level, first_level.name);
        const auto& only_child = t_schemas[curr_pos + 2];
        const bool single_key_set_wrapper =
                second_level.__isset.converted_type &&
                second_level.converted_type == tparquet::ConvertedType::MAP_KEY_VALUE &&
                !is_repeated_node(only_child);
        const bool nested_collection_annotation =
                is_list_node(second_level) ||
                (is_map_node(second_level) && !single_key_set_wrapper);
        if (num_children == 1 && !structural_wrapper && nested_collection_annotation) {
            // The repeated node is already the outer LIST element. Preserve its own LIST/MAP
            // annotation. A MAP_KEY_VALUE node with a repeated child is the legacy uncontained
            // MAP shape; only its direct, non-repeated child denotes the enclosing SET key.
            set_child_node_level(list_field, list_field->definition_level);
            if (is_list_node(second_level)) {
                RETURN_IF_ERROR(parse_list_field(t_schemas, curr_pos + 1, list_child, true));
            } else if (is_map_node(second_level)) {
                RETURN_IF_ERROR(parse_map_field(t_schemas, curr_pos + 1, list_child, true));
            } else {
                RETURN_IF_ERROR(parse_struct_field(t_schemas, curr_pos + 1, list_child));
            }
        } else if (num_children == 1 && !structural_wrapper && !is_repeated_node(only_child)) {
            // optional field, and the third level element is the nested structure in list
            // produce nested structure like: LIST<INT>, LIST<MAP>, LIST<LIST<...>>
            // skip bag/list, it's a repeated element.
            set_child_node_level(list_field, list_field->definition_level);
            RETURN_IF_ERROR(parse_node_field(t_schemas, curr_pos + 2, list_child));
        } else {
            // required field, produce the list of struct
            set_child_node_level(list_field, list_field->definition_level);
            RETURN_IF_ERROR(parse_struct_field(t_schemas, curr_pos + 1, list_child));
        }
    } else if (num_children == 0) {
        // required two level list, for compatibility reason.
        set_child_node_level(list_field, list_field->definition_level);
        parse_physical_field(second_level, false, list_child);
        _next_schema_pos = curr_pos + 2;
    }

    list_field->name = first_level.name;
    list_field->lower_case_name = to_lower(first_level.name);
    list_field->column_id = NATIVE_UNASSIGNED_COLUMN_ID; // Initialize column_id
    list_field->data_type =
            std::make_shared<DataTypeArray>(make_nullable(list_field->children[0].data_type));
    if (is_optional) {
        list_field->data_type = make_nullable(list_field->data_type);
    }
    list_field->field_id = first_level.__isset.field_id ? first_level.field_id : -1;

    return Status::OK();
}

Status NativeFieldDescriptor::parse_map_field(const std::vector<tparquet::SchemaElement>& t_schemas,
                                              size_t curr_pos, NativeFieldSchema* map_field,
                                              bool repeated_node_is_enclosing_list_element) {
    // the map definition in parquet:
    // optional group <name> (MAP) {
    //   repeated group map (MAP_KEY_VALUE) {
    //     required <type> key;
    //     optional <type> value;
    //   }
    // }
    // Map value can be optional, the map without values is a SET
    if (curr_pos + 2 >= t_schemas.size()) {
        return Status::InvalidArgument("Map element should have at least three levels");
    }
    auto& map_schema = t_schemas[curr_pos];
    if (map_schema.num_children != 1) {
        return Status::InvalidArgument(
                "Map element should have only one child(name='map', type='MAP_KEY_VALUE')");
    }
    if (is_repeated_node(map_schema) && !repeated_node_is_enclosing_list_element) {
        return Status::InvalidArgument("Map element can't be a repeated schema");
    }
    auto& map_key_value = t_schemas[curr_pos + 1];
    if (!is_group_node(map_key_value) || !is_repeated_node(map_key_value)) {
        return Status::InvalidArgument(
                "the second level in map must be a repeated group(key and value)");
    }
    auto& map_key = t_schemas[curr_pos + 2];
    if (!is_required_node(map_key)) {
        LOG(WARNING) << "Filed " << map_schema.name << " is map type, but with nullable key column";
    }

    if (map_key_value.num_children == 1) {
        // The map with three levels is a SET
        return parse_list_field(t_schemas, curr_pos, map_field,
                                repeated_node_is_enclosing_list_element);
    }
    if (map_key_value.num_children != 2) {
        // A standard map should have four levels
        return Status::InvalidArgument(
                "the second level in map(MAP_KEY_VALUE) should have two children");
    }
    // standard map
    bool is_optional = is_optional_node(map_schema);
    if (is_optional) {
        map_field->definition_level++;
    }
    map_field->repetition_level++;
    map_field->definition_level++;

    // Directly create key and value children instead of intermediate key_value node
    map_field->children.resize(2);
    // map is a repeated node, we should set the `repeated_parent_def_level` of its children as `definition_level`
    set_child_node_level(map_field, map_field->definition_level);

    auto key_field = &map_field->children[0];
    auto value_field = &map_field->children[1];

    // Parse key and value fields directly from the key_value group's children
    _next_schema_pos = curr_pos + 2; // Skip key_value group, go directly to key
    RETURN_IF_ERROR(parse_node_field(t_schemas, _next_schema_pos, key_field));
    RETURN_IF_ERROR(parse_node_field(t_schemas, _next_schema_pos, value_field));

    map_field->name = map_schema.name;
    map_field->lower_case_name = to_lower(map_field->name);
    map_field->column_id = NATIVE_UNASSIGNED_COLUMN_ID; // Initialize column_id
    map_field->data_type = std::make_shared<DataTypeMap>(make_nullable(key_field->data_type),
                                                         make_nullable(value_field->data_type));
    if (is_optional) {
        map_field->data_type = make_nullable(map_field->data_type);
    }
    map_field->field_id = map_schema.__isset.field_id ? map_schema.field_id : -1;

    return Status::OK();
}

Status NativeFieldDescriptor::parse_struct_field(
        const std::vector<tparquet::SchemaElement>& t_schemas, size_t curr_pos,
        NativeFieldSchema* struct_field) {
    // the nested column in parquet, parse group to struct.
    auto& struct_schema = t_schemas[curr_pos];
    bool is_optional = is_optional_node(struct_schema);
    if (is_optional) {
        struct_field->definition_level++;
    }
    auto num_children = struct_schema.num_children;
    struct_field->children.resize(num_children);
    set_child_node_level(struct_field, struct_field->repeated_parent_def_level);
    _next_schema_pos = curr_pos + 1;
    for (int i = 0; i < num_children; ++i) {
        RETURN_IF_ERROR(parse_node_field(t_schemas, _next_schema_pos, &struct_field->children[i]));
    }
    struct_field->name = struct_schema.name;
    struct_field->lower_case_name = to_lower(struct_field->name);
    struct_field->column_id = NATIVE_UNASSIGNED_COLUMN_ID; // Initialize column_id

    struct_field->field_id = struct_schema.__isset.field_id ? struct_schema.field_id : -1;
    DataTypes res_data_types;
    std::vector<String> names;
    for (int i = 0; i < num_children; ++i) {
        res_data_types.push_back(make_nullable(struct_field->children[i].data_type));
        names.push_back(struct_field->children[i].name);
    }
    struct_field->data_type = std::make_shared<DataTypeStruct>(res_data_types, names);
    if (is_optional) {
        struct_field->data_type = make_nullable(struct_field->data_type);
    }
    return Status::OK();
}

int NativeFieldDescriptor::get_column_index(const std::string& column) const {
    for (int32_t i = 0; i < _fields.size(); i++) {
        if (_fields[i].name == column) {
            return i;
        }
    }
    return -1;
}

NativeFieldSchema* NativeFieldDescriptor::get_column(const std::string& name) const {
    auto it = _name_to_field.find(name);
    if (it != _name_to_field.end()) {
        return it->second;
    }
    throw Exception(Status::InternalError("Name {} not found in NativeFieldDescriptor!", name));
    return nullptr;
}

void NativeFieldDescriptor::get_column_names(std::unordered_set<std::string>* names) const {
    names->clear();
    for (const NativeFieldSchema& f : _fields) {
        names->emplace(f.name);
    }
}

std::string NativeFieldDescriptor::debug_string() const {
    std::stringstream ss;
    ss << "fields=[";
    for (int i = 0; i < _fields.size(); ++i) {
        if (i != 0) {
            ss << ", ";
        }
        ss << _fields[i].debug_string();
    }
    ss << "]";
    return ss.str();
}

void NativeFieldDescriptor::assign_ids() {
    uint64_t next_id = 1;
    for (auto& field : _fields) {
        field.assign_ids(next_id);
    }
}

const NativeFieldSchema* NativeFieldDescriptor::find_column_by_id(uint64_t column_id) const {
    for (const auto& field : _fields) {
        if (auto result = field.find_column_by_id(column_id)) {
            return result;
        }
    }
    return nullptr;
}

void NativeFieldSchema::assign_ids(uint64_t& next_id) {
    column_id = next_id++;

    for (auto& child : children) {
        child.assign_ids(next_id);
    }

    max_column_id = next_id - 1;
}

const NativeFieldSchema* NativeFieldSchema::find_column_by_id(uint64_t target_id) const {
    if (column_id == target_id) {
        return this;
    }

    for (const auto& child : children) {
        if (auto result = child.find_column_by_id(target_id)) {
            return result;
        }
    }

    return nullptr;
}

uint64_t NativeFieldSchema::get_column_id() const {
    return column_id;
}

void NativeFieldSchema::set_column_id(uint64_t id) {
    column_id = id;
}

uint64_t NativeFieldSchema::get_max_column_id() const {
    return max_column_id;
}

} // namespace doris::format::parquet
