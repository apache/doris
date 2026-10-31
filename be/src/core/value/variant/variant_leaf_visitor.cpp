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

#include "core/value/variant/variant_leaf_visitor.h"

#include <fmt/compile.h>
#include <fmt/format.h>

#include <cmath>

#include "common/exception.h"
#include "core/types.h"
#include "core/value/variant/variant_value.h"

namespace doris {
namespace {

template <typename T>
std::string numeric_text(T value) {
    if (std::trunc(value) == value) {
        if (value >= -0x1p63 && value < 0x1p63) {
            return fmt::format(FMT_COMPILE("{}"), static_cast<int64_t>(value));
        }
        if (value >= 0x1p63 && value < 0x1p64) {
            return fmt::format(FMT_COMPILE("{}"), static_cast<uint64_t>(value));
        }
    }
    return fmt::format(FMT_COMPILE("{}"), value);
}

Status walk(const VariantRef& value, const std::function<Status(std::string_view)>& fn) {
    switch (value.basic_type()) {
    case VariantBasicType::OBJECT: {
        const auto object = value.object_view();
        for (uint32_t i = 0; i < object.size(); ++i) {
            RETURN_IF_ERROR(walk(object.value_at(i), fn));
        }
        return Status::OK();
    }
    case VariantBasicType::ARRAY:
        for (uint32_t i = 0; i < value.num_elements(); ++i) {
            RETURN_IF_ERROR(walk(value.array_at(i), fn));
        }
        return Status::OK();
    case VariantBasicType::SHORT_STRING:
        return fn(value.get_string().to_string_view());
    case VariantBasicType::PRIMITIVE:
        break;
    }
    switch (value.primitive_id()) {
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
        return fn(value.get_bool() ? "true" : "false");
    case VariantPrimitiveId::INT8:
    case VariantPrimitiveId::INT16:
    case VariantPrimitiveId::INT32:
    case VariantPrimitiveId::INT64:
        return fn(fmt::format(FMT_COMPILE("{}"), value.get_int()));
    case VariantPrimitiveId::FLOAT: {
        // Preserve float's shortest representation: widening 0.1f prints 0.10000000149011612.
        const float number = value.get_float();
        return std::isfinite(number) ? fn(numeric_text(number)) : Status::OK();
    }
    case VariantPrimitiveId::DOUBLE: {
        const double number = value.get_double();
        // NaN and infinities have no searchable canonical text.
        return std::isfinite(number) ? fn(numeric_text(number)) : Status::OK();
    }
    case VariantPrimitiveId::DECIMAL4:
    case VariantPrimitiveId::DECIMAL8:
    case VariantPrimitiveId::DECIMAL16: {
        const auto decimal = value.get_decimal();
        auto text = decimal.scale == 0 ? fmt::format(FMT_COMPILE("{}"), decimal.unscaled)
                                       : decimal_to_string(decimal.unscaled, decimal.scale);
        // Promotion must keep 42, 42.00 and 42.0 searchable under the same text.
        if (decimal.scale > 0) {
            text.erase(text.find_last_not_of('0') + 1);
            if (text.back() == '.') {
                text.pop_back();
            }
        }
        return fn(text);
    }
    case VariantPrimitiveId::STRING:
        return fn(value.get_string().to_string_view());
    default:
        return Status::OK(); // JSON null, temporal, binary and UUID.
    }
}

} // namespace

Status for_each_variant_leaf_text(const VariantRef& root,
                                  const std::function<Status(std::string_view)>& fn) {
    try {
        return walk(root, fn);
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}
} // namespace doris
