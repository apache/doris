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

#include "exprs/function/scalar_leaf_compiler.h"

#include <glog/logging.h>

#include <memory>
#include <roaring/roaring.hh>
#include <utility>

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/field.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/olap_common.h"
#include "util/string_parser.hpp"

namespace doris {

namespace {

namespace logical = index_query::logical;
namespace query_v2 = segment_v2::inverted_index::query_v2;

// The scalar type the index stores: nullable and array wrappers are stripped.
DataTypePtr unwrap_value_type(const DataTypePtr& column_type) {
    DataTypePtr value_type = remove_nullable(column_type);
    while (value_type != nullptr &&
           value_type->get_storage_field_type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        const auto* array_type = dynamic_cast<const DataTypeArray*>(value_type.get());
        if (array_type == nullptr) {
            return value_type;
        }
        value_type = remove_nullable(array_type->get_nested_type());
    }
    return value_type;
}

template <PrimitiveType primitive_type, typename CppType>
Status parse_integral(const std::string& value, Field* field) {
    StringParser::ParseResult parse_result = StringParser::PARSE_FAILURE;
    auto parsed = StringParser::string_to_int<CppType>(value.data(), value.size(), &parse_result);
    if (parse_result != StringParser::PARSE_SUCCESS) {
        return Status::InvalidArgument("failed to parse '{}' as {}", value,
                                       type_to_string(primitive_type));
    }
    *field = Field::create_field<primitive_type>(parsed);
    return Status::OK();
}

template <PrimitiveType primitive_type, typename CppType>
Status parse_floating(const std::string& value, Field* field) {
    StringParser::ParseResult parse_result = StringParser::PARSE_FAILURE;
    auto parsed = StringParser::string_to_float<CppType>(value.data(), value.size(), &parse_result);
    if (parse_result != StringParser::PARSE_SUCCESS) {
        return Status::InvalidArgument("failed to parse '{}' as {}", value,
                                       type_to_string(primitive_type));
    }
    *field = Field::create_field<primitive_type>(parsed);
    return Status::OK();
}

Status parse_scalar_value(const DataTypePtr& column_type, const std::string& value, Field* field) {
    if (column_type == nullptr || field == nullptr) {
        return Status::InvalidArgument("missing column type for scalar search value");
    }
    switch (column_type->get_storage_field_type()) {
    case FieldType::OLAP_FIELD_TYPE_BOOL: {
        StringParser::ParseResult parse_result = StringParser::PARSE_FAILURE;
        bool parsed = StringParser::string_to_bool(value.data(), value.size(), &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            return Status::InvalidArgument("failed to parse '{}' as bool", value);
        }
        *field = Field::create_field<TYPE_BOOLEAN>(parsed);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        return parse_integral<TYPE_TINYINT, Int8>(value, field);
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        return parse_integral<TYPE_SMALLINT, Int16>(value, field);
    case FieldType::OLAP_FIELD_TYPE_INT:
        return parse_integral<TYPE_INT, Int32>(value, field);
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        return parse_integral<TYPE_BIGINT, Int64>(value, field);
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        return parse_integral<TYPE_LARGEINT, Int128>(value, field);
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
        return parse_floating<TYPE_FLOAT, Float32>(value, field);
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
        return parse_floating<TYPE_DOUBLE, Float64>(value, field);
    default:
        return Status::NotSupported("scalar search does not support storage field type {}",
                                    static_cast<int>(column_type->get_storage_field_type()));
    }
}

} // namespace

ScalarLeafCompiler::ScalarLeafCompiler(segment_v2::IndexIterator* iterator, DataTypePtr column_type,
                                       std::string stored_field_name)
        : _iterator(iterator),
          _column_type(std::move(column_type)),
          _stored_field_name(std::move(stored_field_name)) {}

Status ScalarLeafCompiler::compile(const logical::Node& leaf, const SearchLeafContext& ctx,
                                   query_v2::QueryPtr* out) {
    const auto* compare = leaf.as<logical::Compare>();
    if (compare == nullptr || _iterator == nullptr) {
        *out = make_unknown_leaf_query(ctx.num_rows);
        return Status::OK();
    }
    DataTypePtr value_type = unwrap_value_type(_column_type);
    Field value;
    if (Status parsed = parse_scalar_value(value_type, compare->value, &value); !parsed.ok()) {
        LOG(INFO) << "search: scalar leaf value is unsupported, field=" << compare->field.name
                  << ", value='" << compare->value << "', reason=" << parsed.to_string();
        *out = make_unknown_leaf_query(ctx.num_rows);
        return Status::OK();
    }

    segment_v2::InvertedIndexParam param;
    param.column_name = _stored_field_name;
    param.column_type = value_type;
    param.query_value = value;
    param.query_type = segment_v2::InvertedIndexQueryType::EQUAL_QUERY;
    param.num_rows = ctx.num_rows;
    param.roaring = std::make_shared<roaring::Roaring>();
    RETURN_IF_ERROR(_iterator->read_from_index(segment_v2::IndexParam {&param}));

    auto null_bitmap = std::make_shared<roaring::Roaring>();
    auto has_null = _iterator->has_null();
    if (has_null.has_value() && has_null.value()) {
        segment_v2::InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
        RETURN_IF_ERROR(_iterator->read_null_bitmap(&null_bitmap_cache_handle));
        if (auto bitmap = null_bitmap_cache_handle.get_bitmap(); bitmap != nullptr) {
            null_bitmap = bitmap;
        }
    }
    *out = std::make_shared<query_v2::BitSetQuery>(std::move(param.roaring),
                                                   std::move(null_bitmap));
    return Status::OK();
}

} // namespace doris
