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

#include "exprs/function/search/function_search_leaf.h"

#include <glog/logging.h>

#include <memory>
#include <roaring/roaring.hh>
#include <utility>
#include <vector>

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/field.h"
#include "storage/index/inverted/common/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/inverted/reader/text_index_reader.h"
#include "storage/olap_common.h"
#include "util/string_parser.hpp"

namespace doris {

namespace {

namespace logical = index_query::logical;
namespace query_v2 = segment_v2::inverted_index::query_v2;

// The terms a leaf opens, for the source to resolve ahead in one batch.
std::vector<std::string> leaf_terms(const logical::Node& leaf) {
    std::vector<std::string> terms;
    if (const auto* term = leaf.as<logical::Term>()) {
        terms.push_back(term->term);
    } else if (const auto* set = leaf.as<logical::TermSet>()) {
        terms = set->terms;
    } else if (const auto* phrase = leaf.as<logical::Phrase>()) {
        for (const auto& slot : phrase->slots) {
            if (slot.is_single_term()) {
                terms.push_back(slot.get_single_term());
            } else {
                const auto& alternatives = slot.get_multi_terms();
                terms.insert(terms.end(), alternatives.begin(), alternatives.end());
            }
        }
    }
    return terms;
}

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

LazyLeafCompiler::LazyLeafCompiler(std::wstring field, std::string binding_key,
                                   index_query::IndexSourcePtr source)
        : _field(std::move(field)),
          _binding_key(std::move(binding_key)),
          _source(std::move(source)) {}

Status LazyLeafCompiler::compile(const logical::Node& leaf, const SearchLeafContext& ctx,
                                 query_v2::QueryPtr* out) {
    const std::vector<std::string> terms = leaf_terms(leaf);
    if (!terms.empty()) {
        RETURN_IF_ERROR(_source->prepare_terms(terms));
    }
    return segment_v2::plan_query(leaf, ctx.context, _field, _binding_key, /*candidates=*/nullptr,
                                  out);
}

ScalarLeafCompiler::ScalarLeafCompiler(segment_v2::IndexIterator* iterator, DataTypePtr column_type,
                                       std::string stored_field_name)
        : _iterator(iterator),
          _column_type(std::move(column_type)),
          _stored_field_name(std::move(stored_field_name)) {}

Status ScalarLeafCompiler::compile(const logical::Node& leaf, const SearchLeafContext& ctx,
                                   query_v2::QueryPtr* out) {
    const auto* compare = leaf.as<logical::Compare>();
    DORIS_CHECK(compare != nullptr);
    DORIS_CHECK(_iterator != nullptr);
    DataTypePtr value_type = unwrap_value_type(_column_type);
    Field value;
    if (Status parsed = parse_scalar_value(value_type, compare->value, &value); !parsed.ok()) {
        LOG(INFO) << "search: scalar leaf value is unsupported, field=" << compare->field.name
                  << ", value='" << compare->value << "', reason=" << parsed.to_string();
        return parsed;
    }

    segment_v2::InvertedIndexParam param;
    param.column_name = _stored_field_name;
    param.column_type = value_type;
    param.query_value = value;
    param.query_type = segment_v2::InvertedIndexQueryType::EQUAL_QUERY;
    param.num_rows = ctx.num_rows;
    param.roaring = std::make_shared<roaring::Roaring>();
    RETURN_IF_ERROR(_iterator->read_from_index(segment_v2::IndexParam {&param}));

    // Physical NULL postings are excluded by the shared SEARCH leaf mask. Never
    // attach SQL UNKNOWN rows or mutate the reader-owned query cache bitmap here.
    *out = std::make_shared<query_v2::BitSetQuery>(std::move(param.roaring));
    return Status::OK();
}

} // namespace doris
