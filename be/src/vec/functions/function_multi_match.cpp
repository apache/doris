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

#include "vec/functions/function_multi_match.h"

#include <glog/logging.h>

#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "io/fs/file_reader.h"
#include "olap/rowset/segment_v2/inverted_index/query/phrase_prefix_query.h"
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "vec/columns/column.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

Status FunctionMultiMatch::execute_impl(FunctionContext* /*context*/, Block& block,
                                        const ColumnNumbers& arguments, size_t result,
                                        size_t /*input_rows_count*/) const {
    return Status::RuntimeError("only inverted index queries are supported");
}

InvertedIndexQueryType get_query_type(const std::string& query_type) {
    if (query_type == "phrase_prefix") {
        return InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY;
    }
    return InvertedIndexQueryType::UNKNOWN_QUERY;
}

Status FunctionMultiMatch::evaluate_inverted_index(
        const ColumnsWithTypeAndName& arguments,
        const std::vector<vectorized::IndexFieldNameAndTypePair>& data_type_with_names,
        std::vector<segment_v2::InvertedIndexIterator*> iterators, uint32_t num_rows,
        segment_v2::InvertedIndexResultBitmap& bitmap_result) const {
    DCHECK(arguments.size() == 2);
    std::shared_ptr<roaring::Roaring> roaring = std::make_shared<roaring::Roaring>();
    std::shared_ptr<roaring::Roaring> null_bitmap = std::make_shared<roaring::Roaring>();
    // type
    auto query_type_value = arguments[0].column->get_data_at(0);
    auto query_type = get_query_type(query_type_value.to_string());
    if (query_type == InvertedIndexQueryType::UNKNOWN_QUERY) {
        return Status::RuntimeError(
                "parameter query type incorrect for function multi_match: query_type = {}",
                query_type);
    }

    // query
    auto query_str = arguments[1].column->get_data_at(0);
    auto param_type = arguments[1].type->get_type_as_type_descriptor().type;
    if (!is_string_type(param_type)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_INVALID_PARAMETERS>(
                "arguments for multi_match must be string");
    }
    // search
    for (int i = 0; i < data_type_with_names.size(); i++) {
        auto column_name = data_type_with_names[i].first;
        auto* iter = iterators[i];
        auto single_result = std::make_shared<roaring::Roaring>();
        std::shared_ptr<roaring::Roaring> index = std::make_shared<roaring::Roaring>();
        RETURN_IF_ERROR(iter->read_from_inverted_index(column_name, &query_str, query_type,
                                                       num_rows, index));
        *roaring |= *index;
    }
    segment_v2::InvertedIndexResultBitmap result(roaring, null_bitmap);
    bitmap_result = result;
    return Status::OK();
}

void register_function_multi_match(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMultiMatch>();
}

} // namespace doris::vectorized
