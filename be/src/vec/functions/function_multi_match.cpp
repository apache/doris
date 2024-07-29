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

#include <gen_cpp/PaloBrokerService_types.h>
#include <glog/logging.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "io/fs/file_reader.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/inverted_index/query/phrase_prefix_query.h"
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/varray_literal.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

Status FunctionMultiMatch::execute_impl(FunctionContext* /*context*/, Block& block,
                                        const ColumnNumbers& arguments, size_t result,
                                        size_t /*input_rows_count*/) const {
    return Status::RuntimeError("only inverted index queries are supported");
}

Status FunctionMultiMatch::open(FunctionContext* context,
                                FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }

    DCHECK(context->get_num_args() == 4);
    for (int i = 0; i < context->get_num_args(); ++i) {
        DCHECK(is_string_type(context->get_arg_type(i)->type));
    }

    std::shared_ptr<MatchParam> state = std::make_shared<MatchParam>();
    context->set_function_state(scope, state);
    for (int i = 0; i < context->get_num_args(); ++i) {
        const auto& const_column_ptr = context->get_constant_col(i);
        if (const_column_ptr) {
            auto const_data = const_column_ptr->column_ptr->get_data_at(0);
            switch (i) {
            case 1: {
                std::string field_names_str = const_data.to_string();
                field_names_str.erase(
                        std::remove_if(field_names_str.begin(), field_names_str.end(),
                                       [](unsigned char c) { return std::isspace(c); }),
                        field_names_str.end());
                std::vector<std::string> field_names;
                boost::split(field_names, field_names_str, boost::algorithm::is_any_of(","));
                state->fields.insert(field_names.begin(), field_names.end());
            } break;
            case 2:
                state->type = const_data.to_string();
                break;
            case 3:
                state->query = const_data.to_string();
                break;
            default:
                break;
            }
        }
    }

    return Status::OK();
}

Status FunctionMultiMatch::eval_inverted_index(FunctionContext* context,
                                               segment_v2::FuncExprParams& params) {
    auto* match_param = reinterpret_cast<MatchParam*>(
            context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (match_param == nullptr) {
        return Status::RuntimeError("function parameter parsing failed");
    }
    match_param->fields.insert(params._column_name);

    const auto& segment_iterator = params._segment_iterator;
    const auto& opts = segment_iterator->storage_read_options();
    const auto& tablet_schema = opts.tablet_schema;

    std::vector<ColumnId> columns_ids;

    for (const auto& column_name : match_param->fields) {
        auto cid = tablet_schema->field_index(column_name);
        if (cid < 0) {
            return Status::RuntimeError("column name is incorrect");
        }
        const auto& column = tablet_schema->column(cid);
        if (!is_string_type(column.type())) {
            return Status::RuntimeError("column type is incorrect");
        }
        if (!tablet_schema->has_inverted_index(column)) {
            return Status::RuntimeError("column index is incorrect");
        }
        columns_ids.emplace_back(cid);
    }

    // query type
    InvertedIndexQueryType query_type;
    if (match_param->type == "phrase_prefix") {
        query_type = InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY;
    } else {
        return Status::RuntimeError("query type is incorrect");
    }

    // cache key
    roaring::Roaring cids_str;
    cids_str.addMany(columns_ids.size(), columns_ids.data());
    cids_str.runOptimize();
    std::string column_name_binary(cids_str.getSizeInBytes(), 0);
    cids_str.write(column_name_binary.data());

    InvertedIndexQueryCache::CacheKey cache_key;
    io::Path index_path = segment_iterator->segment().file_reader()->path();
    cache_key.index_path = index_path.parent_path() / index_path.stem();
    cache_key.column_name = column_name_binary;
    cache_key.query_type = query_type;
    cache_key.value = match_param->query;

    // query cache
    auto* cache = InvertedIndexQueryCache::instance();
    InvertedIndexQueryCacheHandle cache_handler;
    if (cache->lookup(cache_key, &cache_handler)) {
        params.result = cache_handler.get_bitmap();
        return Status::OK();
    }

    // search
    bool first = true;
    for (const auto& column_name : match_param->fields) {
        auto cid = tablet_schema->field_index(column_name);

        auto& index_iterator = segment_iterator->inverted_index_iterators()[cid];
        if (!index_iterator) {
            RETURN_IF_ERROR(segment_iterator->_init_inverted_index_iterators(cid));
        }
        const auto& index_reader = index_iterator->reader();

        auto result = std::make_shared<roaring::Roaring>();
        RETURN_IF_ERROR(index_reader->query(opts.stats, opts.runtime_state, column_name,
                                            match_param->query.data(), query_type, result));
        if (first) {
            (*params.result).swap(*result);
            first = false;
        } else {
            (*params.result) |= (*result);
        }
    }

    params.result->runOptimize();
    cache->insert(cache_key, params.result, &cache_handler);

    return Status::OK();
}

void register_function_multi_match(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMultiMatch>();
}

} // namespace doris::vectorized
