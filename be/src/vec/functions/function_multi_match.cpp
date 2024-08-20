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

InvertedIndexQueryType get_query_type(const std::string& query_type) {
    if (query_type == "phrase_prefix") {
        return InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY;
    }
    return InvertedIndexQueryType::UNKNOWN_QUERY;
}

Status FunctionMultiMatch::eval_inverted_index(VExpr* expr, segment_v2::FuncExprParams& params,
                                               std::shared_ptr<roaring::Roaring>& result) {
    // fields
    std::vector<std::string> query_fileds;
    size_t i = 0;
    for (; i < expr->get_num_children(); i++) {
        auto child_expr = expr->get_child(i);
        if (child_expr->node_type() == TExprNodeType::type::SLOT_REF) {
            query_fileds.emplace_back(child_expr->expr_name());
        } else {
            break;
        }
    }
    if (i != expr->get_num_children() - 2) {
        return Status::RuntimeError("parameter type incorrect: slot = {}", i);
    }

    // type
    std::string param1 = std::static_pointer_cast<VLiteral>(expr->get_child(i))->value();
    auto query_type = get_query_type(param1);
    if (query_type == InvertedIndexQueryType::UNKNOWN_QUERY) {
        return Status::RuntimeError("parameter query type incorrect: query_type = {}", query_type);
    }

    // query
    std::string query_str = std::static_pointer_cast<VLiteral>(expr->get_child(i + 1))->value();

    auto& segment_iterator = params._segment_iterator;
    auto& segment = segment_iterator->segment();
    auto& opts = segment_iterator->storage_read_options();
    auto& tablet_schema = opts.tablet_schema;
    auto& idx_iterators = segment_iterator->inverted_index_iterators();

    // check
    std::vector<ColumnId> columns_ids;
    for (const auto& column_name : query_fileds) {
        auto cid = tablet_schema->field_index(column_name);
        if (cid < 0) {
            return Status::RuntimeError("column name is incorrect: {}", column_name);
        }
        if (idx_iterators[cid] == nullptr) {
            return Status::RuntimeError("column idx is incorrect: {}", column_name);
        }
        columns_ids.emplace_back(cid);
    }

    // cache key
    roaring::Roaring cids_str;
    cids_str.addMany(columns_ids.size(), columns_ids.data());
    cids_str.runOptimize();
    std::string column_name_binary(cids_str.getSizeInBytes(), 0);
    cids_str.write(column_name_binary.data());

    InvertedIndexQueryCache::CacheKey cache_key;
    io::Path index_path = segment.file_reader()->path();
    cache_key.index_path = index_path.parent_path() / index_path.stem();
    cache_key.column_name = column_name_binary;
    cache_key.query_type = query_type;
    cache_key.value = query_str;

    // query cache
    auto* cache = InvertedIndexQueryCache::instance();
    InvertedIndexQueryCacheHandle cache_handler;
    if (cache->lookup(cache_key, &cache_handler)) {
        result = cache_handler.get_bitmap();
        return Status::OK();
    }

    // search
    for (const auto& column_name : query_fileds) {
        auto cid = tablet_schema->field_index(column_name);
        const auto& column = *DORIS_TRY(tablet_schema->column(column_name));
        const auto& index_reader = idx_iterators[cid]->reader();

        auto single_result = std::make_shared<roaring::Roaring>();
        StringRef query_value(query_str.data());
        auto index_version = tablet_schema->get_inverted_index_storage_format();
        if (index_version == InvertedIndexStorageFormatPB::V1) {
            RETURN_IF_ERROR(index_reader->query(opts.stats, opts.runtime_state, column_name,
                                                &query_value, query_type, single_result));
        } else if (index_version == InvertedIndexStorageFormatPB::V2) {
            RETURN_IF_ERROR(index_reader->query(opts.stats, opts.runtime_state,
                                                std::to_string(column.unique_id()), &query_value,
                                                query_type, single_result));
        }
        (*result) |= (*single_result);
    }

    result->runOptimize();
    cache->insert(cache_key, result, &cache_handler);

    return Status::OK();
}

void register_function_multi_match(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMultiMatch>();
}

} // namespace doris::vectorized
