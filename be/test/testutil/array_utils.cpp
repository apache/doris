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

#include "testutil/array_utils.h"

#include "common/status.h"
#include "exprs/anyval_util.h"
#include "gen_cpp/olap_file.pb.h"
#include "runtime/collection_value.h"
#include "runtime/free_pool.hpp"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "udf/udf_internal.h"
#include "util/array_parser.h"

namespace doris {

using TypeDesc = FunctionContext::TypeDesc;

void ArrayUtils::prepare_context(FunctionContext& context, MemPool& mem_pool,
                                 const ColumnPB& column_pb) {
    auto function_type_desc = create_function_type_desc(column_pb);
    context.impl()->_return_type = function_type_desc;
    context.impl()->_pool = new FreePool(&mem_pool);
}

Status ArrayUtils::create_collection_value(CollectionValue* collection_value,
                                           FunctionContext* context,
                                           const std::string& json_string) {
    CollectionVal collection_val;
    auto status = ArrayParser::parse(collection_val, context, StringVal(json_string.c_str()));
    if (!status.ok()) {
        return status;
    }
    new (collection_value) CollectionValue(collection_val.data, collection_val.length,
                                           collection_val.has_null, collection_val.null_signs);
    return Status::OK();
}

TypeDesc ArrayUtils::create_function_type_desc(const ColumnPB& column_pb) {
    TypeDesc type_desc;
    type_desc.len = column_pb.length();
    type_desc.precision = column_pb.precision();
    type_desc.scale = column_pb.frac();
    if (column_pb.type() == "ARRAY") {
        type_desc.type = FunctionContext::TYPE_ARRAY;
    } else if (column_pb.type() == "INT") {
        type_desc.type = FunctionContext::TYPE_INT;
    } else if (column_pb.type() == "VARCHAR") {
        type_desc.type = FunctionContext::TYPE_VARCHAR;
    }
    for (const auto& sub_column_pb : column_pb.children_columns()) {
        type_desc.children.push_back(create_function_type_desc(sub_column_pb));
    }
    return type_desc;
}

} // namespace doris
