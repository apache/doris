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

#pragma once

#include <string>

#include "udf/udf.h"

namespace doris {

class ColumnPB;
class MemPool;
class Status;
struct CollectionValue;

class ArrayUtils {
public:
    using TypeDesc = FunctionContext::TypeDesc;
    static void prepare_context(FunctionContext& context, MemPool& mem_pool,
                                const ColumnPB& column_pb);
    static Status create_collection_value(CollectionValue* collection_value,
                                          FunctionContext* context, const std::string& json_string);

private:
    static TypeDesc create_function_type_desc(const ColumnPB& column_pb);
};

} // namespace doris
