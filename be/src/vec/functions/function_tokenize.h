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

#include <stddef.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "udf/udf.h"
#include "vec/columns/column_array.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class FunctionTokenize : public IFunction {
public:
    static constexpr auto name = "tokenize";

    static FunctionPtr create() { return std::make_shared<FunctionTokenize>(); }
    using NullMapType = PaddedPODArray<UInt8>;

    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_string(arguments[0]))
                << "first argument for function: " << name << " should be string"
                << " and arguments[0] is " << arguments[0]->get_name();
        DCHECK(is_string(arguments[1]))
                << "second argument for function: " << name << " should be string"
                << " and arguments[1] is " << arguments[1]->get_name();
        return std::make_shared<DataTypeArray>(make_nullable(arguments[0]));
    }
    void _do_tokenize(const ColumnString& src_column_string, InvertedIndexCtx& inverted_index_ctx,
                      IColumn& dest_nested_column, ColumnArray::Offsets64& dest_offsets,
                      NullMapType* dest_nested_null_map) const;
    Status execute_impl(FunctionContext* /*context*/, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t /*input_rows_count*/) const override;

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        return Status::OK();
    }

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        return Status::OK();
    }
};

void register_function_tokenize(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionTokenize>();
}
} // namespace doris::vectorized
