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

#include <boost/algorithm/string/split.hpp>

#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

class MatchParam {
public:
    std::string query;
    std::set<std::string> fields;
    std::string type;
};

class FunctionMultiMatch : public IFunction {
public:
    static constexpr auto name = "multi_match";

    static FunctionPtr create() { return std::make_shared<FunctionMultiMatch>(); }
    using NullMapType = PaddedPODArray<UInt8>;

    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 4; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeUInt8>();
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override;

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        return Status::OK();
    }

    Status execute_impl(FunctionContext* /*context*/, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t /*input_rows_count*/) const override;

    bool can_push_down_to_index() const override { return true; }

    Status eval_inverted_index(FunctionContext* context,
                               segment_v2::FuncExprParams& params) override;
};

} // namespace doris::vectorized
