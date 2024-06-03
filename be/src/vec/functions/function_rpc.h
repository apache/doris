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

#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <stddef.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "udf/udf.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/function.h"

namespace doris {
class PFunctionCallRequest;
class PFunctionService_Stub;
class PValues;
} // namespace doris

namespace doris::vectorized {

class RPCFnImpl {
public:
    RPCFnImpl(const TFunction& fn);
    ~RPCFnImpl() = default;
    Status vec_call(FunctionContext* context, vectorized::Block& block,
                    const std::vector<size_t>& arguments, size_t result, size_t input_rows_count);
    bool available() { return _client != nullptr; }

private:
    Status _convert_block_to_proto(vectorized::Block& block,
                                   const vectorized::ColumnNumbers& arguments,
                                   size_t input_rows_count, PFunctionCallRequest* request);
    void _convert_to_block(vectorized::Block& block, const PValues& result, size_t pos);

    std::shared_ptr<PFunctionService_Stub> _client;
    std::string _function_name;
    std::string _server_addr;
    std::string _signature;
    TFunction _fn;
};

class FunctionRPC : public IFunctionBase {
public:
    FunctionRPC(const TFunction& fn, const DataTypes& argument_types,
                const DataTypePtr& return_type);

    static FunctionBasePtr create(const TFunction& fn, const ColumnsWithTypeAndName& argument_types,
                                  const DataTypePtr& return_type) {
        DataTypes data_types(argument_types.size());
        for (size_t i = 0; i < argument_types.size(); ++i) {
            data_types[i] = argument_types[i].type;
        }
        return std::make_shared<FunctionRPC>(fn, data_types, return_type);
    }

    /// Get the main function name.
    String get_name() const override {
        return fmt::format("{}: [{}/{}]", _tfn.name.function_name, _tfn.hdfs_location,
                           _tfn.scalar_fn.symbol);
    }

    const DataTypes& get_argument_types() const override { return _argument_types; }
    const DataTypePtr& get_return_type() const override { return _return_type; }

    PreparedFunctionPtr prepare(FunctionContext* context, const Block& sample_block,
                                const ColumnNumbers& arguments, size_t result) const override {
        return nullptr;
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override;

    Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                   size_t result, size_t input_rows_count, bool dry_run = false) const override;

    bool is_use_default_implementation_for_constants() const override { return true; }

private:
    DataTypes _argument_types;
    DataTypePtr _return_type;
    TFunction _tfn;
};

} // namespace doris::vectorized
