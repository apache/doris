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

#include "exprs/function/function.h"

namespace doris {
namespace io {
class FileSystem;
}

class FunctionToFile : public IFunction {
public:
    static constexpr auto name = "to_file";
    static FunctionPtr create() { return std::make_shared<FunctionToFile>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {0}; }
    bool use_default_implementation_for_constants() const override { return false; }
    bool use_default_implementation_for_nulls() const override { return false; }
    bool is_blockable() const override { return true; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override;
    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override;
    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override;
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override;

protected:
    virtual Status create_filesystem(const TFileResourceSnapshot& resource, const std::string& uri,
                                     std::shared_ptr<io::FileSystem>* filesystem) const;
};

} // namespace doris
