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

#include "common/status.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/function.h"

namespace doris {
class FunctionContext;
namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
// FunctionFake is use for some function call expr only work at prepare/open phase, do not support execute().
template <typename Impl>
class FunctionFake : public IFunction {
public:
    static constexpr auto name = "fake";

    static FunctionPtr create() { return std::make_shared<FunctionFake>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return Impl::get_return_type_impl(arguments);
    }

    bool use_default_implementation_for_nulls() const override { return true; }

    bool use_default_implementation_for_constants() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        return Status::NotSupported(Impl::get_error_msg());
    }
};

struct UDTFImpl {
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        DCHECK(false) << "get_return_type_impl not supported";
        return nullptr;
    }
    static std::string get_error_msg() { return "Fake function do not support execute"; }
};

class FakeJavaUDTF : public FunctionFake<UDTFImpl> {
public:
    FakeJavaUDTF(const TFunction& fn, const DataTypes& argument_types,
                 const DataTypePtr& return_type)
            : _fn(fn), _argument_types(argument_types), _return_type(return_type) {}

    static FunctionPtr create(const TFunction& fn, const ColumnsWithTypeAndName& argument_types,
                              const DataTypePtr& return_type) {
        DataTypes data_types(argument_types.size());
        for (size_t i = 0; i < argument_types.size(); ++i) {
            data_types[i] = argument_types[i].type;
        }
        return std::make_shared<FakeJavaUDTF>(fn, data_types, return_type);
    }
    String get_name() const override { return _fn.name.function_name; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return _return_type;
    }

private:
    const TFunction& _fn;
    const DataTypes _argument_types;
    const DataTypePtr _return_type;
};

} // namespace doris::vectorized
