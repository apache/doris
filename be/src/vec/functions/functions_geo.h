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
#include "udf/udf.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_geometry.h"
#include "vec/functions/function.h"

namespace doris {
class GeoShape;

namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

struct StConstructState {
    StConstructState() : is_null(false) {}
    ~StConstructState() {}

    bool is_null;
    std::string encoded_buf;
};

struct StContainsState {
    StContainsState() : is_null(false), shapes {nullptr, nullptr} {}
    ~StContainsState() {}
    bool is_null;
    std::vector<std::shared_ptr<GeoShape>> shapes;
};

template <typename Impl, typename ReturnType = DataTypeGeometry>
class GeoFunction : public IFunction {
public:
    static constexpr auto name = Impl::NAME;
    static FunctionPtr create() { return std::make_shared<GeoFunction<Impl, ReturnType>>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return Impl::NUM_ARGS; }
    bool is_variadic() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<ReturnType>());
    }
    bool use_default_implementation_for_nulls() const override { return true; }

    bool use_default_implementation_for_constants() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        if constexpr (Impl::NEED_CONTEXT) {
            return Impl::execute(context, block, arguments, result);
        } else {
            return Impl::execute(block, arguments, result);
        }
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if constexpr (Impl::NEED_CONTEXT) {
            return Impl::open(context, scope);
        } else {
            return Status::OK();
        }
    }

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if constexpr (Impl::NEED_CONTEXT) {
            return Impl::close(context, scope);
        } else {
            return Status::OK();
        }
    }
};

} // namespace doris::vectorized
