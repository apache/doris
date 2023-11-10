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

#include <glog/logging.h>
#include <stddef.h>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <memory>
#include <string>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_string.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {
class Uuid : public IFunction {
public:
    static constexpr auto name = "uuid";
    static constexpr size_t uuid_length = 36; //uuid fixed length

    static FunctionPtr create() { return std::make_shared<Uuid>(); }

    String get_name() const override { return name; }

    bool use_default_implementation_for_constants() const override { return false; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto col_res = ColumnString::create();
        col_res->get_offsets().reserve(input_rows_count);
        col_res->get_chars().reserve(input_rows_count * uuid_length);

        boost::uuids::random_generator generator;
        for (int i = 0; i < input_rows_count; i++) {
            std::string uuid = boost::uuids::to_string(generator());
            DCHECK(uuid.length() == uuid_length);
            col_res->insert_data_without_reserve(uuid.c_str(), uuid.length());
        }

        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }
};

void register_function_uuid(SimpleFunctionFactory& factory) {
    factory.register_function<Uuid>();
}

} // namespace doris::vectorized
