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

#include "runtime/primitive_type.h"
#include "util/jsonb_document.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

void json_hash(const JsonbValue* jsonb_value, size_t& hash_value) {
    auto update_hash = [&hash_value](const void* data, size_t size) {
        hash_value = HashUtil::hash64(data, (uint32_t)size, hash_value);
    };

    if (jsonb_value->isObject()) {
        hash_value ^= (size_t)JsonbType::T_Object;
        const auto* obj_val = jsonb_value->unpack<ObjectVal>();
        const auto ordered = obj_val->get_ordered_key_value_pairs();
        for (const auto& [key, value] : ordered) {
            update_hash(key.data, key.size);
            json_hash(value, hash_value);
        }
    } else if (jsonb_value->isArray()) {
        hash_value ^= (size_t)JsonbType::T_Array;
        const auto* array_val = jsonb_value->unpack<ArrayVal>();
        for (auto it = array_val->begin(); it != array_val->end(); ++it) {
            json_hash(&*it, hash_value);
        }
    } else {
        // Similar to the code below
        // bool writeValue(const JsonbValue* value) {
        // ...
        //         os_->write((char*)value, value->numPackedBytes());
        // ...
        // }
        // The hash value of the whole structure is directly calculated here, and the Type of Jsonb is included.
        update_hash((const char*)jsonb_value, jsonb_value->numPackedBytes());
    }
}

class FunctionJsonHash : public IFunction {
public:
    static constexpr auto name = "json_hash";

    static FunctionPtr create() { return std::make_shared<FunctionJsonHash>(); }

    String get_name() const override { return name; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt64>();
    }

    size_t get_number_of_arguments() const override { return 1; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t size) const override {
        auto input_column = block.get_by_position(arguments[0]).column;
        auto to_column = ColumnInt64::create(size);
        auto& to_column_data = to_column->get_data();

        const auto& input_jsonb_column = assert_cast<const ColumnString&>(*input_column);

        for (size_t i = 0; i < size; ++i) {
            StringRef val = input_jsonb_column.get_data_at(i);
            JsonbDocument* doc = nullptr;
            auto st = JsonbDocument::checkAndCreateDocument(val.data, val.size, &doc);
            if (!st.ok() || !doc || !doc->getValue()) [[unlikely]] {
                // mayby be invalid jsonb, just insert default
                // invalid jsonb value may be caused by the default null processing
                continue;
            }

            size_t hash_value = 0;
            json_hash(doc->getValue(), hash_value);

            to_column_data[i] = static_cast<int64_t>(hash_value);
        }
        block.get_by_position(result).column = std::move(to_column);
        return Status::OK();
    }
};

void register_function_json_hash(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionJsonHash>();

    factory.register_alias(FunctionJsonHash::name, "jsonb_hash");
}

} // namespace doris::vectorized
