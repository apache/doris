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

#include <vector>

#include "runtime/primitive_type.h"
#include "util/jsonb_document.h"
#include "util/jsonb_document_cast.h"
#include "util/jsonb_writer.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

// Sort the keys of the JSON object and deduplicate the repeated keys, keeping the first one
void sort_json_object_keys(JsonbWriter& jsonb_writer, const JsonbValue* jsonb_value) {
    if (jsonb_value->isObject()) {
        std::vector<std::pair<StringRef, const JsonbValue*>> kvs;
        const auto* obj_val = jsonb_value->unpack<ObjectVal>();
        for (auto it = obj_val->begin(); it != obj_val->end(); ++it) {
            kvs.emplace_back(StringRef(it->getKeyStr(), it->klen()), it->value());
        }
        // sort by key
        std::sort(kvs.begin(), kvs.end(),
                  [](const auto& left, const auto& right) { return left.first < right.first; });
        // unique by key
        kvs.erase(std::unique(kvs.begin(), kvs.end(),
                              [](const auto& left, const auto& right) {
                                  return left.first == right.first;
                              }),
                  kvs.end());
        jsonb_writer.writeStartObject();
        for (const auto& kv : kvs) {
            jsonb_writer.writeKey(kv.first.data, static_cast<uint8_t>(kv.first.size));
            sort_json_object_keys(jsonb_writer, kv.second);
        }
        jsonb_writer.writeEndObject();
    } else if (jsonb_value->isArray()) {
        const auto* array_val = jsonb_value->unpack<ArrayVal>();
        jsonb_writer.writeStartArray();
        for (auto it = array_val->begin(); it != array_val->end(); ++it) {
            sort_json_object_keys(jsonb_writer, &*it);
        }
        jsonb_writer.writeEndArray();
    } else {
        // scalar value
        jsonb_writer.writeValue(jsonb_value);
    }
}

// Convert all numeric types in JSON to double type
void normalize_json_numbers_to_double(JsonbWriter& jsonb_writer, const JsonbValue* jsonb_value) {
    if (jsonb_value->isObject()) {
        jsonb_writer.writeStartObject();
        const auto* obj_val = jsonb_value->unpack<ObjectVal>();
        for (auto it = obj_val->begin(); it != obj_val->end(); ++it) {
            jsonb_writer.writeKey(it->getKeyStr(), it->klen());
            normalize_json_numbers_to_double(jsonb_writer, it->value());
        }
        jsonb_writer.writeEndObject();
    } else if (jsonb_value->isArray()) {
        const auto* array_val = jsonb_value->unpack<ArrayVal>();
        jsonb_writer.writeStartArray();
        for (auto it = array_val->begin(); it != array_val->end(); ++it) {
            normalize_json_numbers_to_double(jsonb_writer, &*it);
        }
        jsonb_writer.writeEndArray();
    } else {
        // scalar value
        if (jsonb_value->isInt() || jsonb_value->isFloat() || jsonb_value->isDouble() ||
            jsonb_value->isDecimal()) {
            double to;
            CastParameters params;
            params.is_strict = false;
            JsonbCast::cast_from_json_to_float(jsonb_value, to, params);
            NormalizeFloat(to);
            jsonb_writer.writeDouble(to);
        } else {
            jsonb_writer.writeValue(jsonb_value);
        }
    }
}

// Input jsonb, output jsonb
template <typename Impl>
class FunctionJsonbTransform : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionJsonbTransform>(); }

    String get_name() const override { return name; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeJsonb>();
    }

    size_t get_number_of_arguments() const override { return 1; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t size) const override {
        auto input_column = block.get_by_position(arguments[0]).column;
        auto to_column = ColumnString::create();

        const auto& input_jsonb_column = assert_cast<const ColumnString&>(*input_column);

        to_column->get_chars().reserve(input_jsonb_column.get_chars().size());
        to_column->get_offsets().reserve(input_jsonb_column.get_offsets().size());

        JsonbWriter writer;
        for (size_t i = 0; i < size; ++i) {
            StringRef val = input_jsonb_column.get_data_at(i);
            JsonbDocument* doc = nullptr;
            auto st = JsonbDocument::checkAndCreateDocument(val.data, val.size, &doc);
            if (!st.ok() || !doc || !doc->getValue()) [[unlikely]] {
                // mayby be invalid jsonb, just insert default
                // invalid jsonb value may be caused by the default null processing
                // insert empty string
                to_column->insert_default();
                continue;
            }
            JsonbValue* value = doc->getValue();
            if (UNLIKELY(!value)) {
                // mayby be invalid jsonb, just insert default
                // invalid jsonb value may be caused by the default null processing
                // insert empty string
                to_column->insert_default();
                continue;
            }

            writer.reset();

            Impl::transform(writer, value);

            to_column->insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
        }
        block.get_by_position(result).column = std::move(to_column);
        return Status::OK();
    }
};

struct SortJsonObjectKeys {
    static constexpr auto name = "sort_json_object_keys";
    static void transform(JsonbWriter& writer, const JsonbValue* value) {
        sort_json_object_keys(writer, value);
    }
};

struct NormalizeJsonNumbersToDouble {
    static constexpr auto name = "normalize_json_numbers_to_double";
    static void transform(JsonbWriter& writer, const JsonbValue* value) {
        normalize_json_numbers_to_double(writer, value);
    }
};

using FunctionSortJsonObjectKeys = FunctionJsonbTransform<SortJsonObjectKeys>;
using FunctionNormalizeJsonNumbersToDouble = FunctionJsonbTransform<NormalizeJsonNumbersToDouble>;

void register_function_json_transform(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionSortJsonObjectKeys>();
    factory.register_function<FunctionNormalizeJsonNumbersToDouble>();

    factory.register_alias(FunctionSortJsonObjectKeys::name, "sort_jsonb_object_keys");
    factory.register_alias(FunctionNormalizeJsonNumbersToDouble::name,
                           "normalize_jsonb_numbers_to_double");
}

} // namespace doris::vectorized
