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

#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_uuid.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/value/uuid_value.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "gtest/gtest.h"
#include "runtime/runtime_state.h"

namespace doris {

TEST(UuidBinaryCastTest, PreservesFileBytesAndNestedNulls) {
    for (bool strict : {false, true}) {
        RuntimeState state;
        for (bool nested : {false, true}) {
            const std::vector<std::string> bytes = {std::string(16, '\0'), std::string(16, '\xff'),
                                                    std::string("\x80", 1) + std::string(15, '\0'),
                                                    "abcDeFGhijkLmnOp"};
            auto values = ColumnUUID::create();
            auto nulls = ColumnUInt8::create();
            for (const auto& value : bytes) {
                values->insert_value(
                        UUIDValue::from_big_endian(reinterpret_cast<const uint8_t*>(value.data())));
                nulls->insert_value(0);
            }
            values->insert_default();
            nulls->insert_value(1);
            ColumnPtr source = ColumnNullable::create(std::move(values), std::move(nulls));
            DataTypePtr source_type = make_nullable(std::make_shared<DataTypeUUID>());
            DataTypePtr target_type = make_nullable(std::make_shared<DataTypeVarbinary>());
            if (nested) {
                auto offsets = ColumnArray::ColumnOffsets::create();
                offsets->insert_value(3);
                offsets->insert_value(3);
                offsets->insert_value(5);
                source = ColumnArray::create(source, std::move(offsets));
                source_type = std::make_shared<DataTypeArray>(source_type);
                target_type = std::make_shared<DataTypeArray>(target_type);
            }
            auto context = FunctionContext::create_context(&state, {}, {});
            context->set_enable_strict_mode(strict);
            Block block;
            block.insert({source, source_type, "uuid"});
            auto function = SimpleFunctionFactory::instance().get_function(
                    "CAST", {{source, source_type, "uuid"}}, target_type);
            ASSERT_NE(function, nullptr);
            const int result_id = block.columns();
            block.insert({nullptr, target_type, "binary"});
            auto status = function->execute(context.get(), block, {0}, result_id, source->size());
            ASSERT_TRUE(status.ok()) << status;
            ColumnPtr result = block.get_by_position(result_id).column;
            if (nested) {
                const auto& array = assert_cast<const ColumnArray&>(*result);
                EXPECT_EQ(array.get_offsets(),
                          assert_cast<const ColumnArray&>(*source).get_offsets());
                result = array.get_data_ptr();
            }
            const auto& nullable = assert_cast<const ColumnNullable&>(*result);
            EXPECT_EQ(nullable.size(), 5);
            for (size_t i = 0; i < bytes.size(); ++i) {
                EXPECT_FALSE(nullable.is_null_at(i));
                EXPECT_EQ(nullable.get_nested_column().get_data_at(i).to_string(), bytes[i]);
            }
            EXPECT_TRUE(nullable.is_null_at(4));
        }
    }
}

} // namespace doris
