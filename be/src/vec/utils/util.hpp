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

#include <thrift/protocol/TJSONProtocol.h>

#include <boost/shared_ptr.hpp>

#include "runtime/descriptors.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {
class VectorizedUtils {
public:
    static Block create_empty_columnswithtypename(const RowDescriptor& row_desc) {
        // Block block;
        return create_columns_with_type_and_name(row_desc);
    }

    static ColumnsWithTypeAndName create_columns_with_type_and_name(const RowDescriptor& row_desc) {
        ColumnsWithTypeAndName columns_with_type_and_name;
        for (const auto& tuple_desc : row_desc.tuple_descriptors()) {
            for (const auto& slot_desc : tuple_desc->slots()) {
                columns_with_type_and_name.emplace_back(nullptr, slot_desc->get_data_type_ptr(),
                                                        slot_desc->col_name());
            }
        }
        return columns_with_type_and_name;
    }

    static void update_null_map(NullMap& dst, const NullMap& src) {
        size_t size = dst.size();
        auto* __restrict l = dst.data();
        auto* __restrict r = src.data();
        for (size_t i = 0; i < size; ++i) {
            l[i] |= r[i];
        }
    }

    static DataTypes get_data_types(const RowDescriptor& row_desc) {
        DataTypes data_types;
        for (const auto& tuple_desc : row_desc.tuple_descriptors()) {
            for (const auto& slot_desc : tuple_desc->slots()) {
                data_types.push_back(slot_desc->get_data_type_ptr());
            }
        }
        return data_types;
    }

    static VExpr* dfs_peel_conjunct(RuntimeState* state, VExprContext* context, VExpr* expr,
                                    int& leaf_index, std::function<bool(int)> checker) {
        static constexpr auto is_leaf = [](VExpr* expr) { return !expr->is_and_expr(); };

        if (is_leaf(expr)) {
            if (checker(leaf_index++)) {
                expr->close(state, context, context->get_function_state_scope());
                return nullptr;
            }
            return expr;
        } else {
            VExpr* left_child =
                    dfs_peel_conjunct(state, context, expr->children()[0], leaf_index, checker);
            VExpr* right_child =
                    dfs_peel_conjunct(state, context, expr->children()[1], leaf_index, checker);

            if (left_child != nullptr && right_child != nullptr) {
                expr->set_children({left_child, right_child});
                return expr;
            } else {
                // here only close the and expr self, do not close the child
                expr->set_children({});
                expr->close(state, context, context->get_function_state_scope());
            }

            // here do not close Expr* now
            return left_child != nullptr ? left_child : right_child;
        }
    }
};

} // namespace doris::vectorized

namespace apache::thrift {
template <typename ThriftStruct>
ThriftStruct from_json_string(const std::string& json_val) {
    using namespace apache::thrift::transport;
    using namespace apache::thrift::protocol;
    ThriftStruct ts;
    TMemoryBuffer* buffer =
            new TMemoryBuffer((uint8_t*)json_val.c_str(), (uint32_t)json_val.size());
    std::shared_ptr<TTransport> trans(buffer);
    TJSONProtocol protocol(trans);
    ts.read(&protocol);
    return ts;
}

} // namespace apache::thrift
