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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/JIT/CompileDAG.cpp
// and modified by Doris

#ifdef DORIS_ENABLE_JIT
#include "compile_dag.h"

#include <stack>

#include "vec/exprs/vliteral.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/common/sip_hash.h"
#include "vec/common/field_visitors.h"
#include "vec/columns/column_const.h"
#include "vec/data_types/native.h"
#include "vec/exprs/vexpr.h"

#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Value.h>

namespace doris::vectorized {

namespace ErrorCodes {
    extern const int LOGICAL_ERROR;
}

Status CompileDAG::compile(llvm::IRBuilderBase& builder, Values input_nodes_values, llvm::Value** result) const {
    assert(input_nodes_values.size() == get_input_nodes_count());

    llvm::IRBuilder<>& b = static_cast<llvm::IRBuilder<>&>(builder);

    PaddedPODArray<llvm::Value *> compiled_values;
    compiled_values.resize_fill(nodes.size());

    size_t input_nodes_values_index = 0;
    size_t compiled_values_index = 0;

    size_t dag_size = nodes.size();

    for (size_t i = 0; i < dag_size; ++i) {
        const auto & node = nodes[i];

        switch (node.type) {
            case CompileType::CONSTANT: {
                auto * native_value = get_column_native_value(b, node.result_type, *node.column, 0);
                if (!native_value)
                    return Status::RuntimeError(fmt::format("Cannot find native value for constant column with type {}", node.result_type->get_name()));

                compiled_values[compiled_values_index] = native_value;
                break;
            }
            case CompileType::FUNCTION: {
                Values temporary_values;
                temporary_values.reserve(node.arguments.size());

                for (auto argument_index : node.arguments) {
                    assert(compiled_values[argument_index] != nullptr);
                    temporary_values.emplace_back(compiled_values[argument_index]);
                }

                node.function->compile(builder, temporary_values, &compiled_values[compiled_values_index]);
                break;
            }
            case CompileType::INPUT: {
                compiled_values[compiled_values_index] = input_nodes_values[input_nodes_values_index];
                ++input_nodes_values_index;
                break;
            }
        }

        ++compiled_values_index;
    }

    *result = compiled_values.back();
    return Status::OK();
}

std::string CompileDAG::dump() const {
    std::vector<std::string> dumped_values;
    dumped_values.resize(nodes.size());

    size_t dag_size = nodes.size();
    for (size_t i = 0; i < dag_size; ++i) {
        const auto & node = nodes[i];

        switch (node.type) {
            case CompileType::CONSTANT: {
                const auto* column = typeid_cast<const ColumnConst*>(node.column.get());
                const auto& data = column->get_data_column();
                dumped_values[i] = apply_visitor(FieldVisitorToString(), data[0]) + " : " + node.result_type->get_name();
                break;
            }
            case CompileType::FUNCTION: {
                std::string function_dump = node.function->get_name();
                function_dump += '(';

                for (auto argument_index : node.arguments) {
                    function_dump += dumped_values[argument_index];
                    function_dump += ", ";
                }

                if (!node.arguments.empty()) {
                    function_dump.pop_back();
                    function_dump.pop_back();
                }

                function_dump += ')';

                dumped_values[i] = std::move(function_dump);
                break;
            }
            case CompileType::INPUT: {
                dumped_values[i] = node.result_type->get_name();
                break;
            }
        }
    }

    return dumped_values.back();
}

UInt128 CompileDAG::hash() const {
    SipHash hash;
    for (const auto & node : nodes) {
        hash.update(node.type);

        const auto & result_type_name = node.result_type->get_name();
        hash.update(result_type_name.size());
        hash.update(result_type_name);

        switch (node.type) {
            case CompileType::CONSTANT: {
                assert_cast<const ColumnConst *>(node.column.get())->get_data_column().update_hash_with_value(0, hash);
                break;
            }
            case CompileType::FUNCTION: {
                const auto & function_name = node.function->get_name();

                hash.update(function_name.size());
                hash.update(function_name);

                for (size_t arg : node.arguments)
                    hash.update(arg);

                break;
            }
            case CompileType::INPUT: {
                break;
            }
        }
    }

    UInt128 result;
    hash.get128((char *)&result);
    return result;
}

}
#endif
