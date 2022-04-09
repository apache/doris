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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/JIT/CompileDAG.h
// and modified by Doris

#ifdef DORIS_ENABLE_JIT
#pragma once

#include <vector>

#include "vec/data_types/data_type.h"
#include "vec/columns/column.h"
#include "vec/functions/function.h"

namespace llvm {
    class Value;
    class IRBuilderBase;
}

namespace doris::vectorized {

class VExpr;

/** For example we have expression (a + 1) + (b + 1) in actions dag.
  * It must be added into CompileDAG in order of compile evaluation.
  * Node a, Constant 1, Function add(a + 1), Input b, Constant 1, Function add(b, 1), Function add(add(a + 1), add(a + 1)).
  *
  * Compile function must be called with input_nodes_values equal to input nodes count.
  * When compile method is called added nodes are compiled in order.
  */
class CompileDAG {
public:

    enum class CompileType {
        INPUT = 0,
        CONSTANT = 1,
        FUNCTION = 2,
    };

    struct Node {
        CompileType type;
        DataTypePtr result_type;

        /// For CONSTANT
        ColumnPtr column;

        /// For FUNCTION
        FunctionBasePtr function;
        std::vector<size_t> arguments;
    };

    // static CompileDAG create_from_expression(VExpr* root);

    Status compile(llvm::IRBuilderBase& builder, Values input_nodes_values, llvm::Value** result) const;

    std::string dump() const;

    UInt128 hash() const;

    void add_node(Node node) {
        input_nodes_count += (node.type == CompileType::INPUT);
        nodes.emplace_back(std::move(node));
    }

    inline size_t get_nodes_count() const { return nodes.size(); }
    inline size_t get_input_nodes_count() const { return input_nodes_count; }

    inline Node & operator[](size_t index) { return nodes[index]; }
    inline const Node & operator[](size_t index) const { return nodes[index]; }

    inline Node & front() { return nodes.front(); }
    inline const Node & front() const { return nodes.front(); }

    inline Node & back() { return nodes.back(); }
    inline const Node & back() const { return nodes.back(); }

private:
    std::vector<Node> nodes;
    size_t input_nodes_count = 0;
};

}
#endif
