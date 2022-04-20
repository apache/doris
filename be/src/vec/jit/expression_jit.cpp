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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/ExpressionJIT.cpp
// and modified by Doris

#ifdef DORIS_ENABLE_JIT
#include <optional>
#include <stack>

#include "vec/exprs/vliteral.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/typeid_cast.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/native.h"
#include "vec/functions/functions_comparison.h"
#include "vec/exprs/vectorized_fn_call.h"

#include "jit/jit.h"

#include "vec/jit/compile_dag.h"
#include "vec/jit/compile_function.h"

#include <chrono>


namespace doris::vectorized {

namespace ErrorCodes {
    extern const int LOGICAL_ERROR;
}

static JIT& get_jit_instance() {
    static JIT jit;
    return jit;
}

class CompiledFunctionHolder {
public:

    explicit CompiledFunctionHolder(CompiledFunction compiled_function_)
        : compiled_function(compiled_function_) {}

    CompiledFunction compiled_function;
};

class LLVMExecutableFunction : public PreparedFunctionImpl {
public:

    explicit LLVMExecutableFunction(const std::string& name_, std::shared_ptr<CompiledFunctionHolder> compiled_function_holder_)
        : name(name_)
        , compiled_function_holder(compiled_function_holder_) {
    }

    String get_name() const override { return name; }

    bool use_default_implementation_for_nulls() const override { return false; }

    bool use_default_implementation_for_constants() const override { return true; }

    virtual Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments, size_t result, size_t input_rows_count) override {

        auto result_column  = block.get_by_position(result).type->create_column();

        if (input_rows_count) {
            result_column = result_column->clone_resized(input_rows_count);

            std::vector<ColumnData> columns(arguments.size() + 1);
            std::vector<ColumnPtr> columns_backup;

            for (size_t i = 0; i < arguments.size(); ++i) {
                auto column = block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
                columns_backup.emplace_back(column);
                auto status = get_column_data(column.get(), columns[i]);
                if (!status.ok()) {
                    return status;
                }
            }

            auto status = get_column_data(result_column.get(), columns[arguments.size()]);
            if (!status.ok()) {
                return status;
            }

            auto jit_compiled_function = compiled_function_holder->compiled_function.compiled_function;
            jit_compiled_function(input_rows_count, columns.data());
            block.get_by_position(result).column = std::move(result_column);
        }

        return Status::OK();
    }

private:
    std::string name;
    std::shared_ptr<CompiledFunctionHolder> compiled_function_holder;
};

class LLVMFunction : public IFunctionBase {
public:

    explicit LLVMFunction(const CompileDAG& dag_)
        : name(dag_.dump())
        , dag(dag_) {
        for (size_t i = 0; i < dag.get_nodes_count(); ++i) {
            const auto& node = dag[i];

            if (node.type == CompileDAG::CompileType::FUNCTION)
                nested_functions.emplace_back(node.function);
            else if (node.type == CompileDAG::CompileType::INPUT)
                argument_types.emplace_back(node.result_type);
        }
    }

    void set_compiled_function(std::shared_ptr<CompiledFunctionHolder> compiled_function_holder_) {
        compiled_function_holder = compiled_function_holder_;
    }

    bool is_compilable() const override { return true; }

    Status compile(llvm::IRBuilderBase& builder, Values values, llvm::Value** result) const override {
        return dag.compile(builder, values, result);
    }

    String get_name() const override { return name; }

    const DataTypes& get_argument_types() const override { return argument_types; }

    const DataTypePtr& get_return_type() const override { return dag.back().result_type; }

    PreparedFunctionPtr prepare(FunctionContext* context, const Block& /*sample_block*/,
                                const ColumnNumbers& /*arguments*/,
                                size_t /*result*/) const override {
        if (!compiled_function_holder) {
            LOG(ERROR) << fmt::format("Compiled function was not initialized {}", name);
            return nullptr;
        }

        return std::make_unique<LLVMExecutableFunction>(name, compiled_function_holder);
    }


    bool is_deterministic() const override {
        for (const auto& f : nested_functions)
            if (!f->is_deterministic())
                return false;

        return true;
    }

    bool is_deterministic_in_scope_of_query() const override {
        for (const auto& f : nested_functions)
            if (!f->is_deterministic_in_scope_of_query())
                return false;

        return true;
    }

    bool is_suitable_for_constant_folding() const override {
        for (const auto& f : nested_functions)
            if (!f->is_suitable_for_constant_folding())
                return false;

        return true;
    }

    bool get_is_injective(const Block& sample_block) override {
        for (const auto& f : nested_functions)
            if (!f->get_is_injective(sample_block))
                return false;

        return true;
    }

    bool has_information_about_monotonicity() const override {
        for (const auto& f : nested_functions)
            if (!f->has_information_about_monotonicity())
                return false;

        return true;
    }

    Monotonicity get_monotonicity_for_range(const IDataType& type, const Field& left, const Field& right) const override {
        return {true, true, true};
    }

private:
    std::string name;
    CompileDAG dag;
    DataTypes argument_types;
    std::vector<FunctionBasePtr> nested_functions;
    std::shared_ptr<CompiledFunctionHolder> compiled_function_holder;
};

static bool is_compilable_constant(const VExpr& node) {
    return node.is_constant() && can_be_native_type(*node.data_type());
}

static bool is_compilable_function(const VExpr& node) {
    const auto* function_expr = dynamic_cast<const VectorizedFnCall*>(&node);
    if (!function_expr)
        return false;

    auto function = function_expr->get_function();

    if (!can_be_native_type(*function->get_return_type()))
        return false;

    for (const auto& type : function->get_argument_types()) {
        if (!can_be_native_type(*type))
            return false;
    }

    return function->is_compilable();
}

static CompileDAG get_compilable_dag(
    VExpr* root,
    std::vector<VExpr*>& children) {
    /// Extract CompileDAG from root actions dag node.

    CompileDAG dag;
    dag.set_root_expr(root);

    if (!is_compilable_function(*root))
        return dag;

    std::unordered_map<VExpr*, size_t> visited_node_to_compile_dag_position;

    struct Frame {
        VExpr* node;
        size_t next_child_to_visit = 0;
    };

    std::stack<Frame> stack;
    stack.emplace(Frame{.node = root});

    while (!stack.empty()) {
        auto& frame = stack.top();
        auto* node = frame.node;

        bool is_compilable_constant_ = is_compilable_constant(*node);
        bool is_compilable_function_ = is_compilable_function(*node);

        if (!is_compilable_function_ || is_compilable_constant_) {
            CompileDAG::Node compile_node;
            compile_node.function = nullptr;
            compile_node.result_type = node->data_type();

            if (is_compilable_constant_) {
                auto* literal = static_cast<const VLiteral*>(node);
                compile_node.type = CompileDAG::CompileType::CONSTANT;
                compile_node.column = literal->get_mutable_column();
            }
            else {
                compile_node.type = CompileDAG::CompileType::INPUT;
                children.emplace_back(node);
            }

            visited_node_to_compile_dag_position[node] = dag.get_nodes_count();
            dag.add_node(std::move(compile_node));
            stack.pop();
            continue;
        }

        while (frame.next_child_to_visit < node->children().size()) {
            const auto& child = node->children()[frame.next_child_to_visit];

            if (visited_node_to_compile_dag_position.find(child) != visited_node_to_compile_dag_position.cend()) {
                ++frame.next_child_to_visit;
                continue;
            }

            stack.emplace(Frame{.node = child});
            break;
        }

        bool all_children_visited = frame.next_child_to_visit == node->children().size();

        if (!all_children_visited)
            continue;

        /// Here we process only functions that are not compiled constants

        const auto function_expr = dynamic_cast<const VectorizedFnCall*>(node);
        CompileDAG::Node compile_node;
        compile_node.function = function_expr->get_function();
        compile_node.result_type = node->data_type();
        compile_node.type = CompileDAG::CompileType::FUNCTION;

        for (auto* child : node->children())
            compile_node.arguments.push_back(visited_node_to_compile_dag_position[child]);

        visited_node_to_compile_dag_position[node] = dag.get_nodes_count();

        dag.add_node(std::move(compile_node));
        stack.pop();
    }

    return dag;
}

Status VExpr::compile_jit(RuntimeState* runtime_state) {
    std::vector<CompileDAG> compile_dags;
    std::vector<FunctionBasePtr> functions_to_compile;

    for (auto* expr : runtime_state->get_exprs_to_compile()) {
        std::vector<VExpr*> arguments;
        auto dag = get_compilable_dag(expr, arguments);
        if (dag.get_input_nodes_count() == 0)
            continue;

        functions_to_compile.emplace_back(std::make_shared<LLVMFunction>(dag));
        dag.get_input_children().swap(arguments);
        compile_dags.emplace_back(std::move(dag));
    }

    DCHECK(compile_dags.size() == functions_to_compile.size());

    if (functions_to_compile.empty() && runtime_state->get_aggregate_functions_to_compile().empty())
        return Status::OK();

    std::vector<CompiledFunction> results;
    auto status = doris::vectorized::compile_functions(runtime_state->get_jit_instance(), functions_to_compile, results, runtime_state->get_aggregate_functions_to_compile());
    if (!status.ok())
        return status;
    
    for (size_t i = 0; i < functions_to_compile.size(); i++) {
        if (!results[i].compiled_function)
            continue;

        auto compiled_function_holder = std::make_shared<CompiledFunctionHolder>(results[i]);
        static_cast<LLVMFunction*>(functions_to_compile[i].get())->set_compiled_function(compiled_function_holder);

        auto* root_expr = compile_dags[i].root_expr();
        root_expr->_children.swap(compile_dags[i].get_input_children());
        root_expr->_compiled_function = functions_to_compile[i];
        root_expr->_is_function_compiled = true;
        root_expr->_prepared_compiled_function =  functions_to_compile[i]->prepare(nullptr, {}, {}, 0);
    }

    return Status::OK();
}

Status VExpr::compile_expressions(std::vector<VExpr*> exprs_to_compile) {
    if (exprs_to_compile.empty())
        return Status::OK();

    std::vector<CompileDAG> compile_dags;
    std::vector<FunctionBasePtr> functions_to_compile;

    for (auto* expr : exprs_to_compile) {
        std::vector<VExpr*> arguments;
        auto dag = get_compilable_dag(expr, arguments);
        if (dag.get_input_nodes_count() == 0)
            continue;

        functions_to_compile.emplace_back(std::make_shared<LLVMFunction>(dag));
        dag.get_input_children().swap(arguments);
        compile_dags.emplace_back(std::move(dag));
    }

    DCHECK(compile_dags.size() == functions_to_compile.size());
    if (compile_dags.empty())
        return Status::OK();

    std::vector<CompiledFunction> results;
    doris::vectorized::compile_functions(get_jit_instance(), functions_to_compile, results);

    DCHECK(functions_to_compile.size() == results.size());

    for (size_t i = 0; i < functions_to_compile.size(); i++) {
        if (!results[i].compiled_function)
            continue;

        auto compiled_function_holder = std::make_shared<CompiledFunctionHolder>(results[i]);
        static_cast<LLVMFunction*>(functions_to_compile[i].get())->set_compiled_function(compiled_function_holder);

        auto* root_expr = compile_dags[i].root_expr();
        root_expr->_children.swap(compile_dags[i].get_input_children());
        root_expr->_compiled_function = functions_to_compile[i];
        root_expr->_is_function_compiled = true;
        root_expr->_prepared_compiled_function =  functions_to_compile[i]->prepare(nullptr, {}, {}, 0);
    }

    return Status::OK();
}

}
#endif
