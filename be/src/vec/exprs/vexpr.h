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

#include <vector>

#include "common/status.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/types.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace vectorized {

class VExpr {
public:
    VExpr(const TExprNode& node);
    VExpr(const TypeDescriptor& type, bool is_slotref, bool is_nullable);
    virtual ~VExpr() = default;

    virtual VExpr* clone(ObjectPool* pool) const = 0;

    virtual const std::string& expr_name() const = 0;
    // VExpr(const VExpr& expr);
    virtual Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                           VExprContext* context);
    virtual void close(RuntimeState* state, VExprContext* context);
    virtual Status open(RuntimeState* state, VExprContext* context);
    virtual Status execute(vectorized::Block* block, int* result_column_id) = 0;

    DataTypePtr& data_type() { return _data_type; }

    TypeDescriptor type() { return _type; }

    void add_child(VExpr* expr) { _children.push_back(expr); }

    static Status create_expr_tree(ObjectPool* pool, const TExpr& texpr, VExprContext** ctx);

    static Status create_expr_trees(ObjectPool* pool, const std::vector<TExpr>& texprs,
                                    std::vector<VExprContext*>* ctxs);

    static Status prepare(const std::vector<VExprContext*>& ctxs, RuntimeState* state,
                          const RowDescriptor& row_desc,
                          const std::shared_ptr<MemTracker>& tracker);

    static Status open(const std::vector<VExprContext*>& ctxs, RuntimeState* state);

    static Status clone_if_not_exists(const std::vector<VExprContext*>& ctxs, RuntimeState* state,
                                      std::vector<VExprContext*>* new_ctxs);

    static void close(const std::vector<VExprContext*>& ctxs, RuntimeState* state);

    bool is_nullable() const { return _data_type->is_nullable(); }

    PrimitiveType result_type() const { return _type.type; }

    static Status create_expr(ObjectPool* pool, const TExprNode& texpr_node, VExpr** expr);

    static Status create_tree_from_thrift(ObjectPool* pool, const std::vector<TExprNode>& nodes,
                                          VExpr* parent, int* node_idx, VExpr** root_expr,
                                          VExprContext** ctx);
    const std::vector<VExpr*>& children() const { return _children; }
    virtual std::string debug_string() const;
    static std::string debug_string(const std::vector<VExpr*>& exprs);
    static std::string debug_string(const std::vector<VExprContext*>& ctxs);

protected:
    /// Simple debug string that provides no expr subclass-specific information
    std::string debug_string(const std::string& expr_name) const {
        std::stringstream out;
        out << expr_name << "(" << VExpr::debug_string() << ")";
        return out.str();
    }

    TExprNodeType::type _node_type;
    TypeDescriptor _type;
    DataTypePtr _data_type;
    std::vector<VExpr*> _children;
    TFunction _fn;
};

} // namespace vectorized
} // namespace doris
