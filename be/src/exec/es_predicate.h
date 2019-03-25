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

#ifndef  BE_EXEC_ES_PREDICATE_H
#define  BE_EXEC_ES_PREDICATE_H

#include <string>
#include <vector>

#include "exprs/slot_ref.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Opcodes_types.h"
#include "gen_cpp/PaloExternalDataSourceService_types.h"
#include "runtime/descriptors.h"
#include "runtime/tuple.h"

namespace doris {

class Status;
class ExprContext;
class ExtBinaryPredicate;

struct ExtPredicate {
    ExtPredicate(TExprNodeType::type node_type) : node_type(node_type) {
    }

   //ExtBinaryPredicate* binary_predicate() {
   //    return dynamic_cast<ExtBinaryPredicate*>(this);
   //}

    TExprNodeType::type node_type;
};

struct ExtColumnDesc {
    ExtColumnDesc(std::string name, TypeDescriptor type) :
        name(name),
        type(type) {
    }

    std::string name;
    TypeDescriptor type;
};

struct ExtBinaryPredicate : public ExtPredicate {
    ExtBinaryPredicate(
                TExprNodeType::type node_type,
                std::string name, 
                TypeDescriptor type,
                TExprOpcode::type op,
                TExtLiteral value) :
        ExtPredicate(node_type),
        col(name, type),
        op(op),
        value(value) {
    }

    ExtColumnDesc col;
    TExprOpcode::type op;
    TExtLiteral value;
};

struct ExtInPredicate : public ExtPredicate {
    ExtInPredicate(
                TExprNodeType::type node_type,
                std::string name, 
                TypeDescriptor type,
                vector<TExtLiteral> values) :
        ExtPredicate(node_type),
        is_not_in(false),
        col(name, type),
        values(values) {
    }

    bool is_not_in;
    ExtColumnDesc col;
    vector<TExtLiteral> values;
};

struct ExtLikePredicate : public ExtPredicate {
    ExtColumnDesc col;
    TExtLiteral value;
};

struct ExtIsNullPredicate : public ExtPredicate {
    bool is_not_null;
    ExtColumnDesc col;
};

struct ExtFunction : public ExtPredicate {
    ExtFunction(
                TExprNodeType::type node_type,
                string func_name, 
                vector<ExtColumnDesc> cols,
                vector<TExtLiteral> values) :
        ExtPredicate(node_type),
        func_name(func_name),
        cols(cols),
        values(values) {
    }

    string func_name;
    vector<ExtColumnDesc> cols;
    vector<TExtLiteral> values;
};

class EsPredicate {

    public:
        EsPredicate(ExprContext* conjunct_ctx, 
                    const TupleDescriptor* tuple_desc);
        ~EsPredicate();
        vector<ExtPredicate> get_predicate_list();
        bool build_disjuncts();

    private:

        bool build_disjuncts(Expr* conjunct, vector<ExtPredicate>& disjuncts);
        bool to_ext_literal(ExprContext* context, Expr* expr, TExtLiteral* literal);
        bool is_match_func(Expr* conjunct);
        SlotDescriptor* get_slot_desc(SlotRef* slotRef);

        ExprContext* _context; 
        const TupleDescriptor* _tuple_desc;
        vector<ExtPredicate> _disjuncts;
        int _disjuncts_num;
};

}

#endif
