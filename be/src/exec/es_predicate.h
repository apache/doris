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
#include "runtime/primitive_type.h"

namespace doris {

class Status;
class ExprContext;
class ExtBinaryPredicate;
class ExtLiteral;

struct ExtPredicate {
    ExtPredicate(TExprNodeType::type node_type) : node_type(node_type) {
    }

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
                std::shared_ptr<ExtLiteral> value) :
        ExtPredicate(node_type),
        col(name, type),
        op(op),
        value(value) {
    }

    ExtColumnDesc col;
    TExprOpcode::type op;
    std::shared_ptr<ExtLiteral> value;
};

struct ExtInPredicate : public ExtPredicate {
    ExtInPredicate(
                TExprNodeType::type node_type,
                std::string name, 
                TypeDescriptor type,
                vector<std::shared_ptr<ExtLiteral>> values) :
        ExtPredicate(node_type),
        is_not_in(false),
        col(name, type),
        values(values) {
    }

    bool is_not_in;
    ExtColumnDesc col;
    vector<std::shared_ptr<ExtLiteral>> values;
};

struct ExtLikePredicate : public ExtPredicate {
    ExtColumnDesc col;
    std::shared_ptr<ExtLiteral> value;
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
                vector<std::shared_ptr<ExtLiteral>> values) :
        ExtPredicate(node_type),
        func_name(func_name),
        cols(cols),
        values(values) {
    }

    string func_name;
    vector<ExtColumnDesc> cols;
    vector<std::shared_ptr<ExtLiteral>> values;
};

class ExtLiteral {
    public:
        ExtLiteral(PrimitiveType type, void *value) : 
            _type(type),
            _value(value) {
        }
        ~ExtLiteral();

        int8_t to_byte();
        int16_t to_short();
        int32_t to_int();
        int64_t to_long();
        float to_float();
        double to_double();
        std::string to_string();
        std::string to_date_string();
        bool to_bool();
        std::string to_decimal_string();
        std::string to_decimalv2_string();
        std::string to_largeint_string();

        std::string value_to_string();

    private:

        PrimitiveType _type;
        void *_value;
};

class EsPredicate {

    public:
        EsPredicate(ExprContext* conjunct_ctx, 
                    const TupleDescriptor* tuple_desc);
        ~EsPredicate();
        vector<std::shared_ptr<ExtPredicate>> get_predicate_list();
        bool build_disjuncts_list();

    private:

        bool build_disjuncts_list(Expr* conjunct, 
                    vector<std::shared_ptr<ExtPredicate>>& disjuncts);
        bool is_match_func(Expr* conjunct);
        SlotDescriptor* get_slot_desc(SlotRef* slotRef);

        ExprContext* _context; 
        int _disjuncts_num;
        const TupleDescriptor* _tuple_desc;
        vector<std::shared_ptr<ExtPredicate>> _disjuncts;
};

}

#endif
