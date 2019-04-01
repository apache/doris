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

struct ExtColumnDesc {
    ExtColumnDesc(const std::string& name, const TypeDescriptor& type) :
        name(name),
        type(type) {
    }

    std::string name;
    TypeDescriptor type;
};

struct ExtPredicate {
    ExtPredicate(TExprNodeType::type node_type) : node_type(node_type) {
    }

    TExprNodeType::type node_type;
};

struct ExtBinaryPredicate : public ExtPredicate {
    ExtBinaryPredicate(
                TExprNodeType::type node_type,
                const std::string& name, 
                const TypeDescriptor& type,
                TExprOpcode::type op,
                const ExtLiteral& value) :
        ExtPredicate(node_type),
        col(name, type),
        op(op),
        value(value) {
    }

    ExtColumnDesc col;
    TExprOpcode::type op;
    ExtLiteral value;
};

struct ExtInPredicate : public ExtPredicate {
    ExtInPredicate(
                TExprNodeType::type node_type,
                const std::string& name, 
                const TypeDescriptor& type,
                const std::vector<ExtLiteral>& values) :
        ExtPredicate(node_type),
        is_not_in(false),
        col(name, type),
        values(values) {
    }

    bool is_not_in;
    ExtColumnDesc col;
    std::vector<ExtLiteral> values;
};

struct ExtLikePredicate : public ExtPredicate {
    ExtLikePredicate(
                TExprNodeType::type node_type,
                const std::string& name, 
                const TypeDescriptor& type,
                ExtLiteral value) :
        ExtPredicate(node_type),
        col(name, type),
        value(value) {
    }

    ExtColumnDesc col;
    ExtLiteral value;
};

struct ExtIsNullPredicate : public ExtPredicate {
    ExtIsNullPredicate(
                TExprNodeType::type node_type,
                const std::string& name, 
                const TypeDescriptor& type,
                ExtLiteral value) :
        ExtPredicate(node_type),
        col(name, type),
        is_not_null(false) {
    }

    ExtColumnDesc col;
    bool is_not_null;
};

struct ExtFunction : public ExtPredicate {
    ExtFunction(TExprNodeType::type node_type,
                const std::string& func_name, 
                std::vector<ExtColumnDesc> cols,
                std::vector<ExtLiteral> values) :
        ExtPredicate(node_type),
        func_name(func_name),
        cols(cols),
        values(values) {
    }

    const std::string& func_name;
    std::vector<ExtColumnDesc> cols;
    std::vector<ExtLiteral> values;
};

class EsPredicate {

    public:
        EsPredicate(ExprContext* context, const TupleDescriptor* tuple_desc);
        ~EsPredicate();
        const std::vector<ExtPredicate*>& get_predicate_list();
        bool build_disjuncts_list();
        // public for tests
        EsPredicate(std::vector<ExtPredicate*>& all_predicates) {
            _disjuncts = all_predicates;
        };


    private:

        bool build_disjuncts_list(Expr* conjunct, 
                    std::vector<ExtPredicate*>& disjuncts);
        bool is_match_func(const Expr* conjunct);
        const SlotDescriptor* get_slot_desc(SlotRef* slotRef);

        ExprContext* _context; 
        int _disjuncts_num;
        const TupleDescriptor* _tuple_desc;
        std::vector<ExtPredicate*> _disjuncts;
};

}

#endif
