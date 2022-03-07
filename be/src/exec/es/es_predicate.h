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

#ifndef BE_EXEC_ES_PREDICATE_H
#define BE_EXEC_ES_PREDICATE_H

#include <string>
#include <vector>

#include "exprs/slot_ref.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Opcodes_types.h"
#include "gen_cpp/PaloExternalDataSourceService_types.h"
#include "runtime/descriptors.h"
#include "runtime/primitive_type.h"
#include "runtime/tuple.h"

namespace doris {

class Status;
class ExprContext;
struct ExtBinaryPredicate;
class EsPredicate;

class ExtLiteral {
public:
    ExtLiteral(PrimitiveType type, void* value) : _type(type), _value(value) {
        _str = value_to_string();
    }
    ~ExtLiteral();
    const std::string& to_string() const { return _str; }

private:
    int8_t get_byte();
    int16_t get_short();
    int32_t get_int();
    int64_t get_long();
    float get_float();
    double get_double();
    std::string get_string();
    std::string get_date_string();
    bool get_bool();
    std::string get_decimal_string();
    std::string get_decimalv2_string();
    std::string get_largeint_string();

    std::string value_to_string();

    PrimitiveType _type;
    void* _value;
    std::string _str;
};

struct ExtColumnDesc {
    ExtColumnDesc(const std::string& name, const TypeDescriptor& type) : name(name), type(type) {}

    std::string name;
    TypeDescriptor type;
};

struct ExtPredicate {
    ExtPredicate(TExprNodeType::type node_type) : node_type(node_type) {}
    virtual ~ExtPredicate() {}

    TExprNodeType::type node_type;
};

// this used for placeholder for compound_predicate
// reserved for compound_not
struct ExtCompPredicates : public ExtPredicate {
    ExtCompPredicates(TExprOpcode::type expr_op, const std::vector<EsPredicate*>& es_predicates)
            : ExtPredicate(TExprNodeType::COMPOUND_PRED), op(expr_op), conjuncts(es_predicates) {}

    TExprOpcode::type op;
    std::vector<EsPredicate*> conjuncts;
};

struct ExtBinaryPredicate : public ExtPredicate {
    ExtBinaryPredicate(TExprNodeType::type node_type, const std::string& name,
                       const TypeDescriptor& type, TExprOpcode::type op, const ExtLiteral& value)
            : ExtPredicate(node_type), col(name, type), op(op), value(value) {}

    ExtColumnDesc col;
    TExprOpcode::type op;
    ExtLiteral value;
};

struct ExtInPredicate : public ExtPredicate {
    ExtInPredicate(TExprNodeType::type node_type, bool is_not_in, const std::string& name,
                   const TypeDescriptor& type, const std::vector<ExtLiteral>& values)
            : ExtPredicate(node_type), is_not_in(is_not_in), col(name, type), values(values) {}

    bool is_not_in;
    ExtColumnDesc col;
    std::vector<ExtLiteral> values;
};

struct ExtLikePredicate : public ExtPredicate {
    ExtLikePredicate(TExprNodeType::type node_type, const std::string& name,
                     const TypeDescriptor& type, ExtLiteral value)
            : ExtPredicate(node_type), col(name, type), value(value) {}

    ExtColumnDesc col;
    ExtLiteral value;
};

struct ExtIsNullPredicate : public ExtPredicate {
    ExtIsNullPredicate(TExprNodeType::type node_type, const std::string& name,
                       const TypeDescriptor& type, bool is_not_null)
            : ExtPredicate(node_type), col(name, type), is_not_null(is_not_null) {}

    ExtColumnDesc col;
    bool is_not_null;
};

struct ExtFunction : public ExtPredicate {
    ExtFunction(TExprNodeType::type node_type, const std::string& func_name,
                std::vector<ExtColumnDesc> cols, std::vector<ExtLiteral> values)
            : ExtPredicate(node_type), func_name(func_name), cols(cols), values(values) {}

    const std::string func_name;
    std::vector<ExtColumnDesc> cols;
    const std::vector<ExtLiteral> values;
};

class EsPredicate {
public:
    EsPredicate(ExprContext* context, const TupleDescriptor* tuple_desc, ObjectPool* pool);
    ~EsPredicate();
    const std::vector<ExtPredicate*>& get_predicate_list() const;
    Status build_disjuncts_list();
    // public for tests
    EsPredicate(const std::vector<ExtPredicate*>& all_predicates) { _disjuncts = all_predicates; }

    Status get_es_query_status() { return _es_query_status; }

    void set_field_context(const std::map<std::string, std::string>& field_context) {
        _field_context = field_context;
    }

private:
    Status build_disjuncts_list(const Expr* conjunct);
    const SlotDescriptor* get_slot_desc(const SlotRef* slotRef);

    ExprContext* _context;
    const TupleDescriptor* _tuple_desc;
    std::vector<ExtPredicate*> _disjuncts;
    Status _es_query_status;
    ObjectPool* _pool;
    std::map<std::string, std::string> _field_context;
};

} // namespace doris

#endif
