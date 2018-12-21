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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_COMPOUND_PREDICATE_H
#define DORIS_BE_SRC_QUERY_EXPRS_COMPOUND_PREDICATE_H

#include <string>

#include "common/object_pool.h"
#include "exprs/predicate.h"
#include "gen_cpp/Exprs_types.h"

namespace llvm {
class Function;
}

namespace doris {

class CompoundPredicate: public Predicate {
public:
    static void init();
    static BooleanVal compound_not(FunctionContext* context, const BooleanVal&);

protected:
    friend class Expr;

    CompoundPredicate(const TExprNode& node);

    Status codegen_compute_fn(bool and_fn, RuntimeState* state, llvm::Function** fn);
    // virtual Status prepare(RuntimeState* state, const RowDescriptor& desc);
    virtual std::string debug_string() const;

    virtual bool is_vectorized() const {
        return false;
    }

private:
    friend class OpcodeRegistry;
};

/// Expr for evaluating and (&&) operators
class AndPredicate: public CompoundPredicate {
public:
    virtual Expr* clone(ObjectPool* pool) const override { 
        return pool->add(new AndPredicate(*this));
    }
    virtual doris_udf::BooleanVal get_boolean_val(ExprContext* context, TupleRow*);

    virtual Status get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) {
        return CompoundPredicate::codegen_compute_fn(true, state, fn);
    }

protected:
    friend class Expr;
    AndPredicate(const TExprNode& node) : CompoundPredicate(node) { }

    virtual std::string debug_string() const {
        std::stringstream out;
        out << "AndPredicate(" << Expr::debug_string() << ")";
        return out.str();
    }

private:
    friend class OpcodeRegistry;
};

/// Expr for evaluating or (||) operators
class OrPredicate: public CompoundPredicate {
public:
    virtual Expr* clone(ObjectPool* pool) const override { 
        return pool->add(new OrPredicate(*this));
    }
    virtual doris_udf::BooleanVal get_boolean_val(ExprContext* context, TupleRow*);

    virtual Status get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) {
        return CompoundPredicate::codegen_compute_fn(false, state, fn);
    }

protected:
    friend class Expr;
    OrPredicate(const TExprNode& node) : CompoundPredicate(node) { }

    virtual std::string debug_string() const {
        std::stringstream out;
        out << "OrPredicate(" << Expr::debug_string() << ")";
        return out.str();
    }

private:
    friend class OpcodeRegistry;
};

/// Expr for evaluating or (||) operators
class NotPredicate: public CompoundPredicate {
public:
    virtual Expr* clone(ObjectPool* pool) const override { 
        return pool->add(new NotPredicate(*this));
    }
    virtual doris_udf::BooleanVal get_boolean_val(ExprContext* context, TupleRow*);

    virtual Status get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) {
        return get_codegend_compute_fn_wrapper(state, fn);
    }

protected:
    friend class Expr;
    NotPredicate(const TExprNode& node) : CompoundPredicate(node) { }

    virtual std::string debug_string() const {
        std::stringstream out;
        out << "NotPredicate(" << Expr::debug_string() << ")";
        return out.str();
    }

private:
    friend class OpcodeRegistry;
};
}

#endif
