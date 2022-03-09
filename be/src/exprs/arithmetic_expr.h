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

#ifndef DORIS_BE_SRC_EXPRS_ARITHMETIC_EXPR_H
#define DORIS_BE_SRC_EXPRS_ARITHMETIC_EXPR_H

#include <set>

#include "common/object_pool.h"
#include "exprs/expr.h"

namespace doris {

class ArithmeticExpr : public Expr {
public:
    static bool is_valid(std::string fn_name) { return _s_valid_fn_names.count(fn_name); }
    static Expr* from_thrift(const TExprNode& node);
    static Expr* from_fn_name(const TExprNode& node);

protected:
    enum BinaryOpType {
        ADD,
        SUB,
        MUL,
        DIV,
        MOD,
        BIT_AND,
        BIT_OR,
        BIT_XOR,
        BIT_NOT,
    };

    ArithmeticExpr(const TExprNode& node) : Expr(node) {}
    virtual ~ArithmeticExpr() {}

    static std::set<std::string> _s_valid_fn_names;
};

class AddExpr : public ArithmeticExpr {
public:
    AddExpr(const TExprNode& node) : ArithmeticExpr(node) {}
    virtual ~AddExpr() {}
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new AddExpr(*this)); }
    virtual TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*) override;
    virtual SmallIntVal get_small_int_val(ExprContext* context, TupleRow*) override;
    virtual IntVal get_int_val(ExprContext* context, TupleRow*) override;
    virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow*) override;
    virtual LargeIntVal get_large_int_val(ExprContext* context, TupleRow*) override;
    virtual FloatVal get_float_val(ExprContext* context, TupleRow*) override;
    virtual DoubleVal get_double_val(ExprContext* context, TupleRow*) override;
    virtual DecimalV2Val get_decimalv2_val(ExprContext* context, TupleRow*) override;
};

class SubExpr : public ArithmeticExpr {
public:
    SubExpr(const TExprNode& node) : ArithmeticExpr(node) {}
    virtual ~SubExpr() {}
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new SubExpr(*this)); }
    virtual TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*) override;
    virtual SmallIntVal get_small_int_val(ExprContext* context, TupleRow*) override;
    virtual IntVal get_int_val(ExprContext* context, TupleRow*) override;
    virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow*) override;
    virtual LargeIntVal get_large_int_val(ExprContext* context, TupleRow*) override;
    virtual FloatVal get_float_val(ExprContext* context, TupleRow*) override;
    virtual DoubleVal get_double_val(ExprContext* context, TupleRow*) override;
    virtual DecimalV2Val get_decimalv2_val(ExprContext* context, TupleRow*) override;
};

class MulExpr : public ArithmeticExpr {
public:
    MulExpr(const TExprNode& node) : ArithmeticExpr(node) {}
    virtual ~MulExpr() {}
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new MulExpr(*this)); }
    virtual TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*) override;
    virtual SmallIntVal get_small_int_val(ExprContext* context, TupleRow*) override;
    virtual IntVal get_int_val(ExprContext* context, TupleRow*) override;
    virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow*) override;
    virtual LargeIntVal get_large_int_val(ExprContext* context, TupleRow*) override;
    virtual FloatVal get_float_val(ExprContext* context, TupleRow*) override;
    virtual DoubleVal get_double_val(ExprContext* context, TupleRow*) override;
    virtual DecimalV2Val get_decimalv2_val(ExprContext* context, TupleRow*) override;
};

class DivExpr : public ArithmeticExpr {
public:
    DivExpr(const TExprNode& node) : ArithmeticExpr(node) {}
    virtual ~DivExpr() {}
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new DivExpr(*this)); }
    virtual TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*) override;
    virtual SmallIntVal get_small_int_val(ExprContext* context, TupleRow*) override;
    virtual IntVal get_int_val(ExprContext* context, TupleRow*) override;
    virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow*) override;
    virtual LargeIntVal get_large_int_val(ExprContext* context, TupleRow*) override;
    virtual FloatVal get_float_val(ExprContext* context, TupleRow*) override;
    virtual DoubleVal get_double_val(ExprContext* context, TupleRow*) override;
    virtual DecimalV2Val get_decimalv2_val(ExprContext* context, TupleRow*) override;
};

class ModExpr : public ArithmeticExpr {
public:
    ModExpr(const TExprNode& node) : ArithmeticExpr(node) {}
    virtual ~ModExpr() {}
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new ModExpr(*this)); }
    virtual TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*) override;
    virtual SmallIntVal get_small_int_val(ExprContext* context, TupleRow*) override;
    virtual IntVal get_int_val(ExprContext* context, TupleRow*) override;
    virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow*) override;
    virtual LargeIntVal get_large_int_val(ExprContext* context, TupleRow*) override;
    virtual FloatVal get_float_val(ExprContext* context, TupleRow*) override;
    virtual DoubleVal get_double_val(ExprContext* context, TupleRow*) override;
    virtual DecimalV2Val get_decimalv2_val(ExprContext* context, TupleRow*) override;
};

class BitAndExpr : public ArithmeticExpr {
public:
    BitAndExpr(const TExprNode& node) : ArithmeticExpr(node) {}
    virtual ~BitAndExpr() {}
    virtual Expr* clone(ObjectPool* pool) const override {
        return pool->add(new BitAndExpr(*this));
    }
    virtual TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*) override;
    virtual SmallIntVal get_small_int_val(ExprContext* context, TupleRow*) override;
    virtual IntVal get_int_val(ExprContext* context, TupleRow*) override;
    virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow*) override;
    virtual LargeIntVal get_large_int_val(ExprContext* context, TupleRow*) override;
};

class BitOrExpr : public ArithmeticExpr {
public:
    BitOrExpr(const TExprNode& node) : ArithmeticExpr(node) {}
    virtual ~BitOrExpr() {}
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new BitOrExpr(*this)); }
    virtual TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*) override;
    virtual SmallIntVal get_small_int_val(ExprContext* context, TupleRow*) override;
    virtual IntVal get_int_val(ExprContext* context, TupleRow*) override;
    virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow*) override;
    virtual LargeIntVal get_large_int_val(ExprContext* context, TupleRow*) override;
};

class BitXorExpr : public ArithmeticExpr {
public:
    BitXorExpr(const TExprNode& node) : ArithmeticExpr(node) {}
    virtual ~BitXorExpr() {}
    virtual Expr* clone(ObjectPool* pool) const override {
        return pool->add(new BitXorExpr(*this));
    }
    virtual TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*) override;
    virtual SmallIntVal get_small_int_val(ExprContext* context, TupleRow*) override;
    virtual IntVal get_int_val(ExprContext* context, TupleRow*) override;
    virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow*) override;
    virtual LargeIntVal get_large_int_val(ExprContext* context, TupleRow*) override;
};

class BitNotExpr : public ArithmeticExpr {
public:
    BitNotExpr(const TExprNode& node) : ArithmeticExpr(node) {}
    virtual ~BitNotExpr() {}
    virtual Expr* clone(ObjectPool* pool) const override {
        return pool->add(new BitNotExpr(*this));
    }
    virtual TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*) override;
    virtual SmallIntVal get_small_int_val(ExprContext* context, TupleRow*) override;
    virtual IntVal get_int_val(ExprContext* context, TupleRow*) override;
    virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow*) override;
    virtual LargeIntVal get_large_int_val(ExprContext* context, TupleRow*) override;
};

} // namespace doris

#endif
