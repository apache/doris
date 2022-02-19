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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_BINARY_PREDICATE_H
#define DORIS_BE_SRC_QUERY_EXPRS_BINARY_PREDICATE_H

#include <iostream>
#include <string>

#include "common/object_pool.h"
#include "exprs/predicate.h"
#include "gen_cpp/Exprs_types.h"

namespace doris {

class BinaryPredicate : public Predicate {
public:
    static Expr* from_thrift(const TExprNode& node);
    BinaryPredicate(const TExprNode& node) : Predicate(node) {}
    virtual ~BinaryPredicate() {}

protected:
    friend class Expr;

    // virtual Status prepare(RuntimeState* state, const RowDescriptor& desc);
    virtual std::string debug_string() const;
};

#define BIN_PRED_CLASS_DEFINE(CLASS)                                                      \
    class CLASS : public BinaryPredicate {                                                \
    public:                                                                               \
        CLASS(const TExprNode& node) : BinaryPredicate(node) {}                           \
        virtual ~CLASS() {}                                                               \
        virtual Expr* clone(ObjectPool* pool) const override {                            \
            return pool->add(new CLASS(*this));                                           \
        }                                                                                 \
                                                                                          \
        virtual BooleanVal get_boolean_val(ExprContext* context, TupleRow* row) override; \
    };

#define BIN_PRED_CLASSES_DEFINE(TYPE)     \
    BIN_PRED_CLASS_DEFINE(Eq##TYPE##Pred) \
    BIN_PRED_CLASS_DEFINE(Ne##TYPE##Pred) \
    BIN_PRED_CLASS_DEFINE(Lt##TYPE##Pred) \
    BIN_PRED_CLASS_DEFINE(Le##TYPE##Pred) \
    BIN_PRED_CLASS_DEFINE(Gt##TYPE##Pred) \
    BIN_PRED_CLASS_DEFINE(Ge##TYPE##Pred)

BIN_PRED_CLASSES_DEFINE(BooleanVal)
BIN_PRED_CLASSES_DEFINE(TinyIntVal)
BIN_PRED_CLASSES_DEFINE(SmallIntVal)
BIN_PRED_CLASSES_DEFINE(IntVal)
BIN_PRED_CLASSES_DEFINE(BigIntVal)
BIN_PRED_CLASSES_DEFINE(LargeIntVal)
BIN_PRED_CLASSES_DEFINE(FloatVal)
BIN_PRED_CLASSES_DEFINE(DoubleVal)
BIN_PRED_CLASSES_DEFINE(StringVal)
BIN_PRED_CLASSES_DEFINE(DateTimeVal)
BIN_PRED_CLASSES_DEFINE(DecimalV2Val)

#define BIN_PRED_FOR_NULL_CLASS_DEFINE(CLASS)                                             \
    class CLASS : public BinaryPredicate {                                                \
    public:                                                                               \
        CLASS(const TExprNode& node) : BinaryPredicate(node) {}                           \
        virtual ~CLASS() {}                                                               \
        virtual Expr* clone(ObjectPool* pool) const override {                            \
            return pool->add(new CLASS(*this));                                           \
        }                                                                                 \
                                                                                          \
        virtual BooleanVal get_boolean_val(ExprContext* context, TupleRow* row) override; \
    };

#define BIN_PRED_FOR_NULL_CLASSES_DEFINE(TYPE) BIN_PRED_FOR_NULL_CLASS_DEFINE(EqForNull##TYPE##Pred)

BIN_PRED_FOR_NULL_CLASSES_DEFINE(BooleanVal)
BIN_PRED_FOR_NULL_CLASSES_DEFINE(TinyIntVal)
BIN_PRED_FOR_NULL_CLASSES_DEFINE(SmallIntVal)
BIN_PRED_FOR_NULL_CLASSES_DEFINE(IntVal)
BIN_PRED_FOR_NULL_CLASSES_DEFINE(BigIntVal)
BIN_PRED_FOR_NULL_CLASSES_DEFINE(LargeIntVal)
BIN_PRED_FOR_NULL_CLASSES_DEFINE(FloatVal)
BIN_PRED_FOR_NULL_CLASSES_DEFINE(DoubleVal)
BIN_PRED_FOR_NULL_CLASSES_DEFINE(StringVal)
BIN_PRED_FOR_NULL_CLASSES_DEFINE(DateTimeVal)
BIN_PRED_FOR_NULL_CLASSES_DEFINE(DecimalV2Val)
} // namespace doris
#endif
