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

#include <stdint.h>

#include "olap/column_predicate.h"

namespace doris {

class VectorizedRowBatch;

#define COMPARISON_PRED_CLASS_DEFINE(CLASS, PT)                                                    \
    template <class T>                                                                             \
    class CLASS : public ColumnPredicate {                                                         \
    public:                                                                                        \
        CLASS(uint32_t column_id, const T& value, bool opposite = false);                          \
        PredicateType type() const override { return PredicateType::PT; }                          \
        virtual void evaluate(VectorizedRowBatch* batch) const override;                           \
        void evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const override;           \
        void evaluate_or(ColumnBlock* block, uint16_t* sel, uint16_t size,                         \
                         bool* flags) const override;                                              \
        void evaluate_and(ColumnBlock* block, uint16_t* sel, uint16_t size,                        \
                          bool* flags) const override;                                             \
        virtual Status evaluate(const Schema& schema,                                              \
                                const std::vector<BitmapIndexIterator*>& iterators,                \
                                uint32_t num_rows, roaring::Roaring* roaring) const override;      \
        void evaluate(vectorized::IColumn& column, uint16_t* sel, uint16_t* size) const override;  \
        void evaluate_and(vectorized::IColumn& column, uint16_t* sel, uint16_t size,               \
                          bool* flags) const override;                                             \
        void evaluate_or(vectorized::IColumn& column, uint16_t* sel, uint16_t size,                \
                         bool* flags) const override;                                              \
        void evaluate_vec(vectorized::IColumn& column, uint16_t size, bool* flags) const override; \
                                                                                                   \
    private:                                                                                       \
        T _value;                                                                                  \
    };

COMPARISON_PRED_CLASS_DEFINE(EqualPredicate, EQ)
COMPARISON_PRED_CLASS_DEFINE(NotEqualPredicate, NE)
COMPARISON_PRED_CLASS_DEFINE(LessPredicate, LT)
COMPARISON_PRED_CLASS_DEFINE(LessEqualPredicate, LE)
COMPARISON_PRED_CLASS_DEFINE(GreaterPredicate, GT)
COMPARISON_PRED_CLASS_DEFINE(GreaterEqualPredicate, GE)

} //namespace doris
