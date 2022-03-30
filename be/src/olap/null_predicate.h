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

#ifndef DORIS_BE_SRC_OLAP_NULL_PREDICATE_H
#define DORIS_BE_SRC_OLAP_NULL_PREDICATE_H

#include <stdint.h>

#include <roaring/roaring.hh>

#include "olap/column_predicate.h"

namespace doris {

class VectorizedRowBatch;

class NullPredicate : public ColumnPredicate {
public:
    NullPredicate(uint32_t column_id, bool is_null, bool opposite = false);

    virtual PredicateType type() const override;

    virtual void evaluate(VectorizedRowBatch* batch) const override;

    void evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const override;

    void evaluate_or(ColumnBlock* block, uint16_t* sel, uint16_t size, bool* flags) const override;

    void evaluate_and(ColumnBlock* block, uint16_t* sel, uint16_t size, bool* flags) const override;

    virtual Status evaluate(const Schema& schema, const vector<BitmapIndexIterator*>& iterators,
                            uint32_t num_rows, roaring::Roaring* roaring) const override;

    void evaluate(vectorized::IColumn& column, uint16_t* sel, uint16_t* size) const override;

    void evaluate_or(vectorized::IColumn& column, uint16_t* sel, uint16_t size,
                     bool* flags) const override;

    void evaluate_and(vectorized::IColumn& column, uint16_t* sel, uint16_t size,
                      bool* flags) const override;

private:
    bool _is_null; //true for null, false for not null
};

} //namespace doris

#endif //DORIS_BE_SRC_OLAP_NULL_PREDICATE_H
