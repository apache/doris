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

#ifndef DORIS_BE_SRC_OLAP_LIKE_COLUMN_PREDICATE_H
#define DORIS_BE_SRC_OLAP_LIKE_COLUMN_PREDICATE_H

#include <stdint.h>

#include "olap/column_predicate.h"

namespace doris {

class VectorizedRowBatch;
struct LikePredicateState;

class LikeColumnPredicate : public ColumnPredicate {
public:
    LikeColumnPredicate(bool opposite, uint32_t column_id, doris_udf::FunctionContext* fn_ctx, StringValue*  val);
    ~LikeColumnPredicate() override = default;

    void evaluate(VectorizedRowBatch* batch) const override;

    void evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const override;

    void evaluate_or(ColumnBlock* block, uint16_t* sel, uint16_t size, bool* flags) const override {}
    void evaluate_and(ColumnBlock* block, uint16_t* sel, uint16_t size, bool* flags) const override {}
    Status evaluate(const Schema& schema,
                            const std::vector<BitmapIndexIterator*>& iterators, uint32_t num_rows,
                            Roaring* roaring) const override { return Status::OK(); }

private:
    std::string _origin;
    doris_udf::StringVal pattern;
    // life time controlled by scan node
    doris_udf::FunctionContext* _fn_ctx;
    LikePredicateState* _state;
};

} //namespace doris

#endif //DORIS_BE_SRC_OLAP_LIKE_COLUMN_PREDICATE_H
