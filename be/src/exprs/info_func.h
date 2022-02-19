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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_INFO_FUNC_H
#define DORIS_BE_SRC_QUERY_EXPRS_INFO_FUNC_H

#include <iostream>
#include <string>

#include "common/object_pool.h"
#include "exprs/expr.h"
#include "gen_cpp/Exprs_types.h"

namespace doris {

class InfoFunc : public Expr {
public:
    virtual ~InfoFunc() {}

    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new InfoFunc(*this)); }

protected:
    friend class Expr;

    InfoFunc(const TExprNode& node);

    virtual StringVal get_string_val(ExprContext* context, TupleRow*) override;
    virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow*) override;

    virtual std::string debug_string() const override;

private:
    static void* compute_fn(Expr* e, TupleRow* row);
    int64_t _int_value;
    std::string _str_value;
};

} // namespace doris

#endif
