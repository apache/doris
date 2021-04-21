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

#include "exprs/conditional_functions.h"

#include "exprs/anyval_util.h"
#include "exprs/case_expr.h"
#include "exprs/expr.h"
#include "runtime/tuple_row.h"
#include "udf/udf.h"

namespace doris {

#define CTOR_DCTOR_FUN(expr_class)                                \
    expr_class::expr_class(const TExprNode& node) : Expr(node){}; \
                                                                  \
    expr_class::~expr_class(){};

CTOR_DCTOR_FUN(IfNullExpr);
CTOR_DCTOR_FUN(NullIfExpr);
CTOR_DCTOR_FUN(IfExpr);
CTOR_DCTOR_FUN(CoalesceExpr);
} // namespace doris
