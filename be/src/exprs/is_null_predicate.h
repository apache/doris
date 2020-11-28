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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_IS_NULL_PREDICATE_H
#define DORIS_BE_SRC_QUERY_EXPRS_IS_NULL_PREDICATE_H

#include <string>

#include "exprs/predicate.h"

namespace doris {

class IsNullPredicate {
public:
    static void init();

    template <typename T>
    static BooleanVal is_null(FunctionContext* ctx, const T& val);

    template <typename T>
    static BooleanVal is_not_null(FunctionContext* ctx, const T& val);
};

} // namespace doris
#endif
