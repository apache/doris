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

#ifndef DORIS_BE_SRC_EXPRS_TOPN_FUNCTION_H
#define DORIS_BE_SRC_EXPRS_TOPN_FUNCTION_H

#include "udf/udf.h"

namespace doris {

class TopNFunctions {
public:
    static void init();

    static void topn_init(FunctionContext*, StringVal* dst);

    template <typename T>
    static void topn_update(FunctionContext*, const T& src, const IntVal& topn, StringVal* dst);

    template <typename T>
    static void topn_update(FunctionContext*, const T& src, const IntVal& topn, const IntVal& space_expand_rate,
            StringVal* dst);

    static void topn_merge(FunctionContext*,const StringVal& src, StringVal* dst);

    static StringVal topn_serialize(FunctionContext* ctx, const StringVal& src);

    static StringVal topn_finalize(FunctionContext*, const StringVal& src);
};

}

#endif //DORIS_BE_SRC_EXPRS_TOPN_FUNCTION_H
