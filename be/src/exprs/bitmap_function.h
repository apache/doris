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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_BITMAP_FUNCTION_H
#define DORIS_BE_SRC_QUERY_EXPRS_BITMAP_FUNCTION_H

#include "udf/udf.h"

namespace doris {

class BitmapFunctions {
public:
    static void init();
    static void bitmap_init(FunctionContext* ctx, StringVal* slot);
    static StringVal bitmap_empty(FunctionContext* ctx);
    template <typename T>
    static void bitmap_update_int(FunctionContext* ctx, const T& src, StringVal* dst);
    // the input src's ptr need to point a RoaringBitmap, this function will release the
    // RoaringBitmap memory
    static BigIntVal bitmap_finalize(FunctionContext* ctx, const StringVal& src);

    static void bitmap_union(FunctionContext* ctx, const StringVal& src, StringVal* dst);
    static BigIntVal bitmap_count(FunctionContext* ctx, const StringVal& src);

    static StringVal bitmap_serialize(FunctionContext* ctx, const StringVal& src);
    static StringVal to_bitmap(FunctionContext* ctx, const StringVal& src);
};
}
#endif //DORIS_BE_SRC_QUERY_EXPRS_BITMAP_FUNCTION_H
