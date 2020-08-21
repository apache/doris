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

#include "udf.h"

namespace doris_udf {

class OrthogonalBitmapFunctions {
public:
    static void init();
    static void bitmap_union_count_init(FunctionContext* ctx, StringVal* slot);
    static void bitmap_union(FunctionContext* ctx, const StringVal& src, StringVal* dst);
    static StringVal bitmap_serialize(FunctionContext* ctx, const StringVal& src);
    // bitmap_count_serialize
    static StringVal bitmap_count_serialize(FunctionContext* ctx, const StringVal& src);
    // count_merge 
    static void bitmap_count_merge(FunctionContext* context, const StringVal& src, StringVal* dst);
    // count_finalize
    static BigIntVal bitmap_count_finalize(FunctionContext* context, const StringVal& src);


     // intersect and intersect count
    template<typename T, typename ValType>
    static void bitmap_intersect_count_init(FunctionContext* ctx, StringVal* dst);
    template<typename T, typename ValType>
    static void bitmap_intersect_init(FunctionContext* ctx, StringVal* dst);

    template<typename T, typename ValType>
    static void bitmap_intersect_update(FunctionContext* ctx, const StringVal& src, const ValType& key,
                                        int num_key, const ValType* keys, const StringVal* dst);
    template<typename T>
    static void bitmap_intersect_merge(FunctionContext* ctx, const StringVal& src, const StringVal* dst);
    template<typename T>
    static StringVal bitmap_intersect_serialize(FunctionContext* ctx, const StringVal& src);
    template<typename T>
    static BigIntVal bitmap_intersect_finalize(FunctionContext* ctx, const StringVal& src);

    // bitmap_intersect_count_serialize
    template<typename T>
    static StringVal bitmap_intersect_count_serialize(FunctionContext* ctx, const StringVal& src);

    // bitmap_intersect_and_serialize
    template<typename T>
    static StringVal bitmap_intersect_and_serialize(FunctionContext* ctx, const StringVal& src);

};
}
