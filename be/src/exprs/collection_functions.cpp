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

#include "exprs/collection_functions.h"
#include "runtime/collection_value.h"

namespace doris {

void CollectionFunctions::init() { }

CollectionVal CollectionFunctions::array(FunctionContext *context) {
    return CollectionVal(0, nullptr, nullptr);
}

CollectionVal CollectionFunctions::array(FunctionContext *context, int num_children, const IntVal *values) {
    CollectionValue v;
    TypeDescriptor td(TYPE_INT);
    v.init_collection(context, num_children, TYPE_INT, &v);
    for (int i = 0; i < num_children; ++i) {
        v.set(i, td, values + i);
    }
    
    CollectionVal ret;
    v.to_collection_val(&ret);
    return ret;
}


CollectionVal CollectionFunctions::array(FunctionContext* context, int num_children, const StringVal *values) {
    CollectionValue v;
    TypeDescriptor td(TYPE_VARCHAR);
    v.init_collection(context, num_children, TYPE_VARCHAR, &v);
    for (int i = 0; i < num_children; ++i) {
        v.set(i, td, values + i);
    }
    
    CollectionVal ret;
    v.to_collection_val(&ret);
    return ret;
}

}
