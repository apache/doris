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

#include "exprs/array_functions.h"
#include "runtime/array_value.h"

#include "common/logging.h"

namespace doris {

void ArrayFunctions::init() { }

#define ARRAY_FUNCTION(TYPE, PRIMARY_TYPE)  \
ArrayVal ArrayFunctions::array(FunctionContext *context, int num_children, const TYPE *values) {  \
    DCHECK_EQ(context->get_return_type().children.size(), 1);  \
    ArrayValue v;  \
    ArrayValue::init_array(context, num_children, PRIMARY_TYPE, &v);  \
    for (int i = 0; i < num_children; ++i) {  \
        v.set(i, PRIMARY_TYPE, values + i);  \
    }  \
    ArrayVal ret;  \
    v.to_array_val(&ret);  \
    return ret;  \
}

ARRAY_FUNCTION(IntVal, TYPE_INT);
ARRAY_FUNCTION(StringVal, TYPE_VARCHAR);

}
