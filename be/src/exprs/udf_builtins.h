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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_UDF_BUILTINS_H
#define DORIS_BE_SRC_QUERY_EXPRS_UDF_BUILTINS_H

#include "udf/udf.h"

namespace doris {

// Builtins written against the UDF interface. The builtins in the other files
// should be replaced to the UDF interface as well.
// This is just to illustrate how builtins against the UDF interface will be
// implemented.
class UdfBuiltins {
public:
    static doris_udf::DoubleVal abs(doris_udf::FunctionContext* context,
                                    const doris_udf::DoubleVal& v);
    static doris_udf::DecimalV2Val decimal_abs(doris_udf::FunctionContext* context,
                                               const doris_udf::DecimalV2Val& v);
    static doris_udf::BigIntVal add_two_number(doris_udf::FunctionContext* context,
                                               const doris_udf::BigIntVal& v1,
                                               const doris_udf::BigIntVal& v2);
    static doris_udf::StringVal sub_string(doris_udf::FunctionContext* context,
                                           const doris_udf::StringVal& v1,
                                           const doris_udf::IntVal& begin,
                                           const doris_udf::IntVal& len);
    static doris_udf::DoubleVal pi(doris_udf::FunctionContext* context);

    static doris_udf::StringVal lower(doris_udf::FunctionContext* context,
                                      const doris_udf::StringVal&);
};

} // namespace doris

#endif
