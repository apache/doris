// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#ifndef BDG_PALO_BE_SRC_QUERY_EXPRS_UDF_BUILTINS_H
#define BDG_PALO_BE_SRC_QUERY_EXPRS_UDF_BUILTINS_H

#include "udf/udf.h"

namespace palo {

// Builtins written against the UDF interface. The builtins in the other files
// should be replaced to the UDF interface as well.
// This is just to illustrate how builtins against the UDF interface will be
// implemented.
class UdfBuiltins {
public:
    static palo_udf::DoubleVal abs(palo_udf::FunctionContext* context, 
                                   const palo_udf::DoubleVal& v);
    static palo_udf::DecimalVal decimal_abs(palo_udf::FunctionContext* context, 
                                  const palo_udf::DecimalVal& v);
    static palo_udf::BigIntVal add_two_number(
            palo_udf::FunctionContext* context,
            const palo_udf::BigIntVal& v1,
            const palo_udf::BigIntVal& v2);
    static  palo_udf::StringVal sub_string(
            palo_udf::FunctionContext* context,
            const palo_udf::StringVal& v1,
            const palo_udf::IntVal& begin,
            const palo_udf::IntVal& len);
    static palo_udf::DoubleVal pi(palo_udf::FunctionContext* context);

    static palo_udf::StringVal lower(palo_udf::FunctionContext* context, 
                                     const palo_udf::StringVal&);
};

}

#endif
