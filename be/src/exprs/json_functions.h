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

#ifndef BDG_PALO_BE_SRC_QUERY_EXPRS_JSON_FUNCTIONS_H
#define BDG_PALO_BE_SRC_QUERY_EXPRS_JSON_FUNCTIONS_H

#include <rapidjson/document.h>
#include "runtime/string_value.h"

namespace palo {

enum JsonFunctionType {
    JSON_FUN_INT = 0, JSON_FUN_DOUBLE, JSON_FUN_STRING
};

class Expr;
class OpcodeRegistry;
class TupleRow;

class JsonFunctions {
public:
    static void init();
    static palo_udf::IntVal get_json_int(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& json_str,
        const palo_udf::StringVal& path);
    static palo_udf::StringVal get_json_string(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& json_str,
        const palo_udf::StringVal& path);
    static palo_udf::DoubleVal get_json_double(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& json_str,
        const palo_udf::StringVal& path);

    static rapidjson::Value* get_json_object(
            const std::string& json_string, const std::string& path_string,
            const JsonFunctionType& fntype, rapidjson::Document* document);
};
}
#endif
