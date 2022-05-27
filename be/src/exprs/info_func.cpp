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

#include "exprs/info_func.h"

#include <sstream>

#include "util/debug_util.h"

namespace doris {

InfoFunc::InfoFunc(const TExprNode& node)
        : Expr(node), _int_value(node.info_func.int_value), _str_value(node.info_func.str_value) {}

StringVal InfoFunc::get_string_val(ExprContext* context, TupleRow*) {
    StringVal val;
    StringValue value(_str_value);
    value.to_string_val(&val);

    return val;
}

BigIntVal InfoFunc::get_big_int_val(ExprContext* context, TupleRow*) {
    return BigIntVal(_int_value);
}

std::string InfoFunc::debug_string() const {
    std::stringstream out;
    out << "InfoFunc(" << Expr::debug_string() << " int_value: " << _int_value
        << "; str_value: " << _str_value << ")";
    return out.str();
}

void* InfoFunc::compute_fn(Expr* e, TupleRow* row) {
    return nullptr;
}

} // namespace doris
