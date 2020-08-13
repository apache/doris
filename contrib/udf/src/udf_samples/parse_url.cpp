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

#include "udf_sample.h"
#include <string>
#include "exprs/anyval_util.h"
using namespace std;
namespace doris_udf {

StringVal ReplaceUdf(FunctionContext *context, const StringVal &origStr, const StringVal &oldStr, const StringVal &newStr) {
    if (origStr.is_null || oldStr.is_null || newStr.is_null) {
        return StringVal::null();
    }
    std::string orig_str = std::string(reinterpret_cast<const char *>(origStr.ptr), origStr.len);
    std::string old_str = std::string(reinterpret_cast<const char *>(oldStr.ptr), oldStr.len);
    std::string new_str = std::string(reinterpret_cast<const char *>(newStr.ptr), newStr.len);
    std::string::size_type pos = 0;
    std::string::size_type oldLen = old_str.size();
    std::string::size_type newLen = new_str.size();
    while(pos = orig_str.find(old_str, pos))
    {
        if(pos == std::string::npos) break;
        orig_str.replace(pos, oldLen, new_str);
        pos += newLen;
    }
    return AnyValUtil::from_string_temp(context, orig_str);
}


/// --- Prepare / Close Functions ---
/// ---------------------------------
void ParUrlUdfPrepare(FunctionContext* context, FunctionContext::FunctionStateScope scope){}
void ParseUrlUdfClose(FunctionContext* context, FunctionContext::FunctionStateScope scope) {}

}
