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

#include "exprs/json_functions.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <re2/re2.h>
#include <stdlib.h>
#include <sys/time.h>

#include <boost/algorithm/string.hpp>
#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "common/logging.h"
#include "exprs/anyval_util.h"
#include "exprs/expr.h"
#include "gutil/strings/stringpiece.h"
#include "olap/olap_define.h"
#include "rapidjson/error/en.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "udf/udf.h"
#include "util/string_util.h"

namespace doris {

// static const re2::RE2 JSON_PATTERN("^([a-zA-Z0-9_\\-\\:\\s#\\|\\.]*)(?:\\[([0-9]+)\\])?");
// json path cannot contains: ", [, ]
static const re2::RE2 JSON_PATTERN("^([^\\\"\\[\\]]*)(?:\\[([0-9]+|\\*)\\])?");

void JsonFunctions::init() {}

IntVal JsonFunctions::get_json_int(FunctionContext* context, const StringVal& json_str,
                                   const StringVal& path) {
    if (json_str.is_null || path.is_null) {
        return IntVal::null();
    }
    std::string_view json_string((char*)json_str.ptr, json_str.len);
    std::string_view path_string((char*)path.ptr, path.len);
    rapidjson::Document document;
    rapidjson::Value* root =
            get_json_object(context, json_string, path_string, JSON_FUN_INT, &document);
    if (root != nullptr && root->IsInt()) {
        return IntVal(root->GetInt());
    } else {
        return IntVal::null();
    }
}

StringVal JsonFunctions::get_json_string(FunctionContext* context, const StringVal& json_str,
                                         const StringVal& path) {
    if (json_str.is_null || path.is_null) {
        return StringVal::null();
    }

    std::string_view json_string((char*)json_str.ptr, json_str.len);
    std::string_view path_string((char*)path.ptr, path.len);
    rapidjson::Document document;
    rapidjson::Value* root =
            get_json_object(context, json_string, path_string, JSON_FUN_STRING, &document);
    if (root == nullptr || root->IsNull()) {
        return StringVal::null();
    } else if (root->IsString()) {
        return AnyValUtil::from_string_temp(context, root->GetString());
    } else {
        rapidjson::StringBuffer buf;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
        root->Accept(writer);
        return AnyValUtil::from_string_temp(context, std::string(buf.GetString()));
    }
}

DoubleVal JsonFunctions::get_json_double(FunctionContext* context, const StringVal& json_str,
                                         const StringVal& path) {
    if (json_str.is_null || path.is_null) {
        return DoubleVal::null();
    }
    std::string_view json_string((char*)json_str.ptr, json_str.len);
    std::string_view path_string((char*)path.ptr, path.len);
    rapidjson::Document document;
    rapidjson::Value* root =
            get_json_object(context, json_string, path_string, JSON_FUN_DOUBLE, &document);
    if (root == nullptr || root->IsNull()) {
        return DoubleVal::null();
    } else if (root->IsInt()) {
        return DoubleVal(static_cast<double>(root->GetInt()));
    } else if (root->IsDouble()) {
        return DoubleVal(root->GetDouble());
    } else {
        return DoubleVal::null();
    }
}

StringVal JsonFunctions::json_array(FunctionContext* context, int num_args,
                                    const StringVal* json_str) {
    if (json_str->is_null) {
        return StringVal::null();
    }
    rapidjson::Value array_obj(rapidjson::kArrayType);
    rapidjson::Document document;
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
    //flag: The number it contains represents the type of previous parameters
    const StringVal& flag = json_str[num_args - 1];
    DCHECK_EQ(num_args - 1, flag.len);
    for (int i = 0; i < num_args - 1; ++i) {
        const StringVal& arg = json_str[i];
        rapidjson::Value val = parse_str_with_flag(arg, flag, i, allocator);
        array_obj.PushBack(val, allocator);
    }
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    array_obj.Accept(writer);
    return AnyValUtil::from_string_temp(context, std::string(buf.GetString()));
}

StringVal JsonFunctions::json_object(FunctionContext* context, int num_args,
                                     const StringVal* json_str) {
    if (json_str->is_null) {
        return StringVal::null();
    }
    rapidjson::Document document(rapidjson::kObjectType);
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
    const StringVal& flag = json_str[num_args - 1];
    document.SetObject();
    DCHECK_EQ(num_args - 1, flag.len);
    for (int i = 1; i < num_args - 1; i = i + 2) {
        const StringVal& arg = json_str[i];
        rapidjson::Value key(rapidjson::kStringType);
        key.SetString((char*)json_str[i - 1].ptr, json_str[i - 1].len, allocator);
        rapidjson::Value val = parse_str_with_flag(arg, flag, i, allocator);
        document.AddMember(key, val, allocator);
    }
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    document.Accept(writer);
    return AnyValUtil::from_string_temp(context, std::string(buf.GetString()));
}

rapidjson::Value JsonFunctions::parse_str_with_flag(const StringVal& arg, const StringVal& flag,
                                                    const int num,
                                                    rapidjson::Document::AllocatorType& allocator) {
    rapidjson::Value val;
    if (*(flag.ptr + num) == '0') { //null
        rapidjson::Value nullObject(rapidjson::kNullType);
        val = nullObject;
    } else if (*(flag.ptr + num) == '1') { //bool
        bool res = ((arg == "1") ? true : false);
        val.SetBool(res);
    } else if (*(flag.ptr + num) == '2') { //int
        std::stringstream ss;
        ss << arg.ptr;
        int number = 0;
        ss >> number;
        val.SetInt(number);
    } else if (*(flag.ptr + num) == '3') { //double
        std::stringstream ss;
        ss << arg.ptr;
        double number = 0.0;
        ss >> number;
        val.SetDouble(number);
    } else if (*(flag.ptr + num) == '4' || *(flag.ptr + num) == '5') {
        StringPiece str((char*)arg.ptr, arg.len);
        if (*(flag.ptr + num) == '4') {
            str = str.substr(1, str.length() - 2);
        }
        val.SetString(str.data(), str.length(), allocator);
    } else {
        DCHECK(false) << "parse json type error with unknown type";
    }
    return val;
}
StringVal JsonFunctions::json_quote(FunctionContext* context, const StringVal& json_str) {
    if (json_str.is_null) {
        return StringVal::null();
    }
    rapidjson::Value array_obj(rapidjson::kObjectType);
    array_obj.SetString(rapidjson::StringRef((char*)json_str.ptr, json_str.len));
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    array_obj.Accept(writer);
    return AnyValUtil::from_string_temp(context, std::string(buf.GetString()));
}

rapidjson::Value* JsonFunctions::match_value(const std::vector<JsonPath>& parsed_paths,
                                             rapidjson::Value* document,
                                             rapidjson::Document::AllocatorType& mem_allocator,
                                             bool is_insert_null) {
    rapidjson::Value* root = document;
    rapidjson::Value* array_obj = nullptr;
    for (int i = 1; i < parsed_paths.size(); i++) {
        VLOG_TRACE << "parsed_paths: " << parsed_paths[i].debug_string();

        if (root == nullptr || root->IsNull()) {
            return nullptr;
        }

        if (UNLIKELY(!parsed_paths[i].is_valid)) {
            return nullptr;
        }

        const std::string& col = parsed_paths[i].key;
        int index = parsed_paths[i].idx;
        if (LIKELY(!col.empty())) {
            if (root->IsArray()) {
                array_obj = static_cast<rapidjson::Value*>(
                        mem_allocator.Malloc(sizeof(rapidjson::Value)));
                array_obj->SetArray();
                bool is_null = true;

                // if array ,loop the array,find out all Objects,then find the results from the objects
                for (int j = 0; j < root->Size(); j++) {
                    rapidjson::Value* json_elem = &((*root)[j]);

                    if (json_elem->IsArray() || json_elem->IsNull()) {
                        continue;
                    } else {
                        if (!json_elem->IsObject()) {
                            continue;
                        }
                        if (!json_elem->HasMember(col.c_str())) {
                            if (is_insert_null) { // not found item, then insert a null object.
                                is_null = false;
                                rapidjson::Value nullObject(rapidjson::kNullType);
                                array_obj->PushBack(nullObject, mem_allocator);
                            }
                            continue;
                        }
                        rapidjson::Value* obj = &((*json_elem)[col.c_str()]);
                        if (obj->IsArray()) {
                            is_null = false;
                            for (int k = 0; k < obj->Size(); k++) {
                                array_obj->PushBack((*obj)[k], mem_allocator);
                            }
                        } else if (!obj->IsNull()) {
                            is_null = false;
                            array_obj->PushBack(*obj, mem_allocator);
                        }
                    }
                }

                root = is_null ? &(array_obj->SetNull()) : array_obj;
            } else if (root->IsObject()) {
                if (!root->HasMember(col.c_str())) {
                    return nullptr;
                } else {
                    root = &((*root)[col.c_str()]);
                }
            } else {
                // root is not a nested type, return nullptr
                return nullptr;
            }
        }

        if (UNLIKELY(index != -1)) {
            // judge the rapidjson:Value, which base the top's result,
            // if not array return nullptr;else get the index value from the array
            if (root->IsArray()) {
                if (root->IsNull()) {
                    return nullptr;
                } else if (index == -2) {
                    // [*]
                    array_obj = static_cast<rapidjson::Value*>(
                            mem_allocator.Malloc(sizeof(rapidjson::Value)));
                    array_obj->SetArray();

                    for (int j = 0; j < root->Size(); j++) {
                        rapidjson::Value v;
                        v.CopyFrom((*root)[j], mem_allocator);
                        array_obj->PushBack(v, mem_allocator);
                    }
                    root = array_obj;
                } else if (index >= root->Size()) {
                    return nullptr;
                } else {
                    root = &((*root)[index]);
                }
            } else {
                return nullptr;
            }
        }
    }
    return root;
}

rapidjson::Value* JsonFunctions::get_json_object(FunctionContext* context,
                                                 const std::string_view& json_string,
                                                 const std::string_view& path_string,
                                                 const JsonFunctionType& fntype,
                                                 rapidjson::Document* document) {
    // split path by ".", and escape quota by "\"
    // eg:
    //    '$.text#abc.xyz'  ->  [$, text#abc, xyz]
    //    '$."text.abc".xyz'  ->  [$, text.abc, xyz]
    //    '$."text.abc"[1].xyz'  ->  [$, text.abc[1], xyz]
    JsonState* json_state = nullptr;
    JsonState tmp_json_state;

#ifndef BE_TEST
    json_state = reinterpret_cast<JsonState*>(
            context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (json_state == nullptr) {
        json_state = &tmp_json_state;
    }

    if (json_state->json_paths.size() == 0) {
        auto tok = get_json_token(path_string);
        std::vector<std::string> paths(tok.begin(), tok.end());
        get_parsed_paths(paths, &json_state->json_paths);
    }
#else
    json_state = &tmp_json_state;
    auto tok = get_json_token(path_string);
    std::vector<std::string> paths(tok.begin(), tok.end());
    get_parsed_paths(paths, &json_state->json_paths);
#endif

    VLOG_TRACE << "first parsed path: " << json_state->json_paths[0].debug_string();

    if (!json_state->json_paths[0].is_valid) {
        return document;
    }

    if (UNLIKELY(json_state->json_paths.size() == 1)) {
        if (fntype == JSON_FUN_STRING) {
            document->SetString(json_string.data(), json_string.length(), document->GetAllocator());
        } else {
            return document;
        }
    }

    if (!json_state->document.IsNull()) {
        document = &json_state->document;
    } else {
        document->Parse(json_string.data(), json_string.length());
        //rapidjson::Document document;
        if (UNLIKELY(document->HasParseError())) {
            VLOG_CRITICAL << "Error at offset " << document->GetErrorOffset() << ": "
                          << GetParseError_En(document->GetParseError());
            document->SetNull();
            return document;
        }
    }

    return match_value(json_state->json_paths, document, document->GetAllocator());
}

rapidjson::Value* JsonFunctions::get_json_array_from_parsed_json(
        const std::string& json_path, rapidjson::Value* document,
        rapidjson::Document::AllocatorType& mem_allocator, bool* wrap_explicitly) {
    std::vector<JsonPath> vec;
    parse_json_paths(json_path, &vec);
    return get_json_array_from_parsed_json(vec, document, mem_allocator, wrap_explicitly);
}

rapidjson::Value* JsonFunctions::get_json_array_from_parsed_json(
        const std::vector<JsonPath>& parsed_paths, rapidjson::Value* document,
        rapidjson::Document::AllocatorType& mem_allocator, bool* wrap_explicitly) {
    *wrap_explicitly = false;
    if (!parsed_paths[0].is_valid) {
        return nullptr;
    }

    if (parsed_paths.size() == 1) {
        // the json path is "$", just return entire document
        // wrapper an array
        rapidjson::Value* array_obj = nullptr;
        array_obj = static_cast<rapidjson::Value*>(mem_allocator.Malloc(sizeof(rapidjson::Value)));
        array_obj->SetArray();
        array_obj->PushBack(*document, mem_allocator);
        return array_obj;
    }

    rapidjson::Value* root = match_value(parsed_paths, document, mem_allocator, true);
    if (root == nullptr || root == document) { // not found
        return nullptr;
    } else if (!root->IsArray()) {
        rapidjson::Value* array_obj = nullptr;
        array_obj = static_cast<rapidjson::Value*>(mem_allocator.Malloc(sizeof(rapidjson::Value)));
        array_obj->SetArray();
        array_obj->PushBack(*root, mem_allocator);
        // set `wrap_explicitly` to true, so that the caller knows that this Array is wrapped actively.
        *wrap_explicitly = true;
        return array_obj;
    }
    return root;
}

rapidjson::Value* JsonFunctions::get_json_object_from_parsed_json(
        const std::vector<JsonPath>& parsed_paths, rapidjson::Value* document,
        rapidjson::Document::AllocatorType& mem_allocator) {
    if (!parsed_paths[0].is_valid) {
        return nullptr;
    }

    if (parsed_paths.size() == 1) {
        // the json path is "$", just return entire document
        return document;
    }

    rapidjson::Value* root = match_value(parsed_paths, document, mem_allocator, true);
    if (root == nullptr || root == document) { // not found
        return nullptr;
    }
    return root;
}

void JsonFunctions::json_path_prepare(doris_udf::FunctionContext* context,
                                      doris_udf::FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }

    if (!context->is_arg_constant(0) && !context->is_arg_constant(1)) {
        return;
    }

    JsonState* json_state = new JsonState;

    StringVal* json_str = reinterpret_cast<StringVal*>(context->get_constant_arg(0));
    if (json_str != nullptr && !json_str->is_null) {
        std::string json_string((char*)json_str->ptr, json_str->len);
        json_state->document.Parse(json_string.c_str());
    }
    StringVal* path = reinterpret_cast<StringVal*>(context->get_constant_arg(1));
    if (path != nullptr && !path->is_null) {
        std::string path_str(reinterpret_cast<char*>(path->ptr), path->len);
        boost::tokenizer<boost::escaped_list_separator<char>> tok(
                path_str, boost::escaped_list_separator<char>("\\", ".", "\""));
        std::vector<std::string> path_exprs(tok.begin(), tok.end());
        get_parsed_paths(path_exprs, &json_state->json_paths);
    }

    context->set_function_state(scope, json_state);
    VLOG_TRACE << "prepare json path. size: " << json_state->json_paths.size();
}

void JsonFunctions::json_path_close(doris_udf::FunctionContext* context,
                                    doris_udf::FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }

    JsonState* json_state = reinterpret_cast<JsonState*>(context->get_function_state(scope));
    if (json_state != nullptr) {
        delete json_state;
        VLOG_TRACE << "close json state";
    }
}

void JsonFunctions::parse_json_paths(const std::string& path_string,
                                     std::vector<JsonPath>* parsed_paths) {
    // split path by ".", and escape quota by "\"
    // eg:
    //    '$.text#abc.xyz'  ->  [$, text#abc, xyz]
    //    '$."text.abc".xyz'  ->  [$, text.abc, xyz]
    //    '$."text.abc"[1].xyz'  ->  [$, text.abc[1], xyz]
    boost::tokenizer<boost::escaped_list_separator<char>> tok(
            path_string, boost::escaped_list_separator<char>("\\", ".", "\""));
    std::vector<std::string> paths(tok.begin(), tok.end());
    get_parsed_paths(paths, parsed_paths);
}

void JsonFunctions::get_parsed_paths(const std::vector<std::string>& path_exprs,
                                     std::vector<JsonPath>* parsed_paths) {
    if (path_exprs.empty()) {
        return;
    }

    if (path_exprs[0] != "$") {
        parsed_paths->emplace_back("", -1, false);
    } else {
        parsed_paths->emplace_back("$", -1, true);
    }

    for (int i = 1; i < path_exprs.size(); i++) {
        std::string col;
        std::string index;
        if (UNLIKELY(!RE2::FullMatch(path_exprs[i], JSON_PATTERN, &col, &index))) {
            parsed_paths->emplace_back("", -1, false);
        } else {
            int idx = -1;
            if (!index.empty()) {
                if (index == "*") {
                    idx = -2;
                } else {
                    idx = atoi(index.c_str());
                }
            }
            parsed_paths->emplace_back(std::move(col), idx, true);
        }
    }
}

} // namespace doris
