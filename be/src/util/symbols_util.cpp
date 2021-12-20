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

#include "util/symbols_util.h"

#include <cxxabi.h>

#include <regex>
#include <sstream>

namespace doris {
// For the rules about gcc-compatible name mangling, see:
// http://mentorembedded.github.io/cxx-abi/abi.html#mangling
// This implementation *is* not generally compatible. It is hard coded to
// only work with functions that implement the UDF or UDA signature. That is,
// functions of the form:
//   namespace::Function(doris_udf::FunctionContext*, const doris_udf::AnyVal&, etc)
//
// The general idea is to walk the types left to right and output them. This happens
// in a single pass. User literals are output as <len><literal>. There are many reserved,
// usually single character tokens for native types and specifying if something is a
// pointer.
//
// One additional piece of complexity is that repeated literals are compressed out.
// As literals are output, they are associated with an ID. The next time that
// we encounter the literal, we output the ID instead.
// We don't implement this generally since the way the literals are added to the
// dictionary is much more general than we need.
// e.g. for the literal ns1::ns2::class::type,
// the dictionary would add 4 literals: 'ns1', 'ns1::ns2', 'ns1::ns2::class',
//    'ns1::ns2::class::type'
// We instead take some shortcuts since we know all the argument types are
// types we define.

// Mangled symbols must start with this.
const char* MANGLE_PREFIX = "_Z";

bool SymbolsUtil::is_mangled(const std::string& symbol) {
    return strncmp(symbol.c_str(), MANGLE_PREFIX, strlen(MANGLE_PREFIX)) == 0;
}

std::string SymbolsUtil::demangle(const std::string& name) {
    int status = 0;
    char* demangled_name = abi::__cxa_demangle(name.c_str(), nullptr, nullptr, &status);
    if (status != 0) {
        return name;
    }
    std::string result = demangled_name;
    free(demangled_name);
    return result;
}

std::string SymbolsUtil::demangle_no_args(const std::string& symbol) {
    std::string fn_name = demangle(symbol);
    // Chop off argument list (e.g. "foo(int)" => "foo")
    return fn_name.substr(0, fn_name.find('('));
}

std::string SymbolsUtil::demangle_name_only(const std::string& symbol) {
    std::string fn_name = demangle_no_args(symbol);
    // Chop off namespace and/or class name if present (e.g. "doris::foo" => "foo")
    // TODO: fix for templates
    return fn_name.substr(fn_name.find_last_of(':') + 1);
}

// Appends <Length><String> to the stream.
// e.g. Hello --> "5Hello"
static void append_mangled_token(const std::string& s, std::stringstream* out) {
    DCHECK(!s.empty());
    (*out) << s.size() << s;
}

// Outputs the seq_id. This is base 36 encoded with an S prefix and _ suffix.
// As an added optimization, the "seq_id - 1" value is output with the first
// token as just "S".
// e.g. seq_id 0: "S_"
//      seq_id 1: "S0_"
//      seq_id 2: "S1_"
static void append_seq_id(int seq_id, std::stringstream* out) {
    DCHECK_GE(seq_id, 0);
    if (seq_id == 0) {
        (*out) << "S_";
        return;
    }
    --seq_id;
    char buffer[10];
    char* ptr = buffer + 10;
    if (seq_id == 0) {
        *--ptr = '0';
    }
    while (seq_id != 0) {
        DCHECK(ptr > buffer);
        char c = static_cast<char>(seq_id % 36);
        *--ptr = (c < 10 ? '0' + c : 'A' + c - 10);
        seq_id /= 36;
    }
    (*out) << "S";
    out->write(ptr, 10 - (ptr - buffer));
    (*out) << "_";
}

static void append_any_val_type(int namespace_id, const TypeDescriptor& type,
                                std::stringstream* s) {
    (*s) << "N";
    // All the AnyVal types are in the doris_udf namespace, that token
    // already came with doris_udf::FunctionContext
    append_seq_id(namespace_id, s);

    switch (type.type) {
    case TYPE_BOOLEAN:
        append_mangled_token("BooleanVal", s);
        break;
    case TYPE_TINYINT:
        append_mangled_token("TinyIntVal", s);
        break;
    case TYPE_SMALLINT:
        append_mangled_token("SmallIntVal", s);
        break;
    case TYPE_INT:
        append_mangled_token("IntVal", s);
        break;
    case TYPE_BIGINT:
        append_mangled_token("BigIntVal", s);
        break;
    case TYPE_LARGEINT:
        append_mangled_token("LargeIntVal", s);
        break;
    case TYPE_FLOAT:
        append_mangled_token("FloatVal", s);
        break;
    case TYPE_TIME:
    case TYPE_DOUBLE:
        append_mangled_token("DoubleVal", s);
        break;
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_HLL:
    case TYPE_OBJECT:
    case TYPE_STRING:
        append_mangled_token("StringVal", s);
        break;
    case TYPE_DATE:
    case TYPE_DATETIME:
        append_mangled_token("DateTimeVal", s);
        break;
    case TYPE_DECIMALV2:
        append_mangled_token("DecimalV2Val", s);
        break;
    default:
        DCHECK(false) << "NYI: " << type.debug_string();
    }
    (*s) << "E"; // end doris_udf namespace
}

std::string SymbolsUtil::mangle_user_function(const std::string& fn_name,
                                              const std::vector<TypeDescriptor>& arg_types,
                                              bool has_var_args, TypeDescriptor* ret_arg_type) {
    // We need to split fn_name by :: to separate scoping from tokens
    const std::regex re("::");
    std::sregex_token_iterator it {fn_name.begin(), fn_name.end(), re, -1};
    std::vector<std::string> name_tokens {it, {}};

    // Mangled names use substitution as a builtin compression. The first time a token
    // is seen, we output the raw token string and store the index ("seq_id"). The
    // next time we see the same token, we output the index instead.
    int seq_id = 0;

    // Sequence id for the doris_udf namespace token
    int doris_udf_seq_id = -1;

    std::stringstream ss;
    ss << MANGLE_PREFIX;
    if (name_tokens.size() > 1) {
        ss << "N";                        // Start namespace
        seq_id += name_tokens.size() - 1; // Append for all the name space tokens.
    }
    for (int i = 0; i < name_tokens.size(); ++i) {
        append_mangled_token(name_tokens[i], &ss);
    }
    if (name_tokens.size() > 1) {
        ss << "E"; // End fn namespace
    }
    ss << "PN"; // First argument and start of FunctionContext namespace
    append_mangled_token("doris_udf", &ss);
    doris_udf_seq_id = seq_id++;
    append_mangled_token("FunctionContext", &ss);
    ++seq_id;
    ss << "E"; // E indicates end of namespace

    std::map<PrimitiveType, int> argument_map;
    for (int i = 0; i < arg_types.size(); ++i) {
        int repeated_symbol_idx = -1; // Set to >0, if we've seen the symbol.
        if (argument_map.find(arg_types[i].type) != argument_map.end()) {
            repeated_symbol_idx = argument_map[arg_types[i].type];
        }

        if (has_var_args && i == arg_types.size() - 1) {
            // We always specify varargs as int32 followed by the type.
            ss << "i"; // The argument for the number of varargs.
            ss << "P"; // This indicates what follows is a ptr (that is the array of varargs)
            ++seq_id;  // For "P"
            if (repeated_symbol_idx > 0) {
                append_seq_id(repeated_symbol_idx - 1, &ss);
                continue;
            }
        } else {
            if (repeated_symbol_idx > 0) {
                append_seq_id(repeated_symbol_idx, &ss);
                continue;
            }
            ss << "R"; // This indicates it is a reference type
            ++seq_id;  // For R.
        }

        ss << "K";   // This indicates it is const
        seq_id += 2; // For doris_udf::*Val, which is two tokens.
        append_any_val_type(doris_udf_seq_id, arg_types[i], &ss);
        argument_map[arg_types[i].type] = seq_id;
    }

    // Output return argument.
    if (ret_arg_type != nullptr) {
        int repeated_symbol_idx = -1;
        if (argument_map.find(ret_arg_type->type) != argument_map.end()) {
            repeated_symbol_idx = argument_map[ret_arg_type->type];
        }
        ss << "P"; // Return argument is a pointer

        if (repeated_symbol_idx != -1) {
            // This is always last and a pointer type.
            append_seq_id(argument_map[ret_arg_type->type] - 2, &ss);
        } else {
            append_any_val_type(doris_udf_seq_id, *ret_arg_type, &ss);
        }
    }

    return ss.str();
}

std::string SymbolsUtil::mangle_prepare_or_close_function(const std::string& fn_name) {
    // We need to split fn_name by :: to separate scoping from tokens
    const std::regex re("::");
    std::sregex_token_iterator it {fn_name.begin(), fn_name.end(), re, -1};
    std::vector<std::string> name_tokens {it, {}};

    // Mangled names use substitution as a builtin compression. The first time a token
    // is seen, we output the raw token string and store the index ("seq_id"). The
    // next time we see the same token, we output the index instead.
    int seq_id = 0;

    std::stringstream ss;
    ss << MANGLE_PREFIX;
    if (name_tokens.size() > 1) {
        ss << "N";                        // Start namespace
        seq_id += name_tokens.size() - 1; // Append for all the name space tokens.
    }
    for (int i = 0; i < name_tokens.size(); ++i) {
        append_mangled_token(name_tokens[i], &ss);
    }
    if (name_tokens.size() > 1) {
        ss << "E"; // End fn namespace
    }

    ss << "PN"; // FunctionContext* argument and start of FunctionContext namespace
    append_mangled_token("doris_udf", &ss);
    append_mangled_token("FunctionContext", &ss);
    ss << "E"; // E indicates end of namespace

    ss << "NS"; // FunctionStateScope argument
    ss << seq_id;
    ss << "_";
    append_mangled_token("FunctionStateScope", &ss);
    ss << "E"; // E indicates end of namespace

    return ss.str();
}
} // namespace doris
