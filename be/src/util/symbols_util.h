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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/symbols-util.h
// and modified by Doris

#ifndef DORIS_BE_SRC_COMMON_UTIL_SYMBOLS_UTIL_H
#define DORIS_BE_SRC_COMMON_UTIL_SYMBOLS_UTIL_H

#include <string>
#include <vector>

#include "runtime/types.h"

namespace doris {

/// Utility class to manipulate c++/IR symbols, mangling and demangling names.
class SymbolsUtil {
public:
    /// Returns true if this symbol is mangled.
    static bool is_mangled(const std::string& symbol);

    /// Returns the demangled string. The name is assumed to be a mangled string using the
    /// gcc/llvm convention.
    /// Returns the original input if it cannot be demangled.
    static std::string demangle(const std::string& name);

    /// Returns the fully-qualified function name of 'symbol' (i.e. it strips the arguments
    /// but retains any namespace and class names). 'symbol' may be mangled or unmangled.
    /// Returns the original input if it cannot be demangled.
    /// Example: "doris::foo(int arg1)" => "doris::foo"
    static std::string demangle_no_args(const std::string& symbol);

    /// Returns the function name of 'symbol' (i.e., it strips the arguments and any
    /// namespace/class qualifiers). 'symbol' may be mangled or unmangled.
    /// Returns the original input if it cannot be demangled.
    /// Example: "doris::foo(int arg1)" => "foo"
    static std::string demangle_name_only(const std::string& symbol);

    /// Mangles fn_name with 'arg_types' to the function signature for user functions.
    /// This maps types to AnyVal* and automatically adds the FunctionContext*
    /// as the first argument.
    /// The fn_name must be fully qualified. i.e namespace::class::fn.
    /// if 'has_var_args' is true, the last argument in arg_types can be variable.
    /// if 'ret_argument' is non-null, it is added as a last return argument.
    /// TODO: this is not a general mangling function and that is more difficult to
    /// do. Find a library to do this.
    /// There is no place we require this to be perfect, if we can't do this right,
    /// the user will need to specify the full mangled string.
    static std::string mangle_user_function(const std::string& fn_name,
                                            const std::vector<TypeDescriptor>& arg_types,
                                            bool has_var_args, TypeDescriptor* ret_argument);

    /// Mangles fn_name assuming arguments
    /// (doris_udf::FunctionContext*, doris_udf::FunctionContext::FunctionStateScope).
    static std::string mangle_prepare_or_close_function(const std::string& fn_name);
};

} // namespace doris

#endif
