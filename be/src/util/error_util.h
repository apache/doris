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

#ifndef DORIS_BE_SRC_UTIL_ERROR_UTIL_H
#define DORIS_BE_SRC_UTIL_ERROR_UTIL_H

#include <boost/cstdint.hpp>
#include <boost/lexical_cast.hpp>
#include <string>
#include <vector>

// #include "gen-cpp/CatalogObjects_types.h"
// #include "gen-cpp/ErrorCodes_types.h"
// #include "gen-cpp/ErrorCodes_constants.h"
// #include "gen-cpp/ImpalaInternalService_types.h"
// #include "gutil/strings/substitute.h"

namespace doris {

// Returns the error message for errno. We should not use strerror directly
// as that is not thread safe.
// Returns empty string if errno is 0.
std::string get_str_err_msg();

#if 0
/// Returns an error message warning that the given table names are missing relevant
/// table/and or column statistics.
std::string get_tables_missing_stats_warning(const std::vector<TTableName>& tables_missing_stats);

/// Class that holds a formatted error message and potentially a set of detail
/// messages. Error messages are intended to be user facing. Error details can be attached
/// as strings to the message. These details should only be accessed internally.
class ErrorMsg {
public:
    typedef strings::internal::SubstituteArg ArgType;

    /// Trivial constructor.
    ErrorMsg() : _error(TErrorCode::OK) {}

    /// Below are a set of overloaded constructors taking all possible number of arguments
    /// that can be passed to Substitute. The reason is to try to avoid forcing the compiler
    /// putting all arguments for Substitute() on the stack whenver this is called and thus
    /// polute the instruction cache.
    explicit ErrorMsg(TErrorCode::type error);
    ErrorMsg(TErrorCode::type error, const ArgType& arg0);
    ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1);
    ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
            const ArgType& arg2);
    ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
            const ArgType& arg2, const ArgType& arg3);
    ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
            const ArgType& arg2, const ArgType& arg3, const ArgType& arg4);
    ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
            const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
            const ArgType& arg5);
    ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
            const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
            const ArgType& arg5, const ArgType& arg6);
    ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
            const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
            const ArgType& arg5, const ArgType& arg6, const ArgType& arg7);
    ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
            const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
            const ArgType& arg5, const ArgType& arg6, const ArgType& arg7,
            const ArgType& arg8);
    ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
            const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
            const ArgType& arg5, const ArgType& arg6, const ArgType& arg7,
            const ArgType& arg8, const ArgType& arg9);

    ErrorMsg(TErrorCode::type error, const std::vector<string>& detail)
        : _error(error), _details(detail) {}

    /// Static initializer that is needed to avoid issues with static initialization order
    /// and the point in time when the string list generated via thrift becomes
    /// available. This method should not be used if no static initialization is needed as
    /// the cost of this method is proportional to the number of entries in the global error
    /// message list.
    /// WARNING: DO NOT CALL THIS METHOD IN A NON STATIC CONTEXT
    static ErrorMsg init(TErrorCode::type error, const ArgType& arg0 = ArgType::NoArg,
            const ArgType& arg1 = ArgType::NoArg,
            const ArgType& arg2 = ArgType::NoArg,
            const ArgType& arg3 = ArgType::NoArg,
            const ArgType& arg4 = ArgType::NoArg,
            const ArgType& arg5 = ArgType::NoArg,
            const ArgType& arg6 = ArgType::NoArg,
            const ArgType& arg7 = ArgType::NoArg,
            const ArgType& arg8 = ArgType::NoArg,
            const ArgType& arg9 = ArgType::NoArg);

    TErrorCode::type error() const { return _error; }

    /// Add detail string message.
    void add_detail(const std::string& d) {
        _details.push_back(d);
    }

    /// Set a specific error code.
    void set_error(TErrorCode::type e) {
        _error = e;
    }

    /// Return the formatted error string.
    const std::string& msg() const {
        return _message;
    }

    const std::vector<std::string>& details() const {
        return _details;
    }

    /// Produce a string representation of the error message that includes the formatted
    /// message of the original error and the attached detail strings.
    std::string get_full_message_details() const {
        std::stringstream ss;
        ss << _message << "\n";
        for (size_t i = 0, end = _details.size(); i < end; ++i) {
            ss << _details[i] << "\n";
        }
        return ss.str();
    }

private:
    TErrorCode::type _error;
    std::string _message;
    std::vector<std::string> _details;
};

/// Track log messages per error code.
typedef std::map<TErrorCode::type, TErrorLogEntry> ErrorLogMap;

/// Merge error maps. Merging of error maps occurs, when the errors from multiple backends
/// are merged into a single error map.  General log messages are simply appended,
/// specific errors are deduplicated by either appending a new instance or incrementing
/// the count of an existing one.
void merge_error_maps(ErrorLogMap* left, const ErrorLogMap& right);

/// Append an error to the error map. Performs the aggregation as follows: GENERAL errors
/// are appended to the list of GENERAL errors, to keep one item each in the map, while
/// for all other error codes only the count is incremented and only the first message
/// is kept as a sample.
void append_error(ErrorLogMap* map, const ErrorMsg& e);

/// Helper method to print the contents of an ErrorMap to a stream.
void print_error_map(std::ostream* stream, const ErrorLogMap& errors);

/// Return the number of errors within this error maps. General errors are counted
/// individually, while specific errors are counted once per distinct occurrence.
size_t error_count(const ErrorLogMap& errors);

/// Generate a string representation of the error map. Produces the same output as
/// PrintErrorMap, but returns a string instead of using a stream.
std::string print_error_map_to_string(const ErrorLogMap& errors);

#endif // end '#if 0': comment these code

} // end namespace doris

#endif // DORIS_BE_SRC_UTIL_ERROR_UTIL_H
