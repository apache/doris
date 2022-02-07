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

#include "util/error_util.h"

#include <errno.h>
#include <cstring>
#include <sstream>
#include <vector>

using std::string;
using std::stringstream;
using std::vector;
using std::ostream;

namespace doris {

string get_str_err_msg() {
    // Save errno. "<<" could reset it.
    int e = errno;
    if (e == 0) {
        return "";
    }
    stringstream ss;
    char buf[1024];
    ss << "Error(" << e << "): " << strerror_r(e, buf, 1024);
    return ss.str();
}

#if 0

string get_tables_missing_stats_warning(const vector<TTableName>& tables_missing_stats) {
    stringstream ss;
    if (tables_missing_stats.empty()) return string("");
    ss << "WARNING: The following tables are missing relevant table and/or column "
        << "statistics.\n";
    for (int i = 0; i < tables_missing_stats.size(); ++i) {
        const TTableName& table_name = tables_missing_stats[i];
        if (i != 0) ss << ",";
        ss << table_name.db_name << "." << table_name.table_name;
    }
    return ss.str();
}

ErrorMsg::ErrorMsg(TErrorCode::type error) : error_(error),
        message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_])) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0) : error_(error),
        message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_], arg0)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1) :
        error_(error),
        message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
                arg0, arg1)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
        const ArgType& arg2) :
        error_(error),
        message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
                arg0, arg1, arg2)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
        const ArgType& arg2, const ArgType& arg3) :
        error_(error),
        message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
                arg0, arg1, arg2, arg3)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
        const ArgType& arg2, const ArgType& arg3, const ArgType& arg4) :
        error_(error),
        message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
                    arg0, arg1, arg2, arg3, arg4)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
        const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
        const ArgType& arg5) :
        error_(error),
        message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
                    arg0, arg1, arg2, arg3, arg4, arg5)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
        const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
        const ArgType& arg5, const ArgType& arg6) :
        error_(error),
        message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
                    arg0, arg1, arg2, arg3, arg4, arg5, arg6)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
        const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
        const ArgType& arg5, const ArgType& arg6, const ArgType& arg7) :
        error_(error),
        message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
                    arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
        const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
        const ArgType& arg5, const ArgType& arg6, const ArgType& arg7,
        const ArgType& arg8) :
        error_(error),
        message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
                    arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
        const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
        const ArgType& arg5, const ArgType& arg6, const ArgType& arg7,
        const ArgType& arg8, const ArgType& arg9) :
        error_(error),
        message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
                    arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9)) {}

ErrorMsg ErrorMsg::init(TErrorCode::type error, const ArgType& arg0,
        const ArgType& arg1, const ArgType& arg2, const ArgType& arg3,
        const ArgType& arg4, const ArgType& arg5, const ArgType& arg6, const ArgType& arg7,
        const ArgType& arg8, const ArgType& arg9) {

    ErrorCodesConstants error_strings;
    ErrorMsg m;
    m.error_ = error;
    m.message_ = strings::Substitute(error_strings.TErrorMessage[m.error_],
            arg0, arg1, arg2, arg3, arg4, arg5,
            arg6, arg7, arg8, arg9);
    return m;
}

void print_error_map(ostream* stream, const ErrorLogMap& errors) {
    for (const ErrorLogMap::value_type& v : errors) {
        if (v.first == TErrorCode::GENERAL) {
            for (const string& s : v.second.messages) {
                *stream << s << "\n";
            }
        } else {
            *stream << v.second.messages.front();
            if (v.second.count < 2) {
                *stream << "\n";
            } else {
                *stream << " (1 of " << v.second.count << " similar)\n";
            }
        }
    }
}

string print_error_map_to_string(const ErrorLogMap& errors) {
    stringstream stream;
    PrintErrorMap(&stream, errors);
    return stream.str();
}

void merge_error_maps(ErrorLogMap* left, const ErrorLogMap& right) {
    for (const ErrorLogMap::value_type& v : right) {
        // Append generic message, append specific codes or increment count if exists
        if (v.first == TErrorCode::GENERAL) {
            (*left)[v.first].messages.insert(
                    (*left)[v.first].messages.end(), v.second.messages.begin(),
                    v.second.messages.end());
        } else {
            if ((*left).count(v.first) > 0) {
                (*left)[v.first].count += v.second.count;
            } else {
                (*left)[v.first].messages.push_back(v.second.messages.front());
                (*left)[v.first].count = v.second.count;
            }
        }
    }
}

void append_error(ErrorLogMap* map, const ErrorMsg& e) {
    if (e.error() == TErrorCode::GENERAL) {
        (*map)[e.error()].messages.push_back(e.msg());
    } else {
        ErrorLogMap::iterator it = map->find(e.error());
        if (it != map->end()) {
            ++(it->second.count);
        } else {
            (*map)[e.error()].messages.push_back(e.msg());
            (*map)[e.error()].count = 1;
        }
    }
}

size_t error_count(const ErrorLogMap& errors) {
    ErrorLogMap::const_iterator cit = errors.find(TErrorCode::GENERAL);
    size_t general_errors = cit != errors.end() ?
        errors.find(TErrorCode::GENERAL)->second.messages.size() - 1 : 0;
    return errors.size() + general_errors;
}

#endif // end '#if 0': comment these code

} // namespace doris
