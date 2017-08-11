// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#ifndef BDG_PALO_BE_SRC_RPC_STRING_EXT_H
#define BDG_PALO_BE_SRC_RPC_STRING_EXT_H

#include "util.h"

#include <cstdio>
#include <set>
#include <map>

namespace palo {

/** STL Set managing std::strings */
typedef std::set<std::string> StringSet;

/** STL Strict Weak Ordering for comparing c-style strings */
struct LtCstr {
    bool operator()(const char *s1, const char *s2) const {
        return strcmp(s1, s2) < 0;
    }
};

/** STL Set managing c-style strings */
typedef std::set<const char *, LtCstr> CstrSet;

/** STL map from c-style string to int32_t */
typedef std::map<const char *, int32_t, LtCstr> CstrToInt32Map;

/** STL map from c-style string to int64_t */
typedef std::map<const char *, int64_t, LtCstr> CstrToInt64MapT;

/** Append operator for shorts */ 
inline std::string operator+(const std::string &s1, short sval) {
    return s1 + Int16Formatter(sval).c_str();
}

/** Append operator for ushorts */
inline std::string operator+(const std::string &s1, uint16_t sval) {
    return s1 + UInt16Formatter(sval).c_str();
}

/** Append operator for integers */ 
inline std::string operator+(const std::string &s1, int ival) {
    return s1 + Int32Formatter(ival).c_str();
}

/** Append operator for unsigned integers */ 
inline std::string operator+(const std::string &s1, uint32_t ival) {
    return s1 + UInt32Formatter(ival).c_str();
}

/** Append operator for 64bit integers */ 
inline std::string operator+(const std::string &s1, int64_t llval) {
    return s1 + Int64Formatter(llval).c_str();
}

/** Append operator for 64bit unsigned integers */ 
inline std::string operator+(const std::string &s1, uint64_t llval) {
    return s1 + UInt64Formatter(llval).c_str();
}

} //namespace palo
#endif //BDG_PALO_BE_SRC_RPC_STRING_EXT_H
