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

#include "vec/functions/function_ip.h"

namespace doris::vectorized {

// IPv4 functions
struct NameFunctionIPv4NumToString {
    static constexpr auto name = "ipv4numtostring";
};

struct NameFunctionIPv4StringToNum {
    static constexpr auto name = "ipv4stringtonum";
};

struct NameFunctionIPv4StringToNumOrDefault {
    static constexpr auto name = "ipv4stringtonum_or_default";
};

struct NameFunctionIsIPv4String {
    static constexpr auto name = "isipv4string";
};

// IPv6 functions
struct NameFunctionIPv6NumToString {
    static constexpr auto name = "ipv6numtostring";
};

struct NameFunctionIPv6StringToNum {
    static constexpr auto name = "ipv6stringtonum";
};

struct NameFunctionIPv6StringToNumOrDefault {
    static constexpr auto name = "ipv6stringtonum_or_default";
};

struct NameFunctionIsIPv6String {
    static constexpr auto name = "isipv6string";
};

void register_function_ip(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionIPv4NumToString<NameFunctionIPv4NumToString>>();
    factory.register_function<FunctionIPv4StringToNum<NameFunctionIPv4StringToNum>>();
    factory.register_function<FunctionIPv4StringToNumOrDefault<NameFunctionIPv4StringToNumOrDefault>>();
    factory.register_function<FunctionIsIPv4String<NameFunctionIsIPv4String>>();
    factory.register_function<FunctionIPv6NumToString<NameFunctionIPv6NumToString>>();
    factory.register_function<FunctionIPv6StringToNum<NameFunctionIPv6StringToNum>>();
    factory.register_function<FunctionIPv6StringToNumOrDefault<NameFunctionIPv6StringToNumOrDefault>>();
    factory.register_function<FunctionIsIPv6String<NameFunctionIsIPv6String>>();

    factory.register_alias(NameFunctionIPv4NumToString::name, "inet_ntoa");
    factory.register_alias(NameFunctionIPv4StringToNum::name, "inet_aton");
    factory.register_alias(NameFunctionIPv4StringToNum::name, "ipv4stringtonum_or_null");
    factory.register_alias(NameFunctionIPv6NumToString::name, "inet6_ntoa");
    factory.register_alias(NameFunctionIPv6StringToNum::name, "inet6_aton");
    factory.register_alias(NameFunctionIPv6StringToNum::name, "ipv6stringtonum_or_null");
}
} // namespace doris::vectorized