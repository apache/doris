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

#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

void register_function_ip(SimpleFunctionFactory& factory) {
    /// IPv4 convert between string and num part
    factory.register_function<FunctionIPv4NumToString>();
    factory.register_alias(FunctionIPv4NumToString::name, "inet_ntoa");
    factory.register_function<FunctionIPv4StringToNum<IPConvertExceptionMode::Throw>>();
    factory.register_function<FunctionIPv4StringToNum<IPConvertExceptionMode::Default>>();
    factory.register_function<FunctionIPv4StringToNum<IPConvertExceptionMode::Null>>();
    factory.register_alias(FunctionIPv4StringToNum<IPConvertExceptionMode::Null>::name,
                           "inet_aton");

    /// IPv6 convert between string and num part
    factory.register_function<FunctionIPv6NumToString>();
    factory.register_alias(FunctionIPv6NumToString::name, "inet6_ntoa");
    factory.register_function<FunctionIPv6StringToNum<IPConvertExceptionMode::Throw>>();
    factory.register_function<FunctionIPv6StringToNum<IPConvertExceptionMode::Default>>();
    factory.register_function<FunctionIPv6StringToNum<IPConvertExceptionMode::Null>>();
    factory.register_alias(FunctionIPv6StringToNum<IPConvertExceptionMode::Null>::name,
                           "inet6_aton");

    /// Judge part
    factory.register_function<FunctionIsIPv4Compat>();
    factory.register_function<FunctionIsIPv4Mapped>();
    factory.register_function<FunctionIsIPString<IPv4>>();
    factory.register_function<FunctionIsIPString<IPv6>>();
    factory.register_function<FunctionIsIPAddressInRange>();

    /// CIDR part
    factory.register_function<FunctionIPv4CIDRToRange>();
    factory.register_function<FunctionIPv6CIDRToRange>();

    /// Convert to IPv4/IPv6 part
    factory.register_function<FunctionToIP<IPConvertExceptionMode::Throw, IPv4>>();
    factory.register_function<FunctionToIP<IPConvertExceptionMode::Default, IPv4>>();
    factory.register_function<FunctionToIP<IPConvertExceptionMode::Null, IPv4>>();
    factory.register_function<FunctionToIP<IPConvertExceptionMode::Throw, IPv6>>();
    factory.register_function<FunctionToIP<IPConvertExceptionMode::Default, IPv6>>();
    factory.register_function<FunctionToIP<IPConvertExceptionMode::Null, IPv6>>();

    /// Convert between IPv4 and IPv6 part
    factory.register_function<FunctionIPv4ToIPv6>();

    /// Cut IPv6 part
    factory.register_function<FunctionCutIPv6>();
}
} // namespace doris::vectorized