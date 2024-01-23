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

void register_function_ip(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionIPv4NumToString>();
    factory.register_alias(FunctionIPv4NumToString::name, "inet_ntoa");
    factory.register_function<FunctionIPv4StringToNum<IPExceptionMode::Throw>>();
    factory.register_function<FunctionIPv4StringToNum<IPExceptionMode::Default>>();
    factory.register_function<FunctionIPv4StringToNum<IPExceptionMode::Null>>();
    factory.register_alias(FunctionIPv4StringToNum<IPExceptionMode::Null>::name, "inet_aton");
    factory.register_function<FunctionIPv6NumToString>();
    factory.register_alias(FunctionIPv6NumToString::name, "inet6_ntoa");
    factory.register_function<FunctionIPv6StringToNum<IPExceptionMode::Throw>>();
    factory.register_function<FunctionIPv6StringToNum<IPExceptionMode::Default>>();
    factory.register_function<FunctionIPv6StringToNum<IPExceptionMode::Null>>();
    factory.register_alias(FunctionIPv6StringToNum<IPExceptionMode::Null>::name, "inet6_aton");
    factory.register_function<FunctionIsIPv4Compat>();
    factory.register_function<FunctionIsIPv4Mapped>();
    factory.register_function<FunctionIsIPString<IPv4>>();
    factory.register_function<FunctionIsIPString<IPv6>>();
    factory.register_function<FunctionIsIPAddressInRange>();
    factory.register_function<FunctionIPv6CIDRToRange>();
    factory.register_function<FunctionToIP<IPExceptionMode::Throw, IPv4>>();
    factory.register_function<FunctionToIP<IPExceptionMode::Default, IPv4>>();
    factory.register_function<FunctionToIP<IPExceptionMode::Null, IPv4>>();
    factory.register_function<FunctionToIP<IPExceptionMode::Throw, IPv6>>();
    factory.register_function<FunctionToIP<IPExceptionMode::Default, IPv6>>();
    factory.register_function<FunctionToIP<IPExceptionMode::Null, IPv6>>();
}
} // namespace doris::vectorized