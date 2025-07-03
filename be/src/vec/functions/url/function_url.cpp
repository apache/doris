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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/URL/domain.cpp
// and modified by Doris

#include "vec/columns/column.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/function_string_to_string.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/functions/url/domain.h"
#include "vec/functions/url/functions_url.h"
#include "vec/functions/url/protocol.h"

namespace doris::vectorized {

struct NameDomain {
    static constexpr auto name = "domain";
};
using FunctionDomain =
        FunctionStringToString<ExtractSubstringImpl<ExtractDomain<false>>, NameDomain>;

struct NameDomainWithoutWWW {
    static constexpr auto name = "domain_without_www";
};
using FunctionDomainWithoutWWW =
        FunctionStringToString<ExtractSubstringImpl<ExtractDomain<true>>, NameDomainWithoutWWW>;

struct NameProtocol {
    static constexpr auto name = "protocol";
};
using FunctionProtocol =
        FunctionStringToString<ExtractSubstringImpl<ExtractProtocol>, NameProtocol>;

struct NameTopLevelDomain {
    static constexpr auto name = "top_level_domain";
};
using FunctionTopLevelDomain =
        FunctionStringToString<ExtractSubstringImpl<ExtractTopLevelDomain>, NameTopLevelDomain>;

struct NameFirstSignificantSubdomain {
    static constexpr auto name = "first_significant_subdomain";
};
using FunctionFirstSignificantSubdomain =
        FunctionStringToString<ExtractSubstringImpl<ExtractFirstSignificantSubdomain>,
                               NameFirstSignificantSubdomain>;

struct NameCutToFirstSignificantSubdomain {
    static constexpr auto name = "cut_to_first_significant_subdomain";
};
using FunctionCutToFirstSignificantSubdomain =
        FunctionStringToString<ExtractSubstringImpl<CutToFirstSignificantSubdomain>,
                               NameCutToFirstSignificantSubdomain>;

void register_function_url(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDomain>();
    factory.register_function<FunctionDomainWithoutWWW>();
    factory.register_function<FunctionProtocol>();
    factory.register_function<FunctionTopLevelDomain>();
    factory.register_function<FunctionFirstSignificantSubdomain>();
    factory.register_function<FunctionCutToFirstSignificantSubdomain>();
}

} // namespace doris::vectorized
