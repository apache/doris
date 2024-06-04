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

#pragma once

#include <CLucene.h>
#include <CLucene/index/IndexReader.h>

#include <cstdint>

CL_NS_USE(index)

namespace doris::segment_v2 {

class PrefixQuery {
public:
    PrefixQuery() = default;
    virtual ~PrefixQuery() = default;

    static void get_prefix_terms(IndexReader* reader, const std::wstring& field_name,
                                 const std::string& prefix,
                                 std::vector<CL_NS(index)::Term*>& prefix_terms,
                                 int32_t max_expansions = 50);
};

} // namespace doris::segment_v2