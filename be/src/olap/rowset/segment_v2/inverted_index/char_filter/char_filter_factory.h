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

#include "olap/rowset/segment_v2/inverted_index/char_filter/char_replace_char_filter.h"

namespace doris {

static const std::string INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE = "char_replace";

class CharFilterFactory {
public:
    template <typename... Args>
    static lucene::analysis::CharFilter* create(const std::string& name, Args&&... args) {
        DBUG_EXECUTE_IF("CharFilterFactory::create_return_nullptr", { return nullptr; })
        if (name == INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE) {
            return new CharReplaceCharFilter(std::forward<Args>(args)...);
        }
        return nullptr;
    }
};

} // namespace doris