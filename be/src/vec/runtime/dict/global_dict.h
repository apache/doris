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

#include "vec/runtime/dict/dict.h"
#include "vec/core/columns_with_type_and_name.h"

namespace doris {
namespace vectorized {
class GlobalDict : public Dict<int16> {
public:
    GlobalDict(const std::vector<std::string>& data);

    //int column to string column
    bool decode(ColumnWithTypeAndName& col);

    int id() { return _id; };

    size_t version() { return _version; };

private:
    // dict version
    size_t _version = 0;
    int _id = -1;
    std::vector<std::string> _dict_data;
};

using GlobalDictSPtr = std::shared_ptr<GlobalDict>;

} //namespace vectorized
} //namespace doris