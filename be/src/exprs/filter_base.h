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

#include "common/exception.h"

namespace doris {

class FilterBase {
public:
    FilterBase(bool null_aware) : _null_aware(null_aware) {}
    bool contain_null() const { return _null_aware && _contain_null; }

    void set_contain_null(bool contain_null) {
        if (_contain_null && !contain_null) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "contain_null cannot be changed from true to false");
        }
        _contain_null = contain_null;
    }

protected:
    // Indicates whether a null datum exists to build this filter.
    bool _contain_null = false;
    // Indicates whether this filter is null-aware.
    const bool _null_aware = false;
};

} // namespace doris
