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

#include "vec/common/typeid_cast.h"

namespace doris::vectorized {

class IDataType;

template <typename... Ts, typename F>
static bool cast_type_to_either(const IDataType* type, F&& f) {
    /// XXX can't use && here because gcc-7 complains about parentheses around && within ||
    return ((typeid_cast<const Ts*>(type) ? f(*typeid_cast<const Ts*>(type)) : false) || ...);
}

} // namespace doris::vectorized
