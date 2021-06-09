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

#include <memory>

#include "common/logging.h"
#include "common/status.h"
#include "gutil/ref_counted.h"
#include "gutil/stringprintf.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/types.h"
#include "util/time.h"

namespace doris {
namespace memory {

template <class T, class ST>
inline T padding(T v, ST pad) {
    return (v + pad - 1) / pad * pad;
}

template <class T, class ST>
inline size_t num_block(T v, ST bs) {
    return (v + bs - 1) / bs;
}

} // namespace memory
} // namespace doris
