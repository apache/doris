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

#ifndef __cplusplus
#error need cpp support
#endif

#if __cplusplus <= 202002L
#include "util/expected.hpp"
namespace doris {
using tl::expected;            // NOLINT
using tl::unexpected;          // NOLINT
using tl::bad_expected_access; // NOLINT
using tl::unexpect_t;          // NOLINT
using tl::unexpect;            // NOLINT
} // namespace doris
#else
#include <expected>
namespace doris {
using std::expected;            // NOLINT
using std::unexpected;          // NOLINT
using std::bad_expected_access; // NOLINT
using std::unexpect_t;          // NOLINT
using std::unexpect;            // NOLINT
} // namespace doris
#endif
