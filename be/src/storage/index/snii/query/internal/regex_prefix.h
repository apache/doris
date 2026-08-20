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

#include <re2/re2.h>

#include <string>
#include <string_view>

namespace doris::snii::query::internal {

// Computes the dictionary-enumeration prefix used to narrow regexp term
// expansion. For left-anchored ("^") patterns it derives a tight common prefix
// from RE2::PossibleMatchRange (e.g. "^(order)" -> "order", where a naive literal
// scan stops at '(' and yields ""); if RE2 cannot compile the pattern, it falls
// back to a conservative literal-prefix scan. Unanchored patterns return no
// prefix because Hyperscan may match after the beginning of a dictionary term.
// The returned prefix only bounds how many terms visit_prefix_terms enumerates;
// final term acceptance is always decided by Hyperscan.
std::string regex_enum_prefix(std::string_view pattern, const re2::RE2& re);

} // namespace doris::snii::query::internal
