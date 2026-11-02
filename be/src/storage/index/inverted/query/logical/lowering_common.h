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

#include <string>
#include <vector>

#include "storage/index/inverted/query/logical/node.h"

namespace doris::index_query::logical::detail {

std::vector<std::string> flatten(std::vector<Token> tokens);
void lower_phrase(Phrase phrase, std::vector<Token> tokens, Node* out);
void lower_phrase_prefix(std::vector<Token> tokens, Node* out);
void lower_phrase_edge(std::vector<Token> tokens, Node* out);

} // namespace doris::index_query::logical::detail
