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

#include "storage/index/inverted/analyzer/analyzer_provider.h"

// gram_scheme.h 只在这个 .cpp 里出现完整定义：analyzer_provider.h 为了不把
// exec/pipeline/dependency.h 的前向包含闭包挤过预算，只前置声明了 GramScheme
// （见 analyzer_provider.h 顶部注释）。gram_scheme() 的默认实现需要构造一个空的
// std::optional<GramScheme>，这要求完整类型，因此挪到这里、只在真正编译这个
// 翻译单元时才需要该完整定义。
#include "storage/index/inverted/gram/gram_scheme.h"

namespace doris::segment_v2::inverted_index {

std::optional<gram::GramScheme> AnalyzerProvider::gram_scheme() const {
    return std::nullopt;
}

} // namespace doris::segment_v2::inverted_index
