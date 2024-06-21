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

#include <variant>
#include <vector>

#include "vec/common/hash_table/hash_map_context_creator.h"
#include "vec/common/hash_table/hash_map_util.h"

namespace doris::pipeline {
using JoinOpVariants =
        std::variant<std::integral_constant<TJoinOp::type, TJoinOp::INNER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::LEFT_SEMI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::LEFT_ANTI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::LEFT_OUTER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::FULL_OUTER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_OUTER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::CROSS_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_SEMI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_ANTI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN>>;

using I8HashTableContext = vectorized::PrimaryTypeHashTableContext<vectorized::UInt8>;
using I16HashTableContext = vectorized::PrimaryTypeHashTableContext<vectorized::UInt16>;
using I32HashTableContext = vectorized::PrimaryTypeHashTableContext<vectorized::UInt32>;
using I64HashTableContext = vectorized::PrimaryTypeHashTableContext<UInt64>;
using I128HashTableContext = vectorized::PrimaryTypeHashTableContext<UInt128>;
using I256HashTableContext = vectorized::PrimaryTypeHashTableContext<UInt256>;

template <bool has_null>
using I64FixedKeyHashTableContext = vectorized::FixedKeyHashTableContext<UInt64, has_null>;

template <bool has_null>
using I128FixedKeyHashTableContext = vectorized::FixedKeyHashTableContext<UInt128, has_null>;

template <bool has_null>
using I256FixedKeyHashTableContext = vectorized::FixedKeyHashTableContext<UInt256, has_null>;

template <bool has_null>
using I136FixedKeyHashTableContext = vectorized::FixedKeyHashTableContext<UInt136, has_null>;

using HashTableVariants =
        std::variant<std::monostate, vectorized::SerializedHashTableContext, I8HashTableContext,
                     I16HashTableContext, I32HashTableContext, I64HashTableContext,
                     I128HashTableContext, I256HashTableContext, I64FixedKeyHashTableContext<true>,
                     I64FixedKeyHashTableContext<false>, I128FixedKeyHashTableContext<true>,
                     I128FixedKeyHashTableContext<false>, I256FixedKeyHashTableContext<true>,
                     I256FixedKeyHashTableContext<false>, I136FixedKeyHashTableContext<true>,
                     I136FixedKeyHashTableContext<false>>;

} // namespace doris::pipeline
