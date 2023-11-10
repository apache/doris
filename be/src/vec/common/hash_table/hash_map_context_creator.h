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

#include "vec/common/hash_table/hash_map_context.h"
#include "vec/common/hash_table/ph_hash_map.h"

namespace doris::vectorized {

template <template <typename, typename, typename> typename HashMap,
          template <typename> typename Hash, typename Key, typename Value, typename Variant>
void get_hash_map_context_fixed(Variant& variant, bool has_nullable_key, const Sizes& key_sizes) {
    if (has_nullable_key) {
        variant.template emplace<MethodKeysFixed<HashMap<Key, Value, Hash<Key>>, true>>(key_sizes);
    } else {
        variant.template emplace<MethodKeysFixed<HashMap<Key, Value, Hash<Key>>>>(key_sizes);
    }
}

template <template <typename, typename, typename> typename HashMap,
          template <typename> typename Hash, typename Value, typename Variant>
void get_hash_map_context_fixed(Variant& variant, size_t size, bool has_nullable_key,
                                const Sizes& key_sizes) {
    if (size <= sizeof(UInt64)) {
        get_hash_map_context_fixed<HashMap, Hash, UInt64, Value>(variant, has_nullable_key,
                                                                 key_sizes);
    } else if (size <= sizeof(UInt128)) {
        get_hash_map_context_fixed<HashMap, Hash, UInt128, Value>(variant, has_nullable_key,
                                                                  key_sizes);
    } else if (size <= sizeof(UInt136)) {
        get_hash_map_context_fixed<HashMap, Hash, UInt136, Value>(variant, has_nullable_key,
                                                                  key_sizes);
    } else if (size <= sizeof(UInt256)) {
        get_hash_map_context_fixed<HashMap, Hash, UInt256, Value>(variant, has_nullable_key,
                                                                  key_sizes);
    }
}

template <template <typename, typename, typename> typename HashMap,
          template <typename> typename Hash, typename Value, typename Variant>
bool try_get_hash_map_context_fixed(Variant& variant, const VExprContextSPtrs& expr_ctxs) {
    Sizes key_sizes;

    bool use_fixed_key = true;
    bool has_null = false;
    size_t key_byte_size = 0;

    for (auto ctx : expr_ctxs) {
        const auto& data_type = ctx->root()->data_type();
        if (!data_type->have_maximum_size_of_value()) {
            use_fixed_key = false;
            break;
        }
        has_null |= data_type->is_nullable();
        key_sizes.emplace_back(data_type->get_maximum_size_of_value_in_memory() -
                               (data_type->is_nullable() ? 1 : 0));
        key_byte_size += key_sizes.back();
    }

    size_t bitmap_size = has_null ? get_bitmap_size(expr_ctxs.size()) : 0;
    if (bitmap_size + key_byte_size > sizeof(UInt256)) {
        use_fixed_key = false;
    }

    if (use_fixed_key) {
        get_hash_map_context_fixed<HashMap, Hash, Value>(variant, bitmap_size + key_byte_size,
                                                         has_null, key_sizes);
    }
    return use_fixed_key;
}

} // namespace doris::vectorized