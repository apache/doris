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

#include <exception>

#include "vec/common/hash_table/hash_map_context.h"
#include "vec/common/hash_table/ph_hash_map.h"

namespace doris {
using vectorized::MethodKeysFixed;
using vectorized::Sizes;
using vectorized::UInt64;
using vectorized::UInt128;
using vectorized::UInt136;
using vectorized::UInt256;
using vectorized::DataTypePtr;
using vectorized::VExprContextSPtrs;
using vectorized::DataTypePtr;

template <template <typename...> typename HashMap, template <typename> typename Hash, typename Key,
          typename... Mapped, typename Variant>
void get_hash_map_context_fixed(Variant& variant, bool has_nullable_key, const Sizes& key_sizes) {
    if (has_nullable_key) {
        variant.template emplace<MethodKeysFixed<HashMap<Key, Mapped..., Hash<Key>>, true>>(
                key_sizes);
    } else {
        variant.template emplace<MethodKeysFixed<HashMap<Key, Mapped..., Hash<Key>>>>(key_sizes);
    }
}

template <template <typename... Args> typename HashMap, template <typename> typename Hash,
          typename... Mapped, typename Variant>
void get_hash_map_context_fixed(Variant& variant, size_t size, bool has_nullable_key,
                                const Sizes& key_sizes) {
    if (size <= sizeof(UInt64)) {
        get_hash_map_context_fixed<HashMap, Hash, UInt64, Mapped...>(variant, has_nullable_key,
                                                                     key_sizes);
    } else if (size <= sizeof(UInt128)) {
        get_hash_map_context_fixed<HashMap, Hash, UInt128, Mapped...>(variant, has_nullable_key,
                                                                      key_sizes);
    } else if (size <= sizeof(UInt136)) {
        get_hash_map_context_fixed<HashMap, Hash, UInt136, Mapped...>(variant, has_nullable_key,
                                                                      key_sizes);
    } else if (size <= sizeof(UInt256)) {
        get_hash_map_context_fixed<HashMap, Hash, UInt256, Mapped...>(variant, has_nullable_key,
                                                                      key_sizes);
    }
}

template <template <typename... Args> typename HashMap, template <typename> typename Hash,
          typename... Mapped, typename Variant>
bool try_get_hash_map_context_fixed(Variant& variant, const VExprContextSPtrs& expr_ctxs) {
    std::vector<DataTypePtr> data_types;
    for (const auto& ctx : expr_ctxs) {
        data_types.emplace_back(ctx->root()->data_type());
    }
    return try_get_hash_map_context_fixed<HashMap, Hash, Mapped...>(variant, data_types);
}

template <template <typename... Args> typename HashMap, template <typename> typename Hash,
          typename... Mapped, typename Variant>
bool try_get_hash_map_context_fixed(Variant& variant, const std::vector<DataTypePtr>& data_types) {
    Sizes key_sizes;

    bool use_fixed_key = true;
    bool has_null = false;
    size_t key_byte_size = 0;

    for (const auto& data_type : data_types) {
        if (!data_type->have_maximum_size_of_value()) {
            use_fixed_key = false;
            break;
        }
        has_null |= data_type->is_nullable();
        key_sizes.emplace_back(data_type->get_maximum_size_of_value_in_memory() -
                               (data_type->is_nullable() ? 1 : 0));
        key_byte_size += key_sizes.back();
    }

    size_t bitmap_size = has_null ? vectorized::get_bitmap_size(data_types.size()) : 0;
    if (bitmap_size + key_byte_size > sizeof(UInt256)) {
        use_fixed_key = false;
    }

    if (use_fixed_key) {
        get_hash_map_context_fixed<HashMap, Hash, Mapped...>(variant, bitmap_size + key_byte_size,
                                                             has_null, key_sizes);
    }
    return use_fixed_key;
}

template <typename DataVariants, typename Data>
Status init_hash_method(DataVariants* agg_data, const vectorized::VExprContextSPtrs& probe_exprs,
                        bool is_first_phase) {
    using Type = DataVariants::Type;
    Type t(Type::serialized);

    try {
        if (probe_exprs.size() == 1) {
            auto is_nullable = probe_exprs[0]->root()->is_nullable();
            PrimitiveType type = probe_exprs[0]->root()->result_type();
            switch (type) {
            case TYPE_TINYINT:
            case TYPE_BOOLEAN:
            case TYPE_SMALLINT:
            case TYPE_INT:
            case TYPE_FLOAT:
            case TYPE_DATEV2:
            case TYPE_BIGINT:
            case TYPE_DOUBLE:
            case TYPE_DATE:
            case TYPE_DATETIME:
            case TYPE_DATETIMEV2:
            case TYPE_LARGEINT:
            case TYPE_DECIMALV2:
            case TYPE_DECIMAL32:
            case TYPE_DECIMAL64:
            case TYPE_DECIMAL128I: {
                size_t size = get_primitive_type_size(type);
                if (size == 1) {
                    t = Type::int8_key;
                } else if (size == 2) {
                    t = Type::int16_key;
                } else if (size == 4) {
                    t = Type::int32_key;
                } else if (size == 8) {
                    t = Type::int64_key;
                } else if (size == 16) {
                    t = Type::int128_key;
                } else {
                    throw Exception(ErrorCode::INTERNAL_ERROR,
                                    "meet invalid type size, size={}, type={}", size,
                                    type_to_string(type));
                }
                break;
            }
            case TYPE_CHAR:
            case TYPE_VARCHAR:
            case TYPE_STRING: {
                t = Type::string_key;
                break;
            }
            default:
                t = Type::serialized;
            }

            agg_data->init(get_hash_key_type_with_phase(t, !is_first_phase), is_nullable);
        } else {
            if (!try_get_hash_map_context_fixed<PHNormalHashMap, HashCRC32, Data>(
                        agg_data->method_variant, probe_exprs)) {
                agg_data->init(Type::serialized);
            }
        }
    } catch (const Exception& e) {
        // method_variant may meet valueless_by_exception, so we set it to monostate
        agg_data->method_variant.template emplace<std::monostate>();
        return e.to_status();
    }

    CHECK(!agg_data->method_variant.valueless_by_exception());

    if (agg_data->method_variant.index() == 0) { // index is 0 means variant is monostate
        return Status::InternalError("agg_data->method_variant init failed");
    }
    return Status::OK();
}
} // namespace doris