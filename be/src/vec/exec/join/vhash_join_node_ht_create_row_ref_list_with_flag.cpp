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

#include "gen_cpp/PlanNodes_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_filter_mgr.h"
#include "util/defer_op.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/template_helpers.hpp"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
void HashJoinNode::_hash_table_create_row_ref_list_with_flag() {
    if (_build_expr_ctxs.size() == 1 && !_store_null_in_hash_table[0]) {
        // Single column optimization
        switch (_build_expr_ctxs[0]->root()->result_type()) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
            _hash_table_variants.emplace<I8HashTableContextRowRefListWithFlag>();
            break;
        case TYPE_SMALLINT:
            _hash_table_variants.emplace<I16HashTableContextRowRefListWithFlag>();
            break;
        case TYPE_INT:
        case TYPE_FLOAT:
        case TYPE_DATEV2:
            _hash_table_variants.emplace<I32HashTableContextRowRefListWithFlag>();
            break;
        case TYPE_BIGINT:
        case TYPE_DOUBLE:
        case TYPE_DATETIME:
        case TYPE_DATE:
        case TYPE_DATETIMEV2:
            _hash_table_variants.emplace<I64HashTableContextRowRefListWithFlag>();
            break;
        case TYPE_LARGEINT:
        case TYPE_DECIMALV2:
        case TYPE_DECIMAL32:
        case TYPE_DECIMAL64:
        case TYPE_DECIMAL128: {
            DataTypePtr& type_ptr = _build_expr_ctxs[0]->root()->data_type();
            TypeIndex idx = _build_expr_ctxs[0]->root()->is_nullable()
                                    ? assert_cast<const DataTypeNullable&>(*type_ptr)
                                              .get_nested_type()
                                              ->get_type_id()
                                    : type_ptr->get_type_id();
            WhichDataType which(idx);
            if (which.is_decimal32()) {
                _hash_table_variants.emplace<I32HashTableContextRowRefListWithFlag>();
            } else if (which.is_decimal64()) {
                _hash_table_variants.emplace<I64HashTableContextRowRefListWithFlag>();
            } else {
                _hash_table_variants.emplace<I128HashTableContextRowRefListWithFlag>();
            }
            break;
        }
        default:
            _hash_table_variants.emplace<SerializedHashTableContextRowRefListWithFlag>();
        }
        return;
    }

    bool use_fixed_key = true;
    bool has_null = false;
    int key_byte_size = 0;

    _probe_key_sz.resize(_probe_expr_ctxs.size());
    _build_key_sz.resize(_build_expr_ctxs.size());

    for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
        const auto vexpr = _build_expr_ctxs[i]->root();
        const auto& data_type = vexpr->data_type();

        if (!data_type->have_maximum_size_of_value()) {
            use_fixed_key = false;
            break;
        }

        auto is_null = data_type->is_nullable();
        has_null |= is_null;
        _build_key_sz[i] = data_type->get_maximum_size_of_value_in_memory() - (is_null ? 1 : 0);
        _probe_key_sz[i] = _build_key_sz[i];
        key_byte_size += _probe_key_sz[i];
    }

    if (std::tuple_size<KeysNullMap<UInt256>>::value + key_byte_size > sizeof(UInt256)) {
        use_fixed_key = false;
    }

    if (use_fixed_key) {
        // TODO: may we should support uint256 in the future
        if (has_null) {
            if (std::tuple_size<KeysNullMap<UInt64>>::value + key_byte_size <= sizeof(UInt64)) {
                _hash_table_variants.emplace<I64FixedKeyHashTableContextRowRefListWithFlag<true>>();
            } else if (std::tuple_size<KeysNullMap<UInt128>>::value + key_byte_size <=
                       sizeof(UInt128)) {
                _hash_table_variants
                        .emplace<I128FixedKeyHashTableContextRowRefListWithFlag<true>>();
            } else {
                _hash_table_variants
                        .emplace<I256FixedKeyHashTableContextRowRefListWithFlag<true>>();
            }
        } else {
            if (key_byte_size <= sizeof(UInt64)) {
                _hash_table_variants
                        .emplace<I64FixedKeyHashTableContextRowRefListWithFlag<false>>();
            } else if (key_byte_size <= sizeof(UInt128)) {
                _hash_table_variants
                        .emplace<I128FixedKeyHashTableContextRowRefListWithFlag<false>>();
            } else {
                _hash_table_variants
                        .emplace<I256FixedKeyHashTableContextRowRefListWithFlag<false>>();
            }
        }
    } else {
        _hash_table_variants.emplace<SerializedHashTableContextRowRefListWithFlag>();
    }
}

} // namespace doris::vectorized