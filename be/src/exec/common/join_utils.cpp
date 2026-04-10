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

#include "exec/common/join_utils.h"

// join_utils.h now only includes the IJoinHashTableContext interface.
// This cpp needs the full template definitions for JoinHashTableContextHelper
// and JoinHashTableContextImplBase.
#include "exec/common/hash_table/join_hash_table_context_impl.h"

// The following includes provide the full definitions of ProcessHashTableBuild
// and ProcessHashTableProbe, which are only forward-declared in
// join_hash_table_context_impl.h. They are needed here so that explicit
// template instantiation can compile all method bodies.
#include "exec/operator/hashjoin_build_sink.h"
#include "exec/operator/join/process_hash_table_probe.h"

namespace doris {

// ─────────────────────────────────────────────────────────────────────────────
// JoinDataVariants::init() — create JoinHashTableContextHelper<> via unique_ptr.
// ─────────────────────────────────────────────────────────────────────────────
void JoinDataVariants::init(const std::vector<DataTypePtr>& data_types, HashKeyType type) {
    DCHECK(!method) << "JoinDataVariants already initialized";
    switch (type) {
    case HashKeyType::serialized:
        method = std::make_unique<JoinHashTableContextHelper<SerializedHashTableContext>>();
        break;
    case HashKeyType::int8_key:
        method = std::make_unique<JoinHashTableContextHelper<PrimaryTypeHashTableContext<UInt8>>>();
        break;
    case HashKeyType::int16_key:
        method = std::make_unique<JoinHashTableContextHelper<PrimaryTypeHashTableContext<UInt16>>>();
        break;
    case HashKeyType::int32_key:
        method = std::make_unique<JoinHashTableContextHelper<PrimaryTypeHashTableContext<UInt32>>>();
        break;
    case HashKeyType::int64_key:
        method = std::make_unique<JoinHashTableContextHelper<PrimaryTypeHashTableContext<UInt64>>>();
        break;
    case HashKeyType::int128_key:
        method = std::make_unique<JoinHashTableContextHelper<PrimaryTypeHashTableContext<UInt128>>>();
        break;
    case HashKeyType::int256_key:
        method = std::make_unique<JoinHashTableContextHelper<PrimaryTypeHashTableContext<UInt256>>>();
        break;
    case HashKeyType::string_key:
        method = std::make_unique<JoinHashTableContextHelper<MethodOneString>>();
        break;
    case HashKeyType::fixed64:
        method = std::make_unique<JoinHashTableContextHelper<FixedKeyHashTableContext<UInt64>>>(
                get_key_sizes(data_types));
        break;
    case HashKeyType::fixed72:
        method = std::make_unique<JoinHashTableContextHelper<FixedKeyHashTableContext<UInt72>>>(
                get_key_sizes(data_types));
        break;
    case HashKeyType::fixed96:
        method = std::make_unique<JoinHashTableContextHelper<FixedKeyHashTableContext<UInt96>>>(
                get_key_sizes(data_types));
        break;
    case HashKeyType::fixed104:
        method = std::make_unique<JoinHashTableContextHelper<FixedKeyHashTableContext<UInt104>>>(
                get_key_sizes(data_types));
        break;
    case HashKeyType::fixed128:
        method = std::make_unique<JoinHashTableContextHelper<FixedKeyHashTableContext<UInt128>>>(
                get_key_sizes(data_types));
        break;
    case HashKeyType::fixed136:
        method = std::make_unique<JoinHashTableContextHelper<FixedKeyHashTableContext<UInt136>>>(
                get_key_sizes(data_types));
        break;
    case HashKeyType::fixed256:
        method = std::make_unique<JoinHashTableContextHelper<FixedKeyHashTableContext<UInt256>>>(
                get_key_sizes(data_types));
        break;
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "JoinDataVariants meet invalid key type, type={}", type);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// try_convert_to_direct for PrimaryType — needs JoinDataVariants complete type.
//
// Migrated from the original primary_to_direct_mapping() + try_convert_to_direct_mapping().
// DirectPrimaryTypeHashTableContext only exists for UInt8/16/32/64/128 (no UInt256).
// ─────────────────────────────────────────────────────────────────────────────
template <typename T>
bool JoinHashTableContextHelper<PrimaryTypeHashTableContext<T>>::try_convert_to_direct(
        const ColumnRawPtrs& key_columns,
        std::vector<std::shared_ptr<JoinDataVariants>>& all_variants) {
    // UInt256 has no DirectPrimaryTypeHashTableContext variant — skip.
    if constexpr (std::is_same_v<T, UInt256>) {
        return false;
    } else {
        using FieldType = T;
        FieldType max_key = std::numeric_limits<FieldType>::min();
        FieldType min_key = std::numeric_limits<FieldType>::max();

        size_t num_rows = key_columns[0]->size();
        if (key_columns[0]->is_nullable()) {
            const FieldType* input_keys =
                    (FieldType*)assert_cast<const ColumnNullable*>(key_columns[0])
                            ->get_nested_column_ptr()
                            ->get_raw_data()
                            .data;
            const NullMap& null_map =
                    assert_cast<const ColumnNullable*>(key_columns[0])->get_null_map_data();
            // skip first mocked row
            for (size_t i = 1; i < num_rows; i++) {
                if (null_map[i]) {
                    continue;
                }
                max_key = std::max(max_key, input_keys[i]);
                min_key = std::min(min_key, input_keys[i]);
            }
        } else {
            const FieldType* input_keys = (FieldType*)key_columns[0]->get_raw_data().data;
            // skip first mocked row
            for (size_t i = 1; i < num_rows; i++) {
                max_key = std::max(max_key, input_keys[i]);
                min_key = std::min(min_key, input_keys[i]);
            }
        }

        constexpr auto MAX_MAPPING_RANGE = 1 << 23;
        bool allow_direct_mapping =
                (max_key >= min_key && max_key - min_key < MAX_MAPPING_RANGE - 1);
        if (allow_direct_mapping) {
            for (const auto& variant_ptr : all_variants) {
                variant_ptr->method = std::make_unique<
                        JoinHashTableContextHelper<DirectPrimaryTypeHashTableContext<FieldType>>>(
                        max_key, min_key);
            }
            return true;
        }
        return false;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Explicit template instantiation — all 20 concrete hash table context types.
//
// ImplBase must be instantiated BEFORE Helper so that base class members are
// available when the derived class is instantiated.
// ─────────────────────────────────────────────────────────────────────────────
template class JoinHashTableContextImplBase<SerializedHashTableContext>;
template class JoinHashTableContextImplBase<PrimaryTypeHashTableContext<UInt8>>;
template class JoinHashTableContextImplBase<PrimaryTypeHashTableContext<UInt16>>;
template class JoinHashTableContextImplBase<PrimaryTypeHashTableContext<UInt32>>;
template class JoinHashTableContextImplBase<PrimaryTypeHashTableContext<UInt64>>;
template class JoinHashTableContextImplBase<PrimaryTypeHashTableContext<UInt128>>;
template class JoinHashTableContextImplBase<PrimaryTypeHashTableContext<UInt256>>;
template class JoinHashTableContextImplBase<DirectPrimaryTypeHashTableContext<UInt8>>;
template class JoinHashTableContextImplBase<DirectPrimaryTypeHashTableContext<UInt16>>;
template class JoinHashTableContextImplBase<DirectPrimaryTypeHashTableContext<UInt32>>;
template class JoinHashTableContextImplBase<DirectPrimaryTypeHashTableContext<UInt64>>;
template class JoinHashTableContextImplBase<DirectPrimaryTypeHashTableContext<UInt128>>;
template class JoinHashTableContextImplBase<FixedKeyHashTableContext<UInt64>>;
template class JoinHashTableContextImplBase<FixedKeyHashTableContext<UInt72>>;
template class JoinHashTableContextImplBase<FixedKeyHashTableContext<UInt96>>;
template class JoinHashTableContextImplBase<FixedKeyHashTableContext<UInt104>>;
template class JoinHashTableContextImplBase<FixedKeyHashTableContext<UInt128>>;
template class JoinHashTableContextImplBase<FixedKeyHashTableContext<UInt136>>;
template class JoinHashTableContextImplBase<FixedKeyHashTableContext<UInt256>>;
template class JoinHashTableContextImplBase<MethodOneString>;

template class JoinHashTableContextHelper<SerializedHashTableContext>;
template class JoinHashTableContextHelper<PrimaryTypeHashTableContext<UInt8>>;
template class JoinHashTableContextHelper<PrimaryTypeHashTableContext<UInt16>>;
template class JoinHashTableContextHelper<PrimaryTypeHashTableContext<UInt32>>;
template class JoinHashTableContextHelper<PrimaryTypeHashTableContext<UInt64>>;
template class JoinHashTableContextHelper<PrimaryTypeHashTableContext<UInt128>>;
template class JoinHashTableContextHelper<PrimaryTypeHashTableContext<UInt256>>;
template class JoinHashTableContextHelper<DirectPrimaryTypeHashTableContext<UInt8>>;
template class JoinHashTableContextHelper<DirectPrimaryTypeHashTableContext<UInt16>>;
template class JoinHashTableContextHelper<DirectPrimaryTypeHashTableContext<UInt32>>;
template class JoinHashTableContextHelper<DirectPrimaryTypeHashTableContext<UInt64>>;
template class JoinHashTableContextHelper<DirectPrimaryTypeHashTableContext<UInt128>>;
template class JoinHashTableContextHelper<FixedKeyHashTableContext<UInt64>>;
template class JoinHashTableContextHelper<FixedKeyHashTableContext<UInt72>>;
template class JoinHashTableContextHelper<FixedKeyHashTableContext<UInt96>>;
template class JoinHashTableContextHelper<FixedKeyHashTableContext<UInt104>>;
template class JoinHashTableContextHelper<FixedKeyHashTableContext<UInt128>>;
template class JoinHashTableContextHelper<FixedKeyHashTableContext<UInt136>>;
template class JoinHashTableContextHelper<FixedKeyHashTableContext<UInt256>>;
template class JoinHashTableContextHelper<MethodOneString>;

} // namespace doris
