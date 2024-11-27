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

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <unordered_map>
#include <variant>
#include <vector>

#include "common/status.h"
#include "exec/exec_node.h"
#include "util/runtime_profile.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/common/arena.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/exec/join/process_hash_table_probe.h"
#include "vec/exec/join/vhash_join_node.h"

namespace doris {
class DescriptorTbl;
class ObjectPool;
class RuntimeState;
class TPlanNode;

namespace vectorized {
class VExprContext;
struct RowRefListWithFlags;

using SetHashTableVariants = std::variant<
        std::monostate, MethodSerialized<HashMap<StringRef, RowRefListWithFlags>>,
        SetPrimaryTypeHashTableContext<UInt8>, SetPrimaryTypeHashTableContext<UInt16>,
        SetPrimaryTypeHashTableContext<UInt32>, SetPrimaryTypeHashTableContext<UInt64>,
        SetPrimaryTypeHashTableContext<UInt128>, SetPrimaryTypeHashTableContext<UInt256>,
        SetFixedKeyHashTableContext<UInt64, true>, SetFixedKeyHashTableContext<UInt64, false>,
        SetFixedKeyHashTableContext<UInt128, true>, SetFixedKeyHashTableContext<UInt128, false>,
        SetFixedKeyHashTableContext<UInt256, true>, SetFixedKeyHashTableContext<UInt256, false>,
        SetFixedKeyHashTableContext<UInt136, true>, SetFixedKeyHashTableContext<UInt136, false>>;

template <bool is_intersect>
class VSetOperationNode final : public ExecNode {
public:
    VSetOperationNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status get_next(RuntimeState* state, Block* output_block, bool* eos) override;

    Status close(RuntimeState* state) override;
    void debug_string(int indentation_level, std::stringstream* out) const override;

    Status alloc_resource(RuntimeState* state) override;
    void release_resource(RuntimeState* state) override;

    Status sink(RuntimeState* state, Block* block, bool eos) override;
    Status pull(RuntimeState* state, Block* output_block, bool* eos) override;

    Status sink_probe(RuntimeState* state, int child_id, Block* block, bool eos);

    bool is_child_finished(int child_id) const;

    int64_t* valid_element_in_hash_tbl() { return &_valid_element_in_hash_tbl; }

private:
    void _finalize_probe(int child_id);
    //Todo: In build process of hashtable, It's same as join node.
    //It's time to abstract out the same methods and provide them directly to others;
    void hash_table_init();
    Status hash_table_build(RuntimeState* state);
    Status process_build_block(Block& block, RuntimeState* state);
    Status extract_build_column(Block& block, ColumnRawPtrs& raw_ptrs);
    Status extract_probe_column(Block& block, ColumnRawPtrs& raw_ptrs, int child_id);
    void refresh_hash_table();

    template <typename HashTableContext>
    Status get_data_in_hashtable(HashTableContext& hash_table_ctx, Block* output_block,
                                 const int batch_size, bool* eos);

    void add_result_columns(RowRefListWithFlags& value, int& block_size);

    void create_mutable_cols(Block* output_block);
    void release_mem();

    std::unique_ptr<SetHashTableVariants> _hash_table_variants;

    std::vector<bool> _build_not_ignore_null;

    //record element size in hashtable
    int64_t _valid_element_in_hash_tbl;

    //The i-th result expr list refers to the i-th child.
    std::vector<VExprContextSPtrs> _child_expr_lists;
    //record build column type
    DataTypes _left_table_data_types;
    //first: idx mapped to column types
    //second: column_id, could point to origin column or cast column
    std::unordered_map<int, int> _build_col_idx;
    //record insert column id during probe
    std::vector<uint16_t> _probe_column_inserted_id;

    Block _build_block;
    Block _probe_block;
    ColumnRawPtrs _probe_columns;
    std::vector<MutableColumnPtr> _mutable_cols;
    bool _build_finished;
    std::vector<bool> _probe_finished_children_index;
    MutableBlock _mutable_block;
    RuntimeProfile::Counter* _build_timer = nullptr; // time to build hash table
    RuntimeProfile::Counter* _probe_timer = nullptr; // time to probe
    RuntimeProfile::Counter* _pull_timer = nullptr;  // time to pull data
    Arena _arena;

    template <class HashTableContext, bool is_intersected>
    friend struct HashTableBuild;
    template <class HashTableContext, bool is_intersected>
    friend struct HashTableProbe;
};

using VIntersectNode = VSetOperationNode<true>;
using VExceptNode = VSetOperationNode<false>;

} // namespace vectorized
} // namespace doris
