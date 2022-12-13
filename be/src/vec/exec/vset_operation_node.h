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

#include "exec/exec_node.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "vec/core/materialize_block.h"
#include "vec/exec/join/join_op.h"
#include "vec/exec/join/vacquire_list.hpp"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/functions/function.h"
#include "vec/utils/util.hpp"

namespace doris {

namespace vectorized {

template <bool is_intersect>
class VSetOperationNode final : public ExecNode {
public:
    VSetOperationNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* /*state*/, RowBatch* /*row_batch*/, bool* /*eos*/) override {
        return Status::NotSupported("Not implemented get RowBatch in vectorized execution.");
    }

    Status get_next(RuntimeState* state, Block* output_block, bool* eos) override;

    Status close(RuntimeState* state) override;
    void debug_string(int indentation_level, std::stringstream* out) const override;

    Status alloc_resource(RuntimeState* state) override;
    void release_resource(RuntimeState* state) override;

    Status sink(RuntimeState* state, Block* block, bool eos) override;
    Status pull(RuntimeState* state, Block* output_block, bool* eos) override;

    Status sink_probe(RuntimeState* state, int child_id, Block* block, bool eos);
    Status finalize_probe(RuntimeState* state, int child_id);

    bool is_child_finished(int child_id) const;

private:
    //Todo: In build process of hashtable, It's same as join node.
    //It's time to abstract out the same methods and provide them directly to others;
    void hash_table_init();
    Status hash_table_build(RuntimeState* state);
    Status process_build_block(Block& block, uint8_t offset);
    Status extract_build_column(Block& block, ColumnRawPtrs& raw_ptrs);
    Status extract_probe_column(Block& block, ColumnRawPtrs& raw_ptrs, int child_id);
    void refresh_hash_table();

    template <typename HashTableContext>
    Status get_data_in_hashtable(HashTableContext& hash_table_ctx, Block* output_block,
                                 const int batch_size, bool* eos);

    void add_result_columns(RowRefListWithFlags& value, int& block_size);

    void create_mutable_cols(Block* output_block);
    void release_mem();

    std::unique_ptr<HashTableVariants> _hash_table_variants;

    std::vector<size_t> _probe_key_sz;
    std::vector<size_t> _build_key_sz;
    std::vector<bool> _build_not_ignore_null;

    std::unique_ptr<Arena> _arena;
    //record element size in hashtable
    int64_t _valid_element_in_hash_tbl;

    //The i-th result expr list refers to the i-th child.
    std::vector<std::vector<VExprContext*>> _child_expr_lists;
    //record build column type
    DataTypes _left_table_data_types;
    //first:column_id, could point to origin column or cast column
    //second:idx mapped to column types
    std::unordered_map<int, int> _build_col_idx;
    //record memory during running
    int64_t _mem_used;
    //record insert column id during probe
    std::vector<uint16_t> _probe_column_inserted_id;

    std::vector<Block> _build_blocks;
    Block _probe_block;
    ColumnRawPtrs _probe_columns;
    std::vector<MutableColumnPtr> _mutable_cols;
    int _build_block_index;
    bool _build_finished;
    std::vector<bool> _probe_finished_children_index;
    MutableBlock _mutable_block;
    RuntimeProfile::Counter* _build_timer; // time to build hash table
    RuntimeProfile::Counter* _probe_timer; // time to probe

    template <class HashTableContext, bool is_intersected>
    friend struct HashTableBuild;
    template <class HashTableContext, bool is_intersected>
    friend struct HashTableProbe;
};

using VIntersectNode = VSetOperationNode<true>;
using VExceptNode = VSetOperationNode<false>;

} // namespace vectorized
} // namespace doris
