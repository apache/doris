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

#include <functional>

#include "common/object_pool.h"
#include "exec/exec_node.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash_map.h"
#include "vec/exprs/vectorized_agg_fn.h"

namespace doris {
class TPlanNode;
class DescriptorTbl;
class MemPool;

namespace vectorized {
class VExprContext;

/** Aggregates by concatenating serialized key values.
  * The serialized value differs in that it uniquely allows to deserialize it, having only the position with which it starts.
  * That is, for example, for strings, it contains first the serialized length of the string, and then the bytes.
  * Therefore, when aggregating by several strings, there is no ambiguity.
  */
template <typename TData>
struct AggregationMethodSerialized {
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using Iterator = typename Data::iterator;

    Data data;
    Iterator iterator;
    bool inited = false;

    AggregationMethodSerialized() = default;

    template <typename Other>
    explicit AggregationMethodSerialized(const Other& other) : data(other.data) {}

    using State = ColumnsHashing::HashMethodSerialized<typename Data::value_type, Mapped>;

    static const bool low_cardinality_optimization = false;

    static void insertKeyIntoColumns(const StringRef& key, MutableColumns& key_columns,
                                     const Sizes&) {
        auto pos = key.data;
        for (auto& column : key_columns) pos = column->deserialize_and_insert_from_arena(pos);
    }

    void init_once() {
        if (!inited) {
            inited = true;
            iterator = data.begin();
        }
    }
};

using AggregatedDataWithoutKey = AggregateDataPtr;
using AggregatedDataWithStringKey = HashMapWithSavedHash<StringRef, AggregateDataPtr>;

struct AggregatedDataVariants {
    AggregatedDataVariants() = default;
    AggregatedDataVariants(const AggregatedDataVariants&) = delete;
    AggregatedDataVariants& operator=(const AggregatedDataVariants&) = delete;
    AggregatedDataWithoutKey without_key = nullptr;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKey>> serialized;
    enum class Type {
        EMPTY = 0,
        without_key,
        serialized
        // TODO: add more key optimize types
    };

    Type _type = Type::EMPTY;

    void init(Type type) {
        _type = type;
        switch (_type) {
        case Type::serialized:
            serialized.reset(new AggregationMethodSerialized<AggregatedDataWithStringKey>());
            break;
        default:
            break;
        }
    }
};

using AggregatedDataVariantsPtr = std::shared_ptr<AggregatedDataVariants>;

// not support spill
class AggregationNode : public ::doris::ExecNode {
public:
    using Sizes = std::vector<size_t>;

    AggregationNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~AggregationNode();
    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status get_next(RuntimeState* state, Block* block, bool* eos);
    virtual Status close(RuntimeState* state);

private:
    // group by k1,k2
    std::vector<VExprContext*> _probe_expr_ctxs;

    std::vector<AggFnEvaluator*> _aggregate_evaluators;

    // may be we don't have to know the tuple id
    TupleId _intermediate_tuple_id;
    TupleDescriptor* _intermediate_tuple_desc;

    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc;

    bool _needs_finalize;
    bool _is_merge;
    std::unique_ptr<MemPool> _mem_pool;

    size_t _align_aggregate_states = 1;
    /// The offset to the n-th aggregate function in a row of aggregate functions.
    Sizes _offsets_of_aggregate_states;
    /// The total size of the row from the aggregate functions.
    size_t _total_size_of_aggregate_states = 0;

    AggregatedDataVariants _agg_data;

    Arena _agg_arena_pool;

    RuntimeProfile::Counter* _build_timer;
    RuntimeProfile::Counter* _exec_timer;
    RuntimeProfile::Counter* _merge_timer;
    RuntimeProfile::Counter* _expr_timer;

    bool _is_streaming_preagg;
    bool _should_expand_hash_table = true;
    char* _streaming_pre_agg_buffer = nullptr;
    size_t _max_size_of_stream_pre_agg_buffer = 0;

    /// Expose the minimum reduction factor to continue growing the hash tables.
    RuntimeProfile::Counter* preagg_streaming_ht_min_reduction_;

private:
    /// Return true if we should keep expanding hash tables in the preagg. If false,
    /// the preagg should pass through any rows it can't fit in its tables.
    bool _should_expand_preagg_hash_tables();

    Status _create_agg_status(AggregateDataPtr data);
    Status _destory_agg_status(AggregateDataPtr data);

    Status _get_without_key_result(RuntimeState* state, Block* block, bool* eos);
    Status _serialize_without_key(RuntimeState* state, Block* block, bool* eos);
    Status _execute_without_key(Block* block);
    Status _merge_without_key(Block* block);
    void _update_memusage_without_key();
    void _close_without_key();

    Status _get_with_serialized_key_result(RuntimeState* state, Block* block, bool* eos);
    Status _serialize_with_serialized_key_result(RuntimeState* state, Block* block, bool* eos);
    Status _pre_agg_with_serialized_key(Block* in_block, Block* out_block);
    Status _execute_with_serialized_key(Block* block);
    Status _merge_with_serialized_key(Block* block);
    void _update_memusage_with_serialized_key();
    void _close_with_serialized_key();

    void release_tracker();

    using vectorized_execute = std::function<Status(Block* block)>;
    using vectorized_pre_agg = std::function<Status(Block* in_block, Block* out_block)>;
    using vectorized_get_result =
            std::function<Status(RuntimeState* state, Block* block, bool* eos)>;
    using vectorized_closer = std::function<void()>;
    using vectorized_update_memusage = std::function<void()>;

    struct executor {
        vectorized_execute execute;
        vectorized_pre_agg pre_agg;
        vectorized_get_result get_result;
        vectorized_closer close;
        vectorized_update_memusage update_memusage;
    };

    executor _executor;

    struct MemoryRecord {
        MemoryRecord() : used_in_arena(0), used_in_state(0) {}
        int64_t used_in_arena;
        int64_t used_in_state;
    };

    MemoryRecord _mem_usage_record;
};
} // namespace vectorized
} // namespace doris
