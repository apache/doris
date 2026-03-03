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

#include "common/status.h"
#include "udf/python/python_env.h"
#include "udf/python/python_udaf_client.h"
#include "udf/python/python_udf_meta.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

/**
 * Aggregate state data for Python UDAF
 * 
 * Python UDAF state is managed remotely (in Python server).
 * We cache serialized state for shuffle/merge operations (similar to Java UDAF).
 */
struct AggregatePythonUDAFData {
    std::string serialize_data;
    PythonUDAFClientPtr client;

    AggregatePythonUDAFData() = default;

    AggregatePythonUDAFData(const AggregatePythonUDAFData& other)
            : serialize_data(other.serialize_data), client(other.client) {}

    ~AggregatePythonUDAFData() = default;

    Status create(int64_t place);

    Status add(int64_t place_id, const IColumn** columns, int64_t row_num_start,
               int64_t row_num_end, const DataTypes& argument_types);

    Status add_batch(AggregateDataPtr* places, size_t place_offset, size_t num_rows,
                     const IColumn** columns, const DataTypes& argument_types, size_t start,
                     size_t end);

    Status merge(const AggregatePythonUDAFData& rhs, int64_t place);

    Status write(BufferWritable& buf, int64_t place) const;

    void read(BufferReadable& buf);

    Status reset(int64_t place);

    Status destroy(int64_t place);

    Status get(IColumn& to, const DataTypePtr& result_type, int64_t place) const;
};

/**
 * Python UDAF Aggregate Function
 * 
 * Implements Snowflake-style UDAF pattern:
 * - __init__(): Initialize aggregate state
 * - aggregate_state: Property returning serializable state
 * - accumulate(*args): Add input to state
 * - merge(other_state): Combine two states
 * - finish(): Get final result
 * 
 * Communication with Python server via PythonUDAFClient using Arrow Flight.
 */
class AggregatePythonUDAF final
        : public IAggregateFunctionDataHelper<AggregatePythonUDAFData, AggregatePythonUDAF>,
          VarargsExpression,
          NullableAggregateFunction {
public:
    ENABLE_FACTORY_CREATOR(AggregatePythonUDAF);

    AggregatePythonUDAF(const TFunction& fn, const DataTypes& argument_types_,
                        const DataTypePtr& return_type)
            : IAggregateFunctionDataHelper(argument_types_), _fn(fn), _return_type(return_type) {}

    ~AggregatePythonUDAF() override = default;

    static AggregateFunctionPtr create(const TFunction& fn, const DataTypes& argument_types_,
                                       const DataTypePtr& return_type) {
        return std::make_shared<AggregatePythonUDAF>(fn, argument_types_, return_type);
    }

    String get_name() const override { return _fn.name.function_name; }

    DataTypePtr get_return_type() const override { return _return_type; }

    /**
     * Initialize function metadata
     */
    Status open();

    /**
     * Create aggregate state in Python server
     */
    void create(AggregateDataPtr __restrict place) const override;

    /**
     * Destroy aggregate state in Python server
     */
    void destroy(AggregateDataPtr __restrict place) const noexcept override;

    /**
     * Add single row to aggregate state
     */
    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override;

    /**
     * Add batch of rows to multiple aggregate states (GROUP BY)
     */
    void add_batch(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                   const IColumn** columns, Arena&, bool /*agg_many*/) const override;

    /**
     * Add batch of rows to single aggregate state (no GROUP BY)
     */
    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena&) const override;

    /**
     * Add range of rows to single place (for window functions)
     */
    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena, UInt8* current_window_empty,
                                UInt8* current_window_has_inited) const override;

    /**
     * Reset aggregate state to initial value
     */
    void reset(AggregateDataPtr place) const override;

    /**
     * Merge two aggregate states
     */
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena&) const override;

    /**
     * Serialize aggregate state for shuffle
     */
    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override;

    /**
     * Deserialize aggregate state from shuffle
     */
    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf, Arena&) const override;

    /**
     * Get final result and insert into output column
     */
    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override;

private:
    TFunction _fn;
    DataTypePtr _return_type;
    PythonUDFMeta _func_meta;
    PythonVersion _python_version;
    // Arrow Flight schema: [argument_types..., places: int64, binary_data: binary]
    // Used for all UDAF RPC operations
    // - places column is always present (NULL in single-place mode, actual place_id values in GROUP BY mode)
    // - binary_data column contains serialized data for MERGE operations (NULL for ACCUMULATE)
    mutable std::shared_ptr<arrow::Schema> _schema;
    mutable std::once_flag _schema_init_flag;
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"
