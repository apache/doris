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
 * 
 * All places within the same AggregatePythonUDAF share a single
 * PythonUDAFClient (one Python process), distinguished by place_id.
 * This is similar to Java UDAF's design where all places share one UdafExecutor.
 * 
 * The client pointer is set during create() and points to the shared client
 * owned by AggregatePythonUDAF. This data structure does NOT own the client.
 */
struct AggregatePythonUDAFData {
    mutable std::string serialize_data; // Cached serialized state
    PythonUDAFClient* client = nullptr; // Pointer to shared client (not owned)

    AggregatePythonUDAFData() = default;

    // Copy constructor needed for aggregation framework
    AggregatePythonUDAFData(const AggregatePythonUDAFData& other)
            : serialize_data(other.serialize_data), client(other.client) {}

    ~AggregatePythonUDAFData() = default;

    // Set client pointer (called once during create)
    void set_client(PythonUDAFClient* cli) { client = cli; }

    // All methods use the member client pointer
    Status create(int64_t place);

    Status add(int64_t place_id, const IColumn** columns, int64_t row_num_start,
               int64_t row_num_end, const DataTypes& argument_types);

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
            : IAggregateFunctionDataHelper(argument_types_),
              _fn(fn),
              _return_type(return_type),
              _client_initialized(false) {}

    ~AggregatePythonUDAF() override {
        // Clean up shared client when aggregate function is destroyed
        if (_shared_client) {
            Status st = _shared_client->close();
            if (!st.ok()) {
                LOG(WARNING) << "Failed to close shared Python UDAF client: " << st.to_string();
            }
        }
    }

    static AggregateFunctionPtr create(const TFunction& fn, const DataTypes& argument_types_,
                                       const DataTypePtr& return_type) {
        return std::make_shared<AggregatePythonUDAF>(fn, argument_types_, return_type);
    }

    String get_name() const override { return _fn.name.function_name; }

    DataTypePtr get_return_type() const override { return _return_type; }

    /**
     * Initialize function metadata (but not client - each data instance creates its own)
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
    /**
     * Initialize shared client (called lazily on first create())
     */
    Status _init_shared_client() const;

    /**
     * Get the shared client, initializing if necessary
     */
    PythonUDAFClient* _get_shared_client() const;

    TFunction _fn;
    DataTypePtr _return_type;

    // Function metadata initialized in open()
    PythonUDFMeta _func_meta;
    PythonVersion _python_version;

    // Shared client for all places (similar to Java UDAF's _exec_place)
    mutable PythonUDAFClientPtr _shared_client;
    mutable bool _client_initialized;
    mutable std::mutex _client_init_mutex;
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"
