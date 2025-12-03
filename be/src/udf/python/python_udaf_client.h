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

#include <arrow/status.h>

#include "udf/python/python_client.h"

namespace doris {

class PythonUDAFClient;

using PythonUDAFClientPtr = std::shared_ptr<PythonUDAFClient>;

/**
 * Python UDAF Client
 * 
 * Implements Snowflake-style UDAF pattern with the following methods:
 * - __init__(): Initialize aggregate state
 * - aggregate_state: Property that returns internal state
 * - accumulate(input): Add new input to aggregate state
 * - merge(other_state): Combine two intermediate states
 * - finish(): Generate final result from aggregate state
 * 
 * Communication protocol with Python server:
 * 1. CREATE: Initialize UDAF class instance and get initial state
 * 2. ACCUMULATE: Send input data batch and get updated states
 * 3. SERIALIZE: Get serialized state for shuffle/merge
 * 4. MERGE: Combine serialized states
 * 5. FINALIZE: Get final result from state
 * 6. RESET: Reset state to initial value
 * 7. DESTROY: Clean up resources
 */
class PythonUDAFClient : public PythonClient {
public:
    // UDAF operation types
    enum class UDAFOperation : uint8_t {
        CREATE = 0,     // Create new aggregate state
        ACCUMULATE = 1, // Add input rows to state
        SERIALIZE = 2,  // Serialize state for shuffle
        MERGE = 3,      // Merge two states
        FINALIZE = 4,   // Get final result
        RESET = 5,      // Reset state
        DESTROY = 6     // Destroy state
    };

    PythonUDAFClient() = default;
    ~PythonUDAFClient() override = default;

    static Status create(const PythonUDFMeta& func_meta, ProcessPtr process,
                         PythonUDAFClientPtr* client);

    /**
     * Create aggregate state for a place
     * @param place_id Unique identifier for the aggregate state
     * @return Status
     */
    Status create(int64_t place_id);

    /**
     * Accumulate input data into aggregate state
     * @param place_id Aggregate state identifier
     * @param is_single_place Whether all rows go to single place
     * @param input Input data batch
     * @param row_start Start row index
     * @param row_end End row index (exclusive)
     * @param places Array of place pointers (for GROUP BY)
     * @param place_offset Offset within each place
     * @return Status
     */
    Status accumulate(int64_t place_id, bool is_single_place, const arrow::RecordBatch& input,
                      int64_t row_start, int64_t row_end, const int64_t* places = nullptr,
                      int64_t place_offset = 0);

    /**
     * Serialize aggregate state for shuffle/merge
     * @param place_id Aggregate state identifier
     * @param serialized_state Output serialized state
     * @return Status
     */
    Status serialize(int64_t place_id, std::shared_ptr<arrow::Buffer>* serialized_state);

    /**
     * Merge another serialized state into current state
     * @param place_id Target aggregate state identifier
     * @param serialized_state Serialized state to merge
     * @return Status
     */
    Status merge(int64_t place_id, const std::shared_ptr<arrow::Buffer>& serialized_state);

    /**
     * Get final result from aggregate state
     * @param place_id Aggregate state identifier
     * @param output Output result
     * @return Status
     */
    Status finalize(int64_t place_id, std::shared_ptr<arrow::RecordBatch>* output);

    /**
     * Reset aggregate state to initial value
     * @param place_id Aggregate state identifier
     * @return Status
     */
    Status reset(int64_t place_id);

    /**
     * Destroy aggregate state and free resources
     * @param place_id Aggregate state identifier
     * @return Status
     */
    Status destroy(int64_t place_id);

    /**
     * Destroy all aggregate states
     * @return Status
     */
    Status destroy_all();

    /**
     * Close client connection and cleanup
     * Overrides base class to destroy all states first
     * @return Status
     */
    Status close();

    static std::string print_operation(UDAFOperation op);

private:
    DISALLOW_COPY_AND_ASSIGN(PythonUDAFClient);

    /**
     * Helper to execute a UDAF operation (CREATE, RESET, DESTROY, etc.)
     * This consolidates the common pattern:
     * 1. Check if process is alive
     * 2. Create unified batch
     * 3. Send operation
     * 4. Validate boolean response (if validate_response is true)
     * 
     * Template parameters are compile-time constants for better optimization
     * @tparam operation The UDAF operation to execute
     * @tparam validate_response If true, validates response as boolean and checks success
     * @param place_id Aggregate state identifier
     * @param metadata Optional metadata buffer
     * @param data Optional data buffer
     * @param output Output RecordBatch (can be nullptr if not needed)
     * @return Status
     */
    template <UDAFOperation operation, bool validate_response>
    Status _execute_operation(int64_t place_id, const std::shared_ptr<arrow::Buffer>& metadata,
                              const std::shared_ptr<arrow::Buffer>& data,
                              std::shared_ptr<arrow::RecordBatch>* output);

    /**
     * Send operation request to Python server
     * @param input Input data batch
     * @param output Optional output data
     * @return Status
     */
    Status _send_operation(const arrow::RecordBatch* input,
                           std::shared_ptr<arrow::RecordBatch>* output);

    // Track created states for cleanup
    std::unordered_set<int64_t> _created_states;

    // Thread safety: protect concurrent RPC calls
    // Arrow Flight client (gRPC-based) is not fully thread-safe,
    // so we need to serialize all operations through this mutex
    mutable std::mutex _operation_mutex;
};

} // namespace doris
