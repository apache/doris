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

// Fixed-size (30 bytes) binary metadata structure for UDAF operations (Request)
struct __attribute__((packed)) UDAFMetadata {
    uint32_t meta_version;   // 4 bytes: metadata version (current version = 1)
    uint8_t operation;       // 1 byte: UDAFOperation enum
    uint8_t is_single_place; // 1 byte: boolean (0 or 1, ACCUMULATE only)
    int64_t place_id;        // 8 bytes: aggregate state identifier (globally unique)
    int64_t row_start;       // 8 bytes: start row index (ACCUMULATE only)
    int64_t row_end;         // 8 bytes: end row index (exclusive, ACCUMULATE only)
};

static_assert(sizeof(UDAFMetadata) == 30, "UDAFMetadata size must be 30 bytes");

// Current metadata version constant
constexpr uint32_t UDAF_METADATA_VERSION = 1;

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

    ~PythonUDAFClient() override {
        // Clean up all remaining states on destruction
        auto st = close();
        if (!st.ok()) {
            LOG(WARNING) << "Failed to close PythonUDAFClient in destructor: " << st.to_string();
        }
    }

    static Status create(const PythonUDFMeta& func_meta, ProcessPtr process,
                         const std::shared_ptr<arrow::Schema>& data_schema,
                         PythonUDAFClientPtr* client);

    /**
     * Initialize UDAF client with data schema
     * Overrides base class to set _schema before initialization
     * @param func_meta Function metadata
     * @param process Python process handle
     * @param data_schema Arrow schema for UDAF data
     * @return Status
     */
    Status init(const PythonUDFMeta& func_meta, ProcessPtr process,
                const std::shared_ptr<arrow::Schema>& data_schema);

    /**
     * Create aggregate state for a place
     * @param place_id Unique identifier for the aggregate state
     * @return Status
     */
    Status create(int64_t place_id);

    /**
     * Accumulate input data into aggregate state
     * 
     * For single-place mode (is_single_place=true):
     *   - input RecordBatch contains only data columns
     *   - All rows are accumulated to the same place_id
     * 
     * For multi-place mode (is_single_place=false):
     *   - input RecordBatch MUST contain a "places" column (int64) as the last column
     *   - The "places" column indicates which place each row belongs to
     *   - place_id parameter is ignored (set to 0 by convention)
     * 
     * @param place_id Aggregate state identifier (used only in single-place mode)
     * @param is_single_place Whether all rows go to single place
     * @param input Input data batch (must contain "places" column if is_single_place=false)
     * @param row_start Start row index
     * @param row_end End row index (exclusive)
     * @return Status
     */
    Status accumulate(int64_t place_id, bool is_single_place, const arrow::RecordBatch& input,
                      int64_t row_start, int64_t row_end);

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
     * Close client connection and cleanup
     * Overrides base class to destroy the tracked place first
     * @return Status
     */
    Status close();

private:
    DISALLOW_COPY_AND_ASSIGN(PythonUDAFClient);

    /**
     * Send RecordBatch request to Python server with app_metadata
     * @param metadata UDAFMetadata structure (will be sent as app_metadata)
     * @param request_batch Request RecordBatch (contains data columns + binary_data column)
     * @param response_batch Output RecordBatch
     * @return Status
     */
    Status _send_request(const UDAFMetadata& metadata,
                         const std::shared_ptr<arrow::RecordBatch>& request_batch,
                         std::shared_ptr<arrow::RecordBatch>* response_batch);

    /**
     * Create request batch with data columns (for ACCUMULATE)
     * Appends NULL binary_data column to input data batch
     */
    Status _create_data_request_batch(const arrow::RecordBatch& input_data,
                                      std::shared_ptr<arrow::RecordBatch>* out);

    /**
     * Create request batch with binary data (for MERGE)
     * Creates NULL data columns + binary_data column
     */
    Status _create_binary_request_batch(const std::shared_ptr<arrow::Buffer>& binary_data,
                                        std::shared_ptr<arrow::RecordBatch>* out);

    /**
     * Get or create empty request batch (for CREATE/SERIALIZE/FINALIZE/RESET/DESTROY)
     * All columns are NULL. Cached after first creation for reuse.
     */
    Status _get_empty_request_batch(std::shared_ptr<arrow::RecordBatch>* out);

    // Arrow Flight schema: [argument_types..., places: int64, binary_data: binary]
    std::shared_ptr<arrow::Schema> _schema;
    std::shared_ptr<arrow::RecordBatch> _empty_request_batch;
    // Track created state for cleanup
    std::optional<int64_t> _created_place_id;
    // Thread safety: protect gRPC stream operations
    // CRITICAL: gRPC ClientReaderWriter does NOT support concurrent Write() calls
    // Even within same thread, multiple pipeline tasks may trigger concurrent operations
    // (e.g., normal accumulate() + cleanup destroy() during task finalization)
    mutable std::mutex _operation_mutex;
};

} // namespace doris
