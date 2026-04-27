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

#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "format/jni/jni_reader.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
class SlotDescriptor;
class TupleDescriptor;
class Block;

/**
 * JdbcJniReader reads data from JDBC data sources via the unified JniReader
 * framework. It delegates scanning to Java-side JdbcJniScanner (extends JniScanner).
 *
 * This reader follows the same pattern as PaimonJniReader, HudiJniReader, etc:
 * - Passes Java scanner class path and parameters to JniReader base constructor
 * - init_reader() calls open() to start the Java scanner
 * - get_next_block() reads data batch by batch (inherited from JniReader)
 * - close() releases Java resources (inherited from JniReader)
 *
 * Special types like bitmap, HLL and quantile_state are handled by:
 * 1. Temporarily replacing block columns to string type before reading
 * 2. Reading data as strings via JNI
 * 3. Casting string columns back to the target special types after reading
 */
class JdbcJniReader : public JniReader {
    ENABLE_FACTORY_CREATOR(JdbcJniReader);

public:
    /**
     * Construct a JdbcJniReader.
     *
     * @param file_slot_descs Slot descriptors for the output columns
     * @param state Runtime state
     * @param profile Runtime profile for metrics
     * @param jdbc_params JDBC connection parameters (jdbc_url, query_sql, etc.)
     */
    JdbcJniReader(const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
                  RuntimeProfile* profile, const std::map<std::string, std::string>& jdbc_params);

    ~JdbcJniReader() override = default;

    Status init_reader();

    /**
     * Override get_next_block to handle special types (bitmap, HLL, quantile_state, JSONB).
     * Before reading, replaces block columns of special types with string columns.
     * After reading, casts the string data back to the target types.
     */
    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override;

protected:
    Status _do_init_reader(ReaderInitContext* /*ctx*/) override { return init_reader(); }

private:
    std::map<std::string, std::string> _jdbc_params;

    /**
     * Check if a primitive type needs special string-based handling.
     * These types (bitmap, HLL, quantile_state, JSONB) are read as strings via JDBC
     * and need post-read casting back to their target types.
     */
    static bool _is_special_type(PrimitiveType type);

    /**
     * Cast a string column back to the target special type using the CAST function.
     * Follows the same pattern as the old vjdbc_connector.cpp _cast_string_to_hll/bitmap/json.
     */
    Status _cast_string_to_special_type(const SlotDescriptor* slot_desc, Block* block,
                                        int column_index, int num_rows);
};

} // namespace doris
