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
#include "vec/exec/format/jni_reader.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
class SlotDescriptor;
class TupleDescriptor;
namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

/**
 * JdbcJniReader reads data from JDBC data sources via the unified JniReader/JniConnector
 * framework. It delegates scanning to Java-side JdbcJniScanner (extends JniScanner).
 *
 * This reader follows the same pattern as PaimonJniReader, HudiJniReader, etc:
 * - Constructs a JniConnector with the Java scanner class name and parameters
 * - init() + open() starts the Java scanner
 * - get_next_block() reads data batch by batch
 * - close() releases Java resources
 *
 * In the transitional phase (Phase 3), this reader is used internally by JdbcScanner
 * (which remains the pipeline-level scanner). In a future phase (Phase 4), JdbcScanner
 * can be eliminated and JDBC will flow through FileScanner directly.
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

private:
    std::map<std::string, std::string> _jdbc_params;
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
