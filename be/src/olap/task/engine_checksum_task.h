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

#ifndef DORIS_BE_SRC_OLAP_TASK_ENGINE_CHECKSUM_TASK_H
#define DORIS_BE_SRC_OLAP_TASK_ENGINE_CHECKSUM_TASK_H

#include <gen_cpp/Types_types.h>

#include <memory>

#include "common/status.h"
#include "olap/task/engine_task.h"

namespace doris {
class StorageEngine;

// base class for storage engine
// add "Engine" as task prefix to prevent duplicate name with agent task
class EngineChecksumTask final : public EngineTask {
public:
    Status execute() override;

    EngineChecksumTask(StorageEngine& engine, TTabletId tablet_id, TSchemaHash schema_hash,
                       TVersion version, uint32_t* checksum);

    ~EngineChecksumTask() override;

private:
    Status _compute_checksum();

    StorageEngine& _engine;
    TTabletId _tablet_id;
    TSchemaHash _schema_hash;
    TVersion _version;
    uint32_t* _checksum;
}; // EngineTask

} // namespace doris
#endif //DORIS_BE_SRC_OLAP_TASK_ENGINE_CHECKSUM_TASK_H
