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

#include "olap/task/engine_checksum_task.h"

#include "runtime/thread_context.h"

namespace doris {

EngineChecksumTask::EngineChecksumTask(TTabletId tablet_id, TSchemaHash schema_hash,
                                       TVersion version, uint32_t* checksum)
        : _tablet_id(tablet_id), _schema_hash(schema_hash), _version(version) {
    _mem_tracker = std::make_shared<MemTrackerLimiter>(
            MemTrackerLimiter::Type::CONSISTENCY,
            "EngineChecksumTask#tabletId=" + std::to_string(tablet_id));
}

Status EngineChecksumTask::execute() {
    SCOPED_ATTACH_TASK(_mem_tracker);
    return _compute_checksum();
} // execute

Status EngineChecksumTask::_compute_checksum() {
    LOG(INFO) << "begin to process compute checksum."
              << "tablet_id=" << _tablet_id << ", schema_hash=" << _schema_hash
              << ", version=" << _version;
    return Status::InternalError("Not implemented yet");
}

} // namespace doris
