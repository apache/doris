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

#include "olap/tablet_fwd.h"
#include "olap/task/engine_task.h"

namespace doris {
class CloudStorageEngine;
class TAlterInvertedIndexReq;
class TOlapTableIndex;
class CloudTablet;
class TColumn;

class EngineCloudIndexChangeTask final : public EngineTask {
public:
    Status execute() override;

    EngineCloudIndexChangeTask(CloudStorageEngine& engine, const TAlterInvertedIndexReq& request);
    ~EngineCloudIndexChangeTask();

private:
    Result<std::shared_ptr<CloudTablet>> _get_tablet();

    CloudStorageEngine& _engine;
    std::vector<TOlapTableIndex> _index_list;
    std::vector<TColumn> _columns;
    int64_t _tablet_id;
    int32_t _schema_version;
}; // EngineTask

} // namespace doris