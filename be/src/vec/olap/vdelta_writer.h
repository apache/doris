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

#include "olap/delta_writer.h"

namespace doris {

namespace vectorized {

class VDeltaWriter : public DeltaWriter {
public:
    virtual ~VDeltaWriter() override;

    static OLAPStatus open(WriteRequest* req, const std::shared_ptr<MemTracker>& parent,
                            VDeltaWriter** writer);

    virtual OLAPStatus write(const vectorized::Block* block, const std::vector<int>& row_idxs) override;

private:
    VDeltaWriter(WriteRequest* req, const std::shared_ptr<MemTracker>& parent,
                 StorageEngine* storage_engine);
};

} // namespace vectorized

} // namespace doris