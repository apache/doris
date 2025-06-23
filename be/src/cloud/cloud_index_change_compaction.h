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

#include <memory>

#include "cloud/cloud_full_compaction.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "olap/compaction.h"

namespace doris {

class CloudIndexChangeCompaction : public CloudFullCompaction {
public:
    CloudIndexChangeCompaction(CloudStorageEngine& engine, CloudTabletSPtr tablet,
                              RowsetSharedPtr input_rowset, TabletSchemaSPtr output_schema);

    ~CloudIndexChangeCompaction();

    Status pick_rowsets_to_compact() override;

protected:
    std::string_view compaction_name() const override { return "CloudIndexChangeCompaction"; }

    ReaderType compaction_type() const override {
        return ReaderType::READER_INDEX_CHANGE_COMPACTION;
    }

    TabletSchemaSPtr get_output_schema() override { return _output_schema; }

    TabletSchemaSPtr _output_schema {nullptr};
};

}; // namespace doris
