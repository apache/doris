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

#ifndef DORIS_BE_SRC_OLAP_CUMULATIVE_COMPACTION_H
#define DORIS_BE_SRC_OLAP_CUMULATIVE_COMPACTION_H

#include <string>

#include "olap/compaction.h"

namespace doris {

class CumulativeCompaction : public Compaction {
public:
    CumulativeCompaction(TabletSharedPtr tablet);
    ~CumulativeCompaction() override;

    OLAPStatus compact() override;

protected:
    OLAPStatus pick_rowsets_to_compact() override;

    std::string compaction_name() const override {
        return "cumulative compaction";
    }

    ReaderType compaction_type() const override {
        return ReaderType::READER_CUMULATIVE_COMPACTION;
    }

private:
    int64_t _cumulative_rowset_size_threshold;

    DISALLOW_COPY_AND_ASSIGN(CumulativeCompaction);
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_CUMULATIVE_COMPACTION_H
