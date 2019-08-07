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

#include <list>
#include <map>
#include <string>
#include <vector>

#include "olap/merger.h"
#include "olap/olap_define.h"
#include "olap/tablet.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/rowset/alpha_rowset_writer.h"

namespace doris {

class Rowset;

class CumulativeCompaction {
public:
    CumulativeCompaction(TabletSharedPtr tablet);
    ~CumulativeCompaction();

    OLAPStatus compact();
    OLAPStatus pick_rowsets_to_compact();
    OLAPStatus do_cumulative_compaction();
    
    OLAPStatus save_meta();
    OLAPStatus modify_rowsets();
    OLAPStatus gc_unused_rowsets();

    OLAPStatus check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets);
    OLAPStatus check_correctness(const Merger& merger);
    OLAPStatus construct_output_rowset_writer();
    OLAPStatus construct_input_rowset_readers();

private:
    TabletSharedPtr _tablet;

    int64_t _cumulative_rowset_size_threshold;
    bool _cumulative_locked;

    Version _cumulative_version;
    VersionHash _cumulative_version_hash;

    std::vector<RowsetSharedPtr> _input_rowsets;
    int64_t _input_rowsets_size;
    int64_t _input_row_num;
    std::vector<RowsetReaderSharedPtr> _input_rs_readers;

    RowsetSharedPtr _output_rowset;
    RowsetWriterSharedPtr _output_rs_writer;

    enum CumulativeState {
        FAILED = 0,
        SUCCESS = 1
    };
    CumulativeState _cumulative_state;

    DISALLOW_COPY_AND_ASSIGN(CumulativeCompaction);
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_CUMULATIVE_COMPACTION_H
