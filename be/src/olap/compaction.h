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

#ifndef DORIS_BE_SRC_OLAP_COMPACTION_H
#define DORIS_BE_SRC_OLAP_COMPACTION_H

#include <vector>

#include "olap/merger.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/utils.h"
#include "rowset/alpha_rowset_writer.h"
#include "rowset/rowset_id_generator.h"

namespace doris {

class DataDir;
class Merger;

// This class is a base class for compaction.
// The entrance of this class is compact()
// Any compaction should go through four procedures.
//  1. pick rowsets satisfied to compact
//  2. do compaction
//  3. modify rowsets
//  4. gc unused rowstes
class Compaction {
public:
    Compaction(TabletSharedPtr tablet);
    virtual ~Compaction();

    virtual OLAPStatus compact() = 0;

protected:
    virtual OLAPStatus pick_rowsets_to_compact() = 0;
    virtual std::string compaction_name() const = 0;
    virtual ReaderType compaction_type() const = 0;

    OLAPStatus do_compaction();
    OLAPStatus modify_rowsets();
    OLAPStatus gc_unused_rowsets();

    OLAPStatus construct_output_rowset_writer();
    OLAPStatus construct_input_rowset_readers();

    OLAPStatus check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets);
    OLAPStatus check_correctness(const Merger& merger);

protected:
    TabletSharedPtr _tablet;

    std::vector<RowsetSharedPtr> _input_rowsets;
    std::vector<RowsetReaderSharedPtr> _input_rs_readers;
    int64_t _input_rowsets_size;
    int64_t _input_row_num;

    RowsetSharedPtr _output_rowset;
    RowsetWriterSharedPtr _output_rs_writer;

    enum CompactionState {
        INITED = 0,
        SUCCESS = 1
    };
    CompactionState _state;

    Version _output_version;
    VersionHash _output_version_hash;

    DISALLOW_COPY_AND_ASSIGN(Compaction);
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_COMPACTION_H
