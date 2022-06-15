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
#include "rowset/rowset_id_generator.h"
#include "util/semaphore.hpp"

namespace doris {

class DataDir;
class Merger;

// This class is a base class for compaction.
// The entrance of this class is compact()
// Any compaction should go through four procedures.
//  1. pick rowsets satisfied to compact
//  2. do compaction
//  3. modify rowsets
//  4. gc output rowset if failed
class Compaction {
public:
    Compaction(TabletSharedPtr tablet, const std::string& label,
               const std::shared_ptr<MemTracker>& parent_tracker);
    virtual ~Compaction();

    // This is only for http CompactionAction
    OLAPStatus compact();
    OLAPStatus quick_rowsets_compact();

    virtual OLAPStatus prepare_compact() = 0;
    OLAPStatus execute_compact();
    virtual OLAPStatus execute_compact_impl() = 0;

protected:
    virtual OLAPStatus pick_rowsets_to_compact() = 0;
    virtual std::string compaction_name() const = 0;
    virtual ReaderType compaction_type() const = 0;

    OLAPStatus do_compaction(int64_t permits);
    OLAPStatus do_compaction_impl(int64_t permits);

    OLAPStatus modify_rowsets();
    void gc_output_rowset();

    OLAPStatus construct_output_rowset_writer();
    OLAPStatus construct_input_rowset_readers();

    OLAPStatus check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets);
    OLAPStatus check_correctness(const Merger::Statistics& stats);
    OLAPStatus find_longest_consecutive_version(std::vector<RowsetSharedPtr>* rowsets,
                                                std::vector<Version>* missing_version);
    int64_t get_compaction_permits();

private:
    // get num rows from segment group meta of input rowsets.
    // return -1 if these are not alpha rowsets.
    int64_t _get_input_num_rows_from_seg_grps();

protected:
    // the root tracker for this compaction
    std::shared_ptr<MemTracker> _mem_tracker;

    // the child of root, only track rowset readers mem
    std::shared_ptr<MemTracker> _readers_tracker;

    // the child of root, only track rowset writer mem
    std::shared_ptr<MemTracker> _writer_tracker;
    TabletSharedPtr _tablet;

    std::vector<RowsetSharedPtr> _input_rowsets;
    std::vector<RowsetReaderSharedPtr> _input_rs_readers;
    int64_t _input_rowsets_size;
    int64_t _input_row_num;

    RowsetSharedPtr _output_rowset;
    std::unique_ptr<RowsetWriter> _output_rs_writer;

    enum CompactionState { INITED = 0, SUCCESS = 1 };
    CompactionState _state;

    Version _output_version;

    DISALLOW_COPY_AND_ASSIGN(Compaction);
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_COMPACTION_H
