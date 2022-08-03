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
    Compaction(TabletSharedPtr tablet, const std::string& label);
    virtual ~Compaction();

    // This is only for http CompactionAction
    Status compact();
    Status quick_rowsets_compact();

    virtual Status prepare_compact() = 0;
    Status execute_compact();
    virtual Status execute_compact_impl() = 0;

protected:
    virtual Status pick_rowsets_to_compact() = 0;
    virtual std::string compaction_name() const = 0;
    virtual ReaderType compaction_type() const = 0;

    Status do_compaction(int64_t permits);
    Status do_compaction_impl(int64_t permits);

    Status modify_rowsets();
    void gc_output_rowset();

    Status construct_output_rowset_writer(TabletSchemaSPtr schema);
    Status construct_input_rowset_readers();

    Status check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets);
    Status check_correctness(const Merger::Statistics& stats);
    Status find_longest_consecutive_version(std::vector<RowsetSharedPtr>* rowsets,
                                            std::vector<Version>* missing_version);
    int64_t get_compaction_permits();

protected:
    // the root tracker for this compaction
    std::shared_ptr<MemTrackerLimiter> _mem_tracker;

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

    int64_t _oldest_write_timestamp;
    int64_t _newest_write_timestamp;
    RowIdConversion _rowid_conversion;

    DISALLOW_COPY_AND_ASSIGN(Compaction);
};

} // namespace doris
