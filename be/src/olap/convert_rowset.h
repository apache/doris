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

#include "olap/merger.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"

namespace doris {
class DataDir;
class ConvertRowset {
public:
    ConvertRowset(TabletSharedPtr tablet, const std::string& label,
                  const std::shared_ptr<MemTracker>& parent_tracker)
        : _tablet(tablet),
          _mem_tracker(MemTracker::CreateTracker(-1, label, parent_tracker, true, false, MemTrackerLevel::TASK)),
          _reader_tracker(MemTracker::CreateTracker(-1, "ConvertRowsetReaderTracker:" + std::to_string(tablet->tablet_id()), _mem_tracker,
                  true, false)),
          _writer_tracker(MemTracker::CreateTracker(-1, "ConvertRowsetWriterTracker:" + std::to_string(tablet->tablet_id()), _mem_tracker,
                  true, false)) {}

    OLAPStatus do_convert();

private:
    OLAPStatus construct_output_rowset_writer(const Version& version, std::unique_ptr<RowsetWriter>* output);
    OLAPStatus check_correctness(RowsetSharedPtr input_rowset, RowsetSharedPtr output_rowset,
                                 const Merger::Statistics& stats);
    int64_t _get_input_num_rows_from_seg_grps(RowsetSharedPtr rowset);
    OLAPStatus _modify_rowsets(RowsetSharedPtr input_rowset, RowsetSharedPtr output_rowset);

private:
    TabletSharedPtr _tablet;

    // the root tracker for this rowset converter
    std::shared_ptr<MemTracker> _mem_tracker;

    // the child of root, only track rowset reader mem
    std::shared_ptr<MemTracker> _reader_tracker;

    // the child of root, only track rowset writer mem
    std::shared_ptr<MemTracker> _writer_tracker;

    DISALLOW_COPY_AND_ASSIGN(ConvertRowset);
};
} // namespace doris