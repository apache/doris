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

#include "olap/olap_define.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/tablet.h"

namespace doris {

class Merger {
public:
    struct Statistics {
        // number of rows written to the destination rowset after merge
        int64_t output_rows = 0;
        int64_t merged_rows = 0;
        int64_t filtered_rows = 0;
    };

    // merge rows from `src_rowset_readers` and write into `dst_rowset_writer`.
    // return OLAP_SUCCESS and set statistics into `*stats_output`.
    // return others on error
    static Status merge_rowsets(TabletSharedPtr tablet, ReaderType reader_type,
                                const std::vector<RowsetReaderSharedPtr>& src_rowset_readers,
                                RowsetWriter* dst_rowset_writer, Statistics* stats_output);

    static Status vmerge_rowsets(TabletSharedPtr tablet, ReaderType reader_type,
                                 const std::vector<RowsetReaderSharedPtr>& src_rowset_readers,
                                 RowsetWriter* dst_rowset_writer, Statistics* stats_output);
};

} // namespace doris
