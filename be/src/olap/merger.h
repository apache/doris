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

#ifndef DORIS_BE_SRC_OLAP_MERGER_H
#define DORIS_BE_SRC_OLAP_MERGER_H

#include "olap/olap_define.h"
#include "olap/tablet.h"
#include "olap/rowset/rowset_writer.h"

namespace doris {

class SegmentGroup;
class ColumnData;

class Merger {
public:
    // parameter index is created by caller, and it is empty.
    Merger(TabletSharedPtr tablet, RowsetWriterSharedPtr writer, ReaderType type);

    virtual ~Merger() {};

    // @brief read from multiple OLAPData and SegmentGroup, then write into single OLAPData and SegmentGroup
    // @return  OLAPStatus: OLAP_SUCCESS or FAIL
    // @note it will take long time to finish.
    OLAPStatus merge(const std::vector<RowsetReaderSharedPtr>& rs_readers, 
                     uint64_t* merged_rows, uint64_t* filted_rows);

    // 获取在做merge过程中累积的行数
    uint64_t row_count() {
        return _row_count;
    }
private:
    TabletSharedPtr _tablet;
    RowsetWriterSharedPtr _rs_writer;
    ReaderType _reader_type;
    uint64_t _row_count;

    DISALLOW_COPY_AND_ASSIGN(Merger);
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_MERGER_H
