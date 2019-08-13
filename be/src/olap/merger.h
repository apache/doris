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
    Merger(TabletSharedPtr tablet, ReaderType type, 
           RowsetWriterSharedPtr writer, const std::vector<RowsetReaderSharedPtr>& rs_readers);

    virtual ~Merger() {};

    // @brief read from multiple OLAPData and SegmentGroup, then write into single OLAPData and SegmentGroup
    // @return  OLAPStatus: OLAP_SUCCESS or FAIL
    // @note it will take long time to finish.
    OLAPStatus merge(const vector<RowsetReaderSharedPtr>& rs_readers,
                     int64_t* merged_rows, int64_t* filted_rows);
    OLAPStatus merge();

    // 获取在做merge过程中累积的行数
    inline int64_t row_count() const { return _row_count; }
    inline int64_t merged_rows() const { return _merged_rows; }
    inline int64_t filted_rows() const { return _filted_rows; }
private:
    TabletSharedPtr _tablet;
    RowsetWriterSharedPtr _output_rs_writer;

    std::vector<RowsetReaderSharedPtr> _input_rs_readers;
    ReaderType _reader_type;
    int64_t _row_count;
    int64_t _merged_rows;
    int64_t _filted_rows;

    DISALLOW_COPY_AND_ASSIGN(Merger);
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_MERGER_H
