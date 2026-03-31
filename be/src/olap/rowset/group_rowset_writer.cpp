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

#include "olap/rowset/group_rowset_writer.h"

#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "vec/common/assert_cast.h"

namespace doris {

void GroupRowsetWriter::set_data_writer(const RowsetWriterSharedPtr& txn_rowset_writer) {
    _txn_rowset_writer = std::dynamic_pointer_cast<BaseBetaRowsetWriter>(txn_rowset_writer);
}

void GroupRowsetWriter::set_row_binlog_writer(
        const RowsetWriterSharedPtr& row_binlog_rowset_writer) {
    _row_binlog_rowset_writer = row_binlog_rowset_writer;
}

Status GroupRowsetWriter::flush_rowsets() {
    RETURN_IF_ERROR(_txn_rowset_writer->flush());
    if (_row_binlog_rowset_writer) {
        RETURN_IF_ERROR(_row_binlog_rowset_writer->flush());
    }
    return Status::OK();
}

Status GroupRowsetWriter::build_rowsets(std::vector<RowsetSharedPtr>& rowsets) {
    if (rowsets.size() < 2) {
        return Status::InvalidArgument(
                "GroupRowsetWriter::build_rowsets expects at least 2 rowset slots");
    }
    RETURN_IF_ERROR(_txn_rowset_writer->build(rowsets[0]));
    if (_row_binlog_rowset_writer) {
        RETURN_IF_ERROR(_row_binlog_rowset_writer->build(rowsets[1]));
    }
    return Status::OK();
}

} // namespace doris

