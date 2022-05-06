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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_WRITER_H
#define DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_WRITER_H

#include <vector>

#include "olap/rowset/column_data_writer.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/segment_group.h"

namespace doris {

enum WriterState { WRITER_CREATED, WRITER_INITED, WRITER_FLUSHED };

class AlphaRowsetWriter : public RowsetWriter {
public:
    AlphaRowsetWriter();
    virtual ~AlphaRowsetWriter();

    Status init(const RowsetWriterContext& rowset_writer_context) override;

    Status add_row(const RowCursor& row) override { return _add_row(row); }
    Status add_row(const ContiguousRow& row) override { return _add_row(row); }

    // add rowset by create hard link
    Status add_rowset(RowsetSharedPtr rowset) override;
    Status add_rowset_for_linked_schema_change(RowsetSharedPtr rowset,
                                               const SchemaMapping& schema_mapping) override;

    Status add_rowset_for_migration(RowsetSharedPtr rowset) override;
    Status flush() override;

    // get a rowset
    RowsetSharedPtr build() override;

    Version version() override { return _rowset_writer_context.version; }

    int64_t num_rows() override { return _num_rows_written; }

    RowsetId rowset_id() override { return _rowset_writer_context.rowset_id; }

    RowsetTypePB type() const override { return RowsetTypePB::ALPHA_ROWSET; }

private:
    Status _init();

    template <typename RowType>
    Status _add_row(const RowType& row);

    // validate rowset build arguments before create rowset to make sure correctness
    bool _validate_rowset();

    Status _garbage_collection();

private:
    int32_t _segment_group_id;
    SegmentGroup* _cur_segment_group;
    ColumnDataWriter* _column_data_writer;
    std::shared_ptr<RowsetMeta> _current_rowset_meta;
    bool _is_pending_rowset;
    int64_t _num_rows_written;
    RowsetWriterContext _rowset_writer_context;
    std::vector<SegmentGroup*> _segment_groups;
    bool _rowset_build;
    WriterState _writer_state;
    // add_rowset does not need to call column_data_writer.finalize()
    bool _need_column_data_writer;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_WRITER_H
