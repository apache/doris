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

#include "storage/rowset/group_rowset_writer.h"

#include "storage/rowset/beta_rowset_writer.h"
#include "storage/segment/segment_writer.h"
#include "util/debug_points.h"

namespace doris {

void GroupRowsetWriter::set_data_writer(const RowsetWriterSharedPtr& txn_rowset_writer) {
    _txn_rowset_writer = txn_rowset_writer;
}

void GroupRowsetWriter::set_row_binlog_writer(
        const RowsetWriterSharedPtr& row_binlog_rowset_writer) {
    _row_binlog_rowset_writer = row_binlog_rowset_writer;
}

Status GroupRowsetWriter::init(const RowsetWriterContext& rowset_writer_context) {
    DCHECK(_txn_rowset_writer != nullptr);
    DCHECK(_row_binlog_rowset_writer != nullptr);

    _context = rowset_writer_context;
    auto& data_ctx = const_cast<RowsetWriterContext&>(_txn_rowset_writer->context());
    auto& row_binlog_ctx = const_cast<RowsetWriterContext&>(_row_binlog_rowset_writer->context());
    _context._need_allocate_lsn =
            data_ctx.need_allocated_lsn() || row_binlog_ctx.need_allocated_lsn();
    if (_context.need_allocated_lsn()) {
        // Share segment LSNs between data and row-binlog writers.
        _context.allocated_lsn_map = std::make_shared<segment_v2::SegmentAllocatedLsnMap>();
        data_ctx.allocated_lsn_map = _context.allocated_lsn_map;
        row_binlog_ctx.allocated_lsn_map = _context.allocated_lsn_map;
    }
    return Status::OK();
}

Status GroupRowsetWriter::flush_rowsets() {
    RETURN_IF_ERROR(_txn_rowset_writer->flush());
    RETURN_IF_ERROR(_row_binlog_rowset_writer->flush());
    return Status::OK();
}

Status GroupRowsetWriter::build_rowsets(std::vector<RowsetSharedPtr>& rowsets) {
    rowsets.clear();
    rowsets.reserve(2);

    RowsetSharedPtr txn_rowset;
    RowsetSharedPtr row_binlog_rowset;
    RETURN_IF_ERROR(_txn_rowset_writer->build(txn_rowset));
    Status st = Status::OK();
    DBUG_EXECUTE_IF("GroupRowsetWriter::build_rowsets.row_binlog_build_failed",
                    { st = Status::InternalError("debug row binlog build failed"); });
    if (st.ok()) {
        st = _row_binlog_rowset_writer->build(row_binlog_rowset);
    }
    if (!st.ok()) {
        RETURN_IF_ERROR(_txn_rowset_writer->force_rollback());
        return st;
    }

    rowsets.emplace_back(std::move(txn_rowset));
    rowsets.emplace_back(std::move(row_binlog_rowset));
    return Status::OK();
}

Status GroupRowsetWriter::flush_memtable(Block* block, int32_t segment_id, int64_t* flush_size) {
    RETURN_IF_ERROR(_txn_rowset_writer->flush_memtable(block, segment_id, flush_size));
    RETURN_IF_ERROR(_row_binlog_rowset_writer->flush_memtable(block, segment_id, flush_size));
    return Status::OK();
}

Status GroupRowsetWriter::flush_single_block(const Block* block) {
    auto segment_id = DORIS_TRY(allocate_segment_id());
    return flush_single_block(block, segment_id);
}

Status GroupRowsetWriter::flush_single_block(const Block* block, int32_t segment_id) {
    RETURN_IF_ERROR(_txn_rowset_writer->flush_single_block(block, segment_id));
    RETURN_IF_ERROR(_row_binlog_rowset_writer->flush_single_block(block, segment_id));
    return Status::OK();
}

Result<int32_t> GroupRowsetWriter::allocate_segment_id() {
    DCHECK(_txn_rowset_writer != nullptr);
    DCHECK(_row_binlog_rowset_writer != nullptr);
    auto segment_id = DORIS_TRY(_txn_rowset_writer->allocate_segment_id());
    auto binlog_segment_id = DORIS_TRY(_row_binlog_rowset_writer->allocate_segment_id());
    DCHECK_EQ(segment_id, binlog_segment_id);
    return segment_id;
}

} // namespace doris
