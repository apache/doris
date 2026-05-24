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

#include "storage/rowset/rowset_writer.h"

namespace doris {
class Block;
class GroupRowsetWriter : public RowsetWriter {
public:
    GroupRowsetWriter() = default;

    void set_data_writer(const RowsetWriterSharedPtr& txn_rowset_writer);

    void set_row_binlog_writer(const RowsetWriterSharedPtr& row_binlog_rowset_writer);

    ~GroupRowsetWriter() = default;

    Status flush_rowsets();

    Status build_rowsets(std::vector<RowsetSharedPtr>& rowsets);

    RowsetWriterSharedPtr row_binlog_writer() { return _row_binlog_rowset_writer; }

    RowsetWriterSharedPtr data_writer() { return _txn_rowset_writer; }

    Status init(const RowsetWriterContext& rowset_writer_context) override {
        _context = rowset_writer_context;
        return Status::OK();
    }

    Status add_block(const Block* block) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "GroupRowsetWriter::add_block is not implemented");
    }

    // add rowset by create hard link
    Status add_rowset(RowsetSharedPtr rowset) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "GroupRowsetWriter::add_rowset is not implemented");
    }

    // Precondition: the input `rowset` should have the same type of the rowset we're building
    Status add_rowset_for_linked_schema_change(RowsetSharedPtr rowset) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "GroupRowsetWriter::add_rowset_for_linked_schema_change is not implemented");
    }

    // explicit flush all buffered rows into segment file.
    // note that `add_row` could also trigger flush when certain conditions are met
    Status flush() override { return flush_rowsets(); }

    Status flush_memtable(Block* block, int32_t segment_id, int64_t* flush_size) override;

    Status flush_single_block(const Block* block) override;

    // GroupRowsetWriter does not support build a single rowset; its build is
    // delegated to underlying writers.
    Status build(RowsetSharedPtr& rowset) override {
        return Status::NotSupported("GroupRowsetWriter::build is not supported");
    }

    RowsetSharedPtr manual_build(const RowsetMetaSharedPtr& rowset_meta) override {
        LOG(FATAL) << "GroupRowsetWriter::manual_build not implemented";
        return nullptr;
    }

    PUniqueId load_id() override { return _context.load_id; }

    Version version() override { return _context.version; }

    int64_t num_rows() const override { return _txn_rowset_writer->num_rows(); }

    int64_t num_rows_updated() const override { return _txn_rowset_writer->num_rows_updated(); }

    int64_t num_rows_deleted() const override { return _txn_rowset_writer->num_rows_deleted(); }

    int64_t num_rows_new_added() const override { return _txn_rowset_writer->num_rows_new_added(); }

    int64_t num_rows_filtered() const override { return _txn_rowset_writer->num_rows_filtered(); }

    RowsetId rowset_id() override {
        LOG(FATAL) << "GroupRowsetWriter::rowset_id not implemented";
        RowsetId res;
        return res;
    }

    RowsetTypePB type() const override { return BETA_ROWSET; }

    Status get_segment_num_rows(std::vector<uint32_t>* segment_num_rows) const override {
        return Status::NotSupported("GroupRowsetWriter::get_segment_num_rows to be implemented");
    }

    int32_t allocate_segment_id() override {
        LOG(FATAL) << "GroupRowsetWriter::allocate_segment_id is not supported";
        return -1;
    }

    int32_t get_allocated_segment_id() override {
        DCHECK(_txn_rowset_writer != nullptr);
        DCHECK(_row_binlog_rowset_writer != nullptr);
        auto seg_id = _txn_rowset_writer->get_allocated_segment_id();
        DCHECK_EQ(seg_id, _row_binlog_rowset_writer->get_allocated_segment_id());
        return seg_id;
    }

    void set_segment_start_id(int num_segment) override {
        LOG(FATAL) << "GroupRowsetWriter::set_segment_start_id not supported";
    }

    int64_t delete_bitmap_ns() override { return 0; }

    int64_t segment_writer_ns() override { return 0; }

    bool is_partial_update() override { return _txn_rowset_writer->is_partial_update(); }

    std::shared_ptr<PartialUpdateInfo> get_partial_update_info() override {
        return _txn_rowset_writer->get_partial_update_info();
    }

private:
    RowsetWriterSharedPtr _txn_rowset_writer;
    RowsetWriterSharedPtr _row_binlog_rowset_writer;
};

} // namespace doris
