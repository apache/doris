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

#include "olap/rowset/rowset_writer.h"

namespace doris {
namespace vectorized {
class Block;
} // namespace vectorized

class GroupRowsetWriter : public RowsetWriter {
public:
    GroupRowsetWriter() = default;

    void set_data_writer(const RowsetWriterSharedPtr& txn_rowset_writer);

    void set_row_binlog_writer(const RowsetWriterSharedPtr& row_binlog_rowset_writer);

    ~GroupRowsetWriter() = default;

    Status flush_rowsets();

    Status build_rowsets(std::vector<RowsetSharedPtr>& rowsets);

    RowsetWriterSharedPtr row_binlog_writer() {
        return _row_binlog_rowset_writer;

    }

    RowsetWriterSharedPtr data_writer() {
        return _txn_rowset_writer;

    }

Status init(const RowsetWriterContext& rowset_writer_context) override {
    _context = rowset_writer_context;
    return Status::OK();
}

    Status add_block(const vectorized::Block* block) override {
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

    // GroupRowsetWriter does not support build a single rowset; its build is
    // delegated to underlying writers.
    Status build(RowsetSharedPtr& rowset) override {
        (void)rowset;
        return Status::NotSupported("GroupRowsetWriter::build is not supported");
    }

    RowsetSharedPtr manual_build(const RowsetMetaSharedPtr& rowset_meta) override {
        (void)rowset_meta;
        LOG(FATAL) << "GroupRowsetWriter::manual_build not implemented";
        return nullptr;
    }

    PUniqueId load_id() override { return _context.load_id; }

    Version version() override { return _context.version; }

    int64_t num_rows() const override { return _txn_rowset_writer->num_rows(); }

    int64_t num_rows_updated() const override { return _txn_rowset_writer->num_rows_updated(); }

    int64_t num_rows_deleted() const override { return _txn_rowset_writer->num_rows_deleted(); }

    int64_t num_rows_new_added() const override {
        return _txn_rowset_writer->num_rows_new_added();
    }

    int64_t num_rows_filtered() const override {
        return _txn_rowset_writer->num_rows_filtered();
    }

    RowsetId rowset_id() override {
        LOG(FATAL) << "GroupRowsetWriter::rowset_id not implemented";
        RowsetId res;
        return res;
    }

    RowsetTypePB type() const override { return BETA_ROWSET; }

    Status get_segment_num_rows(std::vector<uint32_t>* segment_num_rows) const override {
        (void)segment_num_rows;
        return Status::NotSupported("GroupRowsetWriter::get_segment_num_rows to be implemented");
    }

    int32_t allocate_segment_id() override {
        LOG(FATAL) << "GroupRowsetWriter::allocate_segment_id is not supported";
        return -1;
    }

    void set_segment_start_id(int num_segment) override {
        (void)num_segment;
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

