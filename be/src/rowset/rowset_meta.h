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

#ifndef DORIS_BE_SRC_ROWSET_ROWSET_META_H
#define DORIS_BE_SRC_ROWSET_ROWSET_META_H

#include "olap/olap_define.h"
#include "rowset/rowset_reader.h"
#include "rowset/rowset_writer.h"
#include "gen_cpp/olap_file.pb.h"

#include <memory>
#include <vector>

namespace doris {

class Rowset {
public:
    virtual void init(const RowsetMetaPb& rowset_meta) {
        _rowset_meta = rowset_meta;
    }

    virtual bool deserialize_extra_properties()  = 0;

    virtual void get_rowset_meta_pb(RowsetMetaPb* rowset_meta) {
        rowset_meta = &_rowset_meta;
    }
    virtual int64_t get_rowset_id() {
        return _rowset_meta.rowset_id();
    }

    virtual void set_rowset_id(int64_t rowset_id) {
        _rowset_meta.set_rowset_id(rowset_id);
    }

    virtual int64_t get_version() {
        return _rowset_meta->version();
    }

    virtual void set_version(int64_t version) {
        _rowset_meta->set_version(version);
    }

    virtual int64_t get_tablet_id() {
        return _rowset_meta->tablet_id();
    }

    virtual void set_tablet_id(int64_t tablet_id) {
        _rowset_meta->set_tablet_id(tablet_id);
    }

    virtual int32_t get_tablet_schema_hash() {
        return _rowset_meta->tablet_schema_hash();
    }

    virtual void set_tablet_schema_hash(int64_t tablet_schema_hash) {
        _rowset_meta->set_tablet_schema_hash(tablet_schema_hash);
    }

    virtual RowsetType get_rowset_type() {
        return _rowset_meta->rowset_type();
    }

    virtual void set_rowset_type(RowsetType rowset_type) {
        _rowset_meta->set_rowset_type(rowset_type);
    }

    virtual RowsetState get_rowset_state() {
        return _rowset_meta->rowset_state();
    }

    virtual void set_rowset_state(RowsetState rowset_state) {
        _rowset_meta->set_rowset_state(rowset_state);
    }

    virtual int get_start_version() {
        return _rowset_meta->start_version();
    }

    virtual void set_start_version(int start_version) {
        _rowset_meta->set_start_version(start_version);
    }
    
    virtual int get_end_version() {
        return _rowset_meta->end_version();
    }

    virtual void set_end_version(int end_version) {
        _rowset_meta->set_end_version(end_version);
    }
    
    virtual int get_row_number() {
        return _rowset_meta->row_number();
    }

    virtual void set_row_number(int row_number) {
        _rowset_meta->set_row_number(row_number);
    }

    virtual int get_total_disk_size() {
        return _rowset_meta->total_disk_size();
    }

    virtual void set_total_disk_size(int total_disk_size) {
        _rowset_meta->set_total_disk_size(total_disk_size);
    }

    virtual int get_data_disk_size() {
        return _rowset_meta->data_disk_size();
    }

    virtual void set_data_disk_size(int data_disk_size) {
        _rowset_meta->set_data_disk_size(data_disk_size);
    }

    virtual int get_index_disk_size() {
        return _rowset_meta->index_disk_size();
    }

    virtual void set_index_disk_size(int index_disk_size) {
        _rowset_meta->set_index_disk_size(index_disk_size);
    }

    virtual void get_column_statistics(std::vector<ColumnPruning>* column_statistics) {
        *column_statistics = _rowset_meta->column_statistics();
    }

    virtual void set_column_statistics(std::vector<ColumnPruning> column_statistics) {
        std::vector<ColumnPruning>* new_column_statistics = _rowset_meta.mutable_column_pruning();
        *new_column_statistics = column_statistics;
    }

    virtual DeleteConditionMessage get_delete_condition() {
        return _rowset_meta->delete_condition();
    }

    virtual void set_delete_condition(DeleteConditionMessage delete_condition) {
        _rowset_meta->set_delete_condition(delete_condition);
    }

private:
    RowsetMetaPb _rowset_meta;
};

}

#endif // DORIS_BE_SRC_ROWSET_ROWSET_META_H