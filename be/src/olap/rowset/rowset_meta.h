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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ROWSET_META_H
#define DORIS_BE_SRC_OLAP_ROWSET_ROWSET_META_H

#include "gen_cpp/olap_file.pb.h"

#include <memory>
#include <vector>

#include "olap/new_status.h"
#include "olap/olap_common.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"

namespace doris {

class RowsetMeta;
using RowsetMetaSharedPtr = std::shared_ptr<RowsetMeta>;
typedef uint64_t RowsetId;

class RowsetMeta {
public:
    virtual ~RowsetMeta() { }

    virtual bool init(const std::string& pb_rowset_meta) {
        return _deserialize_from_pb(pb_rowset_meta);
    }

    virtual bool init_from_json(const std::string& json_rowset_meta) {
        bool ret = json2pb::JsonToProtoMessage(json_rowset_meta, &_rowset_meta_pb);
        return ret;
    }

    virtual bool init_from_pb(const RowsetMetaPB& rs_meta_pb);

    virtual bool deserialize_extra_properties() {
        return true;
    }

    virtual bool serialize(std::string* value) {
        return _serialize_to_pb(value);
    }

    virtual bool get_json_rowset_meta(std::string* json_rowset_meta) {
        json2pb::Pb2JsonOptions json_options;
        json_options.pretty_json = true;
        bool ret = json2pb::ProtoMessageToJson(_rowset_meta_pb, json_rowset_meta, json_options);
        return ret;
    }

    virtual int64_t rowset_id() {
        return _rowset_meta_pb.rowset_id();
    }

    virtual void set_rowset_id(int64_t rowset_id) {
        _rowset_meta_pb.set_rowset_id(rowset_id);
    }

    virtual int64_t get_tablet_id() {
        return _rowset_meta_pb.tablet_id();
    }

    virtual void set_tablet_id(int64_t tablet_id) {
        _rowset_meta_pb.set_tablet_id(tablet_id);
    }

    virtual int64_t get_txn_id() {
        return _rowset_meta_pb.txn_id();
    }

    virtual void set_txn_id(int64_t txn_id) {
        _rowset_meta_pb.set_txn_id(txn_id);
    }

    virtual int32_t get_tablet_schema_hash() {
        return _rowset_meta_pb.tablet_schema_hash();
    }

    virtual void set_tablet_schema_hash(int64_t tablet_schema_hash) {
        _rowset_meta_pb.set_tablet_schema_hash(tablet_schema_hash);
    }

    virtual RowsetTypePB get_rowset_type() {
        return _rowset_meta_pb.rowset_type();
    }

    virtual void set_rowset_type(RowsetTypePB rowset_type) {
        _rowset_meta_pb.set_rowset_type(rowset_type);
    }

    virtual RowsetStatePB get_rowset_state() {
        return _rowset_meta_pb.rowset_state();
    }

    virtual void set_rowset_state(RowsetStatePB rowset_state) {
        _rowset_meta_pb.set_rowset_state(rowset_state);
    }

    virtual Version version() {
        Version version;
        version.first = _rowset_meta_pb.start_version();
        version.second = _rowset_meta_pb.end_version();
        return version;
    }

    virtual void set_version(Version version) {
        _rowset_meta_pb.set_start_version(version.first);
        _rowset_meta_pb.set_end_version(version.second);
    }

    virtual int start_version() {
        return _rowset_meta_pb.start_version();
    }

    virtual void set_start_version(int start_version) {
        _rowset_meta_pb.set_start_version(start_version);
    }
    
    virtual int end_version() {
        return _rowset_meta_pb.end_version();
    }

    virtual void set_end_version(int end_version) {
        _rowset_meta_pb.set_end_version(end_version);
    }
    
    virtual int64_t get_version_hash() {
        return _rowset_meta_pb.version_hash();
    }

    virtual void set_version_hash(int64_t version_hash) {
        _rowset_meta_pb.set_version_hash(version_hash);
    }

    virtual int get_row_number() {
        return _rowset_meta_pb.row_number();
    }

    virtual void set_row_number(int row_number) {
        _rowset_meta_pb.set_row_number(row_number);
    }

    virtual int get_total_disk_size() {
        return _rowset_meta_pb.total_disk_size();
    }

    virtual void set_total_disk_size(int total_disk_size) {
        _rowset_meta_pb.set_total_disk_size(total_disk_size);
    }

    virtual int get_data_disk_size() {
        return _rowset_meta_pb.data_disk_size();
    }

    virtual void set_data_disk_size(int data_disk_size) {
        _rowset_meta_pb.set_data_disk_size(data_disk_size);
    }

    virtual int get_index_disk_size() {
        return _rowset_meta_pb.index_disk_size();
    }

    virtual void set_index_disk_size(int index_disk_size) {
        _rowset_meta_pb.set_index_disk_size(index_disk_size);
    }

    virtual void get_column_statistics(std::vector<ColumnPruning>* column_statistics) {
        for (const ColumnPruning& column_statistic : _rowset_meta_pb.column_statistics()) {
            column_statistics->push_back(column_statistic);
        }
    }

    virtual void set_column_statistics(const std::vector<ColumnPruning>& column_statistics) {
        for (const ColumnPruning& column_statistic : column_statistics) {
            ColumnPruning* new_column_statistic = _rowset_meta_pb.add_column_statistics();
            *new_column_statistic = column_statistic;
        }
    }

    virtual void add_column_statistic(const ColumnPruning& column_statistic) {
        ColumnPruning* new_column_statistic = _rowset_meta_pb.add_column_statistics();
        *new_column_statistic = column_statistic;
    }

    virtual const DeletePredicatePB& get_delete_predicate() {
        return _rowset_meta_pb.delete_predicate();
    }

    virtual void set_delete_predicate(DeletePredicatePB& delete_predicate) {
        DeletePredicatePB* new_delete_condition = _rowset_meta_pb.mutable_delete_predicate();
        *new_delete_condition = delete_predicate;
    }

     virtual bool get_empty() {
        return _rowset_meta_pb.empty();
    }

    virtual void set_empty(bool empty) {
        _rowset_meta_pb.set_empty(empty);
    }

    virtual std::string get_rowset_path() {
        return _rowset_meta_pb.rowset_path();
    }

    virtual void set_rowset_path(std::string rowset_path) {
        _rowset_meta_pb.set_rowset_path(rowset_path);
    }

    virtual PUniqueId get_load_id() {
        return _rowset_meta_pb.load_id();
    }

    virtual void set_load_id(PUniqueId load_id) {
        PUniqueId* new_load_id = _rowset_meta_pb.mutable_load_id();
        new_load_id->set_hi(load_id.hi());
        new_load_id->set_lo(load_id.lo());
    }

    std::string get_extra_properties() {
        return _rowset_meta_pb.extra_properties();
    }

    void set_extra_properties(std::string extra_properties) {
        _rowset_meta_pb.set_extra_properties(extra_properties);
    }

    OLAPStatus to_rowset_pb(RowsetMetaPB* rs_meta_pb);
    virtual int64_t creation_time() const;
private:
    bool _deserialize_from_pb(const std::string& value) {
        return _rowset_meta_pb.ParseFromString(value); 
    }

    bool _serialize_to_pb(std::string* value) {
        if (value == nullptr) {
           return false;
        }
        return _rowset_meta_pb.SerializeToString(value);
    }

private:
    RowsetMetaPB _rowset_meta_pb;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_META_H
