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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/descriptors.h
// and modified by Doris

#pragma once

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/port.h>
#include <stdint.h>

#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/global_types.h"
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/types.h"
#include "vec/data_types/data_type.h"

namespace google::protobuf {
template <typename Element>
class RepeatedField;
} // namespace google::protobuf

namespace doris {

class ObjectPool;
class PTupleDescriptor;
class PSlotDescriptor;

class SlotDescriptor {
public:
    // virtual ~SlotDescriptor() {};
    SlotId id() const { return _id; }
    const TypeDescriptor& type() const { return _type; }
    TupleId parent() const { return _parent; }
    // Returns the column index of this slot, including partition keys.
    // (e.g., col_pos - num_partition_keys = the table column this slot corresponds to)
    int col_pos() const { return _col_pos; }
    // Returns the field index in the generated llvm struct for this slot's tuple
    int field_idx() const { return _field_idx; }
    bool is_materialized() const { return _is_materialized; }
    bool is_nullable() const { return _is_nullable; }

    const std::string& col_name() const { return _col_name; }
    const std::string& col_name_lower_case() const { return _col_name_lower_case; }

    void to_protobuf(PSlotDescriptor* pslot) const;

    std::string debug_string() const;

    vectorized::MutableColumnPtr get_empty_mutable_column() const;

    doris::vectorized::DataTypePtr get_data_type_ptr() const;

    int32_t col_unique_id() const { return _col_unique_id; }

    bool is_key() const { return _is_key; }
    bool need_materialize() const { return _need_materialize; }
    const std::vector<std::string>& column_paths() const { return _column_paths; };

    bool is_auto_increment() const { return _is_auto_increment; }

    const std::string& col_default_value() const { return _col_default_value; }
    PrimitiveType col_type() const { return _col_type; }

private:
    friend class DescriptorTbl;
    friend class TupleDescriptor;
    friend class SchemaScanner;
    friend class OlapTableSchemaParam;
    friend class PInternalServiceImpl;
    friend class RowIdStorageReader;
    friend class Tablet;
    friend class TabletSchema;

    const SlotId _id;
    const TypeDescriptor _type;
    const TupleId _parent;
    const int _col_pos;
    bool _is_nullable;
    const std::string _col_name;
    const std::string _col_name_lower_case;

    const int32_t _col_unique_id;
    const PrimitiveType _col_type;

    // the idx of the slot in the tuple descriptor (0-based).
    // this is provided by the FE
    const int _slot_idx;

    // the idx of the slot in the llvm codegen'd tuple struct
    // this is set by TupleDescriptor during codegen and takes into account
    // leading null bytes.
    int _field_idx;

    const bool _is_materialized;

    const bool _is_key;
    const bool _need_materialize;
    const std::vector<std::string> _column_paths;

    const bool _is_auto_increment;
    const std::string _col_default_value;

    SlotDescriptor(const TSlotDescriptor& tdesc);
    SlotDescriptor(const PSlotDescriptor& pdesc);
};

// Base class for table descriptors.
class TableDescriptor {
public:
    TableDescriptor(const TTableDescriptor& tdesc);
    virtual ~TableDescriptor() = default;
    int num_cols() const { return _num_cols; }
    int num_clustering_cols() const { return _num_clustering_cols; }
    virtual std::string debug_string() const;

    // The first _num_clustering_cols columns by position are clustering
    // columns.
    bool is_clustering_col(const SlotDescriptor* slot_desc) const {
        return slot_desc->col_pos() < _num_clustering_cols;
    }

    ::doris::TTableType::type table_type() const { return _table_type; }
    const std::string& name() const { return _name; }
    const std::string& database() const { return _database; }
    int64_t table_id() const { return _table_id; }

private:
    ::doris::TTableType::type _table_type;
    std::string _name;
    std::string _database;
    int64_t _table_id;
    int _num_cols;
    int _num_clustering_cols;
};

class OlapTableDescriptor : public TableDescriptor {
public:
    OlapTableDescriptor(const TTableDescriptor& tdesc);
    std::string debug_string() const override;
};

class SchemaTableDescriptor : public TableDescriptor {
public:
    SchemaTableDescriptor(const TTableDescriptor& tdesc);
    ~SchemaTableDescriptor() override;
    std::string debug_string() const override;
    TSchemaTableType::type schema_table_type() const { return _schema_table_type; }

private:
    TSchemaTableType::type _schema_table_type;
};

class BrokerTableDescriptor : public TableDescriptor {
public:
    BrokerTableDescriptor(const TTableDescriptor& tdesc);
    ~BrokerTableDescriptor() override;
    std::string debug_string() const override;

private:
};

class HiveTableDescriptor : public TableDescriptor {
public:
    HiveTableDescriptor(const TTableDescriptor& tdesc);
    ~HiveTableDescriptor() override;
    std::string debug_string() const override;

private:
};

class IcebergTableDescriptor : public TableDescriptor {
public:
    IcebergTableDescriptor(const TTableDescriptor& tdesc);
    ~IcebergTableDescriptor() override;
    std::string debug_string() const override;

private:
};

class MaxComputeTableDescriptor : public TableDescriptor {
public:
    MaxComputeTableDescriptor(const TTableDescriptor& tdesc);
    ~MaxComputeTableDescriptor() override;
    std::string debug_string() const override;
    std::string region() const { return _region; }
    std::string project() const { return _project; }
    std::string table() const { return _table; }
    std::string odps_url() const { return _odps_url; }
    std::string tunnel_url() const { return _tunnel_url; }
    std::string access_key() const { return _access_key; }
    std::string secret_key() const { return _secret_key; }
    std::string public_access() const { return _public_access; }
    std::string endpoint() const { return _endpoint; }
    std::string quota() const { return _quota; }
    Status init_status() const { return _init_status; }

private:
    std::string _region; //deprecated
    std::string _project;
    std::string _table;
    std::string _odps_url;   //deprecated
    std::string _tunnel_url; //deprecated
    std::string _access_key;
    std::string _secret_key;
    std::string _public_access; //deprecated
    std::string _endpoint;
    std::string _quota;
    Status _init_status = Status::OK();
};

class TrinoConnectorTableDescriptor : public TableDescriptor {
public:
    TrinoConnectorTableDescriptor(const TTableDescriptor& tdesc);
    ~TrinoConnectorTableDescriptor() override;
    std::string debug_string() const override;

private:
};

class EsTableDescriptor : public TableDescriptor {
public:
    EsTableDescriptor(const TTableDescriptor& tdesc);
    ~EsTableDescriptor() override;
    std::string debug_string() const override;

private:
};

class MySQLTableDescriptor : public TableDescriptor {
public:
    MySQLTableDescriptor(const TTableDescriptor& tdesc);
    std::string debug_string() const override;
    std::string mysql_db() const { return _mysql_db; }
    std::string mysql_table() const { return _mysql_table; }
    std::string host() const { return _host; }
    std::string port() const { return _port; }
    std::string user() const { return _user; }
    std::string passwd() const { return _passwd; }
    std::string charset() const { return _charset; }

private:
    std::string _mysql_db;
    std::string _mysql_table;
    std::string _host;
    std::string _port;
    std::string _user;
    std::string _passwd;
    std::string _charset;
};

class ODBCTableDescriptor : public TableDescriptor {
public:
    ODBCTableDescriptor(const TTableDescriptor& tdesc);
    std::string debug_string() const override;
    std::string db() const { return _db; }
    std::string table() const { return _table; }
    std::string host() const { return _host; }
    std::string port() const { return _port; }
    std::string user() const { return _user; }
    std::string passwd() const { return _passwd; }
    std::string driver() const { return _driver; }
    TOdbcTableType::type type() const { return _type; }

private:
    std::string _db;
    std::string _table;
    std::string _host;
    std::string _port;
    std::string _user;
    std::string _passwd;
    std::string _driver;
    TOdbcTableType::type _type;
};

class JdbcTableDescriptor : public TableDescriptor {
public:
    JdbcTableDescriptor(const TTableDescriptor& tdesc);
    std::string debug_string() const override;
    int64_t jdbc_catalog_id() const { return _jdbc_catalog_id; }
    const std::string& jdbc_resource_name() const { return _jdbc_resource_name; }
    const std::string& jdbc_driver_url() const { return _jdbc_driver_url; }
    const std::string& jdbc_driver_class() const { return _jdbc_driver_class; }
    const std::string& jdbc_driver_checksum() const { return _jdbc_driver_checksum; }
    const std::string& jdbc_url() const { return _jdbc_url; }
    const std::string& jdbc_table_name() const { return _jdbc_table_name; }
    const std::string& jdbc_user() const { return _jdbc_user; }
    const std::string& jdbc_passwd() const { return _jdbc_passwd; }
    int32_t connection_pool_min_size() const { return _connection_pool_min_size; }
    int32_t connection_pool_max_size() const { return _connection_pool_max_size; }
    int32_t connection_pool_max_wait_time() const { return _connection_pool_max_wait_time; }
    int32_t connection_pool_max_life_time() const { return _connection_pool_max_life_time; }
    bool connection_pool_keep_alive() const { return _connection_pool_keep_alive; }

private:
    int64_t _jdbc_catalog_id;
    std::string _jdbc_resource_name;
    std::string _jdbc_driver_url;
    std::string _jdbc_driver_class;
    std::string _jdbc_driver_checksum;
    std::string _jdbc_url;
    std::string _jdbc_table_name;
    std::string _jdbc_user;
    std::string _jdbc_passwd;
    int32_t _connection_pool_min_size;
    int32_t _connection_pool_max_size;
    int32_t _connection_pool_max_wait_time;
    int32_t _connection_pool_max_life_time;
    bool _connection_pool_keep_alive;
};

class TupleDescriptor {
public:
    TupleDescriptor(TupleDescriptor&&) = delete;
    void operator=(const TupleDescriptor&) = delete;

    ~TupleDescriptor() {
        if (_own_slots) {
            for (SlotDescriptor* slot : _slots) {
                delete slot;
            }
        }
    }
    int num_materialized_slots() const { return _num_materialized_slots; }
    const std::vector<SlotDescriptor*>& slots() const { return _slots; }

    bool has_varlen_slots() const { return _has_varlen_slots; }
    const TableDescriptor* table_desc() const { return _table_desc; }

    TupleId id() const { return _id; }

    std::string debug_string() const;

    void to_protobuf(PTupleDescriptor* ptuple) const;

private:
    friend class DescriptorTbl;
    friend class SchemaScanner;
    friend class OlapTableSchemaParam;
    friend class PInternalServiceImpl;
    friend class RowIdStorageReader;
    friend class TabletSchema;

    const TupleId _id;
    TableDescriptor* _table_desc = nullptr;
    int _num_materialized_slots;
    std::vector<SlotDescriptor*> _slots; // contains all slots

    // Provide quick way to check if there are variable length slots.
    // True if _string_slots or _collection_slots have entries.
    bool _has_varlen_slots;
    bool _own_slots = false;

    TupleDescriptor(const TTupleDescriptor& tdesc, bool own_slot = false);
    TupleDescriptor(const PTupleDescriptor& tdesc, bool own_slot = false);

    void add_slot(SlotDescriptor* slot);
};

class DescriptorTbl {
public:
    // Creates a descriptor tbl within 'pool' from thrift_tbl and returns it via 'tbl'.
    // Returns OK on success, otherwise error (in which case 'tbl' will be unset).
    static Status create(ObjectPool* pool, const TDescriptorTable& thrift_tbl, DescriptorTbl** tbl);

    TableDescriptor* get_table_descriptor(TableId id) const;
    TupleDescriptor* get_tuple_descriptor(TupleId id) const;
    SlotDescriptor* get_slot_descriptor(SlotId id) const;
    const std::vector<TTupleId>& get_row_tuples() const { return _row_tuples; }

    // return all registered tuple descriptors
    std::vector<TupleDescriptor*> get_tuple_descs() const {
        std::vector<TupleDescriptor*> descs;

        for (auto it : _tuple_desc_map) {
            descs.push_back(it.second);
        }

        return descs;
    }

    std::string debug_string() const;

private:
    using TableDescriptorMap = std::unordered_map<TableId, TableDescriptor*>;
    using TupleDescriptorMap = std::unordered_map<TupleId, TupleDescriptor*>;
    using SlotDescriptorMap = std::unordered_map<SlotId, SlotDescriptor*>;

    TableDescriptorMap _tbl_desc_map;
    TupleDescriptorMap _tuple_desc_map;
    SlotDescriptorMap _slot_desc_map;
    std::vector<TTupleId> _row_tuples;

    DescriptorTbl() = default;
};

#define RETURN_IF_INVALID_TUPLE_IDX(tuple_id, tuple_idx)                                         \
    do {                                                                                         \
        if (UNLIKELY(RowDescriptor::INVALID_IDX == tuple_idx)) {                                 \
            return Status::InternalError("failed to get tuple idx with tuple id: {}", tuple_id); \
        }                                                                                        \
    } while (false)

// Records positions of tuples within row produced by ExecNode.
// TODO: this needs to differentiate between tuples contained in row
// and tuples produced by ExecNode (parallel to PlanNode.rowTupleIds and
// PlanNode.tupleIds); right now, we conflate the two (and distinguish based on
// context; for instance, HdfsScanNode uses these tids to create row batches, ie, the
// first case, whereas TopNNode uses these tids to copy output rows, ie, the second
// case)
class RowDescriptor {
public:
    RowDescriptor(const DescriptorTbl& desc_tbl, const std::vector<TTupleId>& row_tuples,
                  const std::vector<bool>& nullable_tuples);

    // standard copy c'tor, made explicit here
    RowDescriptor(const RowDescriptor& desc)
            : _tuple_desc_map(desc._tuple_desc_map),
              _tuple_idx_nullable_map(desc._tuple_idx_nullable_map),
              _tuple_idx_map(desc._tuple_idx_map),
              _has_varlen_slots(desc._has_varlen_slots) {
        auto it = desc._tuple_desc_map.begin();
        for (; it != desc._tuple_desc_map.end(); ++it) {
            _num_materialized_slots += (*it)->num_materialized_slots();
            _num_slots += (*it)->slots().size();
        }
    }

    RowDescriptor(TupleDescriptor* tuple_desc, bool is_nullable);

    RowDescriptor(const RowDescriptor& lhs_row_desc, const RowDescriptor& rhs_row_desc);

    // dummy descriptor, needed for the JNI EvalPredicate() function
    RowDescriptor() = default;

    int num_materialized_slots() const { return _num_materialized_slots; }

    int num_slots() const { return _num_slots; }

    static const int INVALID_IDX;

    // Returns INVALID_IDX if id not part of this row.
    int get_tuple_idx(TupleId id) const;

    // Return true if any Tuple has variable length slots.
    bool has_varlen_slots() const { return _has_varlen_slots; }

    // Return descriptors for all tuples in this row, in order of appearance.
    const std::vector<TupleDescriptor*>& tuple_descriptors() const { return _tuple_desc_map; }

    // Populate row_tuple_ids with our ids.
    void to_thrift(std::vector<TTupleId>* row_tuple_ids);
    void to_protobuf(google::protobuf::RepeatedField<google::protobuf::int32>* row_tuple_ids) const;

    // Return true if the tuple ids of this descriptor are a prefix
    // of the tuple ids of other_desc.
    bool is_prefix_of(const RowDescriptor& other_desc) const;

    // Return true if the tuple ids of this descriptor match tuple ids of other desc.
    bool equals(const RowDescriptor& other_desc) const;

    std::string debug_string() const;

    int get_column_id(int slot_id, bool force_materialize_slot = false) const;

private:
    // Initializes tupleIdxMap during c'tor using the _tuple_desc_map.
    void init_tuple_idx_map();

    // Initializes _has_varlen_slots during c'tor using the _tuple_desc_map.
    void init_has_varlen_slots();

    // map from position of tuple w/in row to its descriptor
    std::vector<TupleDescriptor*> _tuple_desc_map;

    // _tuple_idx_nullable_map[i] is true if tuple i can be null
    std::vector<bool> _tuple_idx_nullable_map;

    // map from TupleId to position of tuple w/in row
    std::vector<int> _tuple_idx_map;

    // Provide quick way to check if there are variable length slots.
    bool _has_varlen_slots = false;

    int _num_materialized_slots = 0;
    int _num_slots = 0;
};

} // namespace doris
