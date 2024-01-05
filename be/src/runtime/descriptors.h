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

#include <google/protobuf/repeated_field.h>
#include <google/protobuf/stubs/common.h>

#include <ostream>
#include <unordered_map>
#include <vector>

#include "common/global_types.h"
#include "common/status.h"
#include "gen_cpp/Descriptors_types.h"     // for TTupleId
#include "gen_cpp/FrontendService_types.h" // for TTupleId
#include "gen_cpp/Types_types.h"
#include "runtime/define_primitive_type.h"
#include "runtime/types.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
struct ColumnWithTypeAndName;
}

namespace doris {

class ObjectPool;
class TDescriptorTable;
class TSlotDescriptor;
class TTupleDescriptor;
class Expr;
class RuntimeState;
class SchemaScanner;
class OlapTableSchemaParam;
class PTupleDescriptor;
class PSlotDescriptor;

// Location information for null indicator bit for particular slot.
// For non-nullable slots, the byte_offset will be 0 and the bit_mask will be 0.
// This allows us to do the NullIndicatorOffset operations (tuple + byte_offset &/|
// bit_mask) regardless of whether the slot is nullable or not.
// This is more efficient than branching to check if the slot is non-nullable.
struct NullIndicatorOffset {
    int byte_offset;
    uint8_t bit_mask;  // to extract null indicator
    int8_t bit_offset; // only used to serialize, from 1 to 8, invalid null value
                       // bit_offset is -1.

    NullIndicatorOffset(int byte_offset, int bit_offset_)
            : byte_offset(byte_offset),
              bit_mask(bit_offset_ == -1 ? 0 : 1 << (7 - bit_offset_)),
              bit_offset(bit_offset_) {
        DCHECK_LE(bit_offset_, 8);
    }

    bool equals(const NullIndicatorOffset& o) const {
        return this->byte_offset == o.byte_offset && this->bit_mask == o.bit_mask;
    }

    std::string debug_string() const;
};

std::ostream& operator<<(std::ostream& os, const NullIndicatorOffset& null_indicator);

class TupleDescriptor;

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
    int tuple_offset() const { return _tuple_offset; }
    const NullIndicatorOffset& null_indicator_offset() const { return _null_indicator_offset; }
    bool is_materialized() const { return _is_materialized; }
    bool is_nullable() const { return _null_indicator_offset.bit_mask != 0; }

    int slot_size() const { return _slot_size; }

    const std::string& col_name() const { return _col_name; }
    const std::string& col_name_lower_case() const { return _col_name_lower_case; }

    /// Return true if the physical layout of this descriptor matches the physical layout
    /// of other_desc, but not necessarily ids.
    bool layout_equals(const SlotDescriptor& other_desc) const;

    void to_protobuf(PSlotDescriptor* pslot) const;

    std::string debug_string() const;

    vectorized::MutableColumnPtr get_empty_mutable_column() const;

    doris::vectorized::DataTypePtr get_data_type_ptr() const;

    int32_t col_unique_id() const { return _col_unique_id; }
    PrimitiveType col_type() const { return _col_type; }

private:
    friend class DescriptorTbl;
    friend class TupleDescriptor;
    friend class SchemaScanner;
    friend class OlapTableSchemaParam;

    const SlotId _id;
    const TypeDescriptor _type;
    const TupleId _parent;
    const int _col_pos;
    const int _tuple_offset;
    const NullIndicatorOffset _null_indicator_offset;
    const std::string _col_name;
    const std::string _col_name_lower_case;

    const int32_t _col_unique_id;
    const PrimitiveType _col_type;

    // the idx of the slot in the tuple descriptor (0-based).
    // this is provided by the FE
    const int _slot_idx;

    // the byte size of this slot.
    const int _slot_size;

    // the idx of the slot in the llvm codegen'd tuple struct
    // this is set by TupleDescriptor during codegen and takes into account
    // leading null bytes.
    int _field_idx;

    const bool _is_materialized;

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

    const std::string& name() const { return _name; }
    const std::string& database() const { return _database; }

private:
    std::string _name;
    std::string _database;
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
    const std::string mysql_db() const { return _mysql_db; }
    const std::string mysql_table() const { return _mysql_table; }
    const std::string host() const { return _host; }
    const std::string port() const { return _port; }
    const std::string user() const { return _user; }
    const std::string passwd() const { return _passwd; }
    const std::string charset() const { return _charset; }

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
    const std::string db() const { return _db; }
    const std::string table() const { return _table; }
    const std::string host() const { return _host; }
    const std::string port() const { return _port; }
    const std::string user() const { return _user; }
    const std::string passwd() const { return _passwd; }
    const std::string driver() const { return _driver; }
    const TOdbcTableType::type type() const { return _type; }

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
    const std::string& jdbc_resource_name() const { return _jdbc_resource_name; }
    const std::string& jdbc_driver_url() const { return _jdbc_driver_url; }
    const std::string& jdbc_driver_class() const { return _jdbc_driver_class; }
    const std::string& jdbc_driver_checksum() const { return _jdbc_driver_checksum; }
    const std::string& jdbc_url() const { return _jdbc_url; }
    const std::string& jdbc_table_name() const { return _jdbc_table_name; }
    const std::string& jdbc_user() const { return _jdbc_user; }
    const std::string& jdbc_passwd() const { return _jdbc_passwd; }

private:
    std::string _jdbc_resource_name;
    std::string _jdbc_driver_url;
    std::string _jdbc_driver_class;
    std::string _jdbc_driver_checksum;
    std::string _jdbc_url;
    std::string _jdbc_table_name;
    std::string _jdbc_user;
    std::string _jdbc_passwd;
};

class TupleDescriptor {
public:
    // virtual ~TupleDescriptor() {}
    int64_t byte_size() const { return _byte_size; }
    int num_materialized_slots() const { return _num_materialized_slots; }
    int num_null_slots() const { return _num_null_slots; }
    int num_null_bytes() const { return _num_null_bytes; }
    const std::vector<SlotDescriptor*>& slots() const { return _slots; }
    const std::vector<SlotDescriptor*>& string_slots() const { return _string_slots; }
    const std::vector<SlotDescriptor*>& no_string_slots() const { return _no_string_slots; }
    const std::vector<SlotDescriptor*>& collection_slots() const { return _collection_slots; }

    bool has_varlen_slots() const {
        { return _has_varlen_slots; }
    }
    const TableDescriptor* table_desc() const { return _table_desc; }

    static bool is_var_length(const std::vector<TupleDescriptor*>& descs) {
        for (auto desc : descs) {
            if (desc->string_slots().size() > 0) {
                return true;
            }
            if (desc->collection_slots().size() > 0) {
                return true;
            }
        }
        return false;
    }

    TupleId id() const { return _id; }

    /// Return true if the physical layout of this descriptor matches that of other_desc,
    /// but not necessarily the id.
    bool layout_equals(const TupleDescriptor& other_desc) const;

    std::string debug_string() const;

    void to_protobuf(PTupleDescriptor* ptuple) const;

private:
    friend class DescriptorTbl;
    friend class SchemaScanner;
    friend class OlapTableSchemaParam;

    const TupleId _id;
    TableDescriptor* _table_desc;
    int64_t _byte_size;
    int _num_null_slots;
    int _num_null_bytes;
    int _num_materialized_slots;
    std::vector<SlotDescriptor*> _slots;        // contains all slots
    std::vector<SlotDescriptor*> _string_slots; // contains only materialized string slots
    // contains only materialized slots except string slots
    std::vector<SlotDescriptor*> _no_string_slots;
    // _collection_slots
    std::vector<SlotDescriptor*> _collection_slots;

    // Provide quick way to check if there are variable length slots.
    // True if _string_slots or _collection_slots have entries.
    bool _has_varlen_slots;

    TupleDescriptor(const TTupleDescriptor& tdesc);
    TupleDescriptor(const PTupleDescriptor& tdesc);

    void add_slot(SlotDescriptor* slot);

    /// Returns slots in their physical order.
    std::vector<SlotDescriptor*> slots_ordered_by_idx() const;
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
        _num_materialized_slots = 0;
        _num_null_slots = 0;
        std::vector<TupleDescriptor*>::const_iterator it = desc._tuple_desc_map.begin();
        for (; it != desc._tuple_desc_map.end(); ++it) {
            _num_materialized_slots += (*it)->num_materialized_slots();
            _num_null_slots += (*it)->num_null_slots();
        }
        _num_null_bytes = (_num_null_slots + 7) / 8;
    }

    RowDescriptor(TupleDescriptor* tuple_desc, bool is_nullable);

    RowDescriptor(const RowDescriptor& lhs_row_desc, const RowDescriptor& rhs_row_desc);

    // dummy descriptor, needed for the JNI EvalPredicate() function
    RowDescriptor() = default;

    // Returns total size in bytes.
    // TODO: also take avg string lengths into account, ie, change this
    // to GetAvgRowSize()
    int get_row_size() const;

    int num_materialized_slots() const { return _num_materialized_slots; }

    int num_null_slots() const { return _num_null_slots; }

    int num_null_bytes() const { return _num_null_bytes; }

    static const int INVALID_IDX;

    // Returns INVALID_IDX if id not part of this row.
    int get_tuple_idx(TupleId id) const;

    // Return true if the Tuple of the given Tuple index is nullable.
    bool tuple_is_nullable(int tuple_idx) const;

    // Return true if any Tuple of the row is nullable.
    bool is_any_tuple_nullable() const;

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

    /// Return true if the physical layout of this descriptor matches the physical layout
    /// of other_desc, but not necessarily the ids.
    bool layout_equals(const RowDescriptor& other_desc) const;

    /// Return true if the tuples of this descriptor are a prefix of the tuples of
    /// other_desc. Tuples are compared by their physical layout and not by ids.
    bool layout_is_prefix_of(const RowDescriptor& other_desc) const;

    std::string debug_string() const;

    int get_column_id(int slot_id) const;

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
    bool _has_varlen_slots;

    int _num_materialized_slots;
    int _num_null_slots;
    int _num_null_bytes;
};

} // namespace doris
