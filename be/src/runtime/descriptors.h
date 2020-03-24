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

#ifndef DORIS_BE_RUNTIME_DESCRIPTORS_H
#define DORIS_BE_RUNTIME_DESCRIPTORS_H

#include <vector>
#include <tr1/unordered_map>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <ostream>

#include <google/protobuf/repeated_field.h>
#include <google/protobuf/stubs/common.h>

#include "common/status.h"
#include "common/global_types.h"
#include "gen_cpp/Descriptors_types.h"  // for TTupleId
#include "gen_cpp/FrontendService_types.h"  // for TTupleId
#include "gen_cpp/Types_types.h"
#include "runtime/types.h"

namespace doris {

class ObjectPool;
class TDescriptorTable;
class TSlotDescriptor;
class TTable;
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
    uint8_t bit_offset; // only used to serialize, from 1 to 8

    NullIndicatorOffset(int byte_offset, int bit_offset_)
        : byte_offset(byte_offset),
        bit_mask(bit_offset_ == -1 ? 0 : 1 << (7 - bit_offset_)),
        bit_offset(bit_offset_) {
    }
 
    bool equals(const NullIndicatorOffset& o) const {
        return this->byte_offset == o.byte_offset && this->bit_mask == o.bit_mask;
    }

    std::string debug_string() const;
};

std::ostream& operator<<(std::ostream& os, const NullIndicatorOffset& null_indicator);

class SlotDescriptor {
public:
    // virtual ~SlotDescriptor() {};
    SlotId id() const {
        return _id;
    }
    const TypeDescriptor& type() const {
        return _type;
    }
    TupleId parent() const {
        return _parent;
    }
    // Returns the column index of this slot, including partition keys.
    // (e.g., col_pos - num_partition_keys = the table column this slot corresponds to)
    int col_pos() const {
        return _col_pos;
    }
    // Returns the field index in the generated llvm struct for this slot's tuple
    int field_idx() const {
        return _field_idx;
    }
    int tuple_offset() const {
        return _tuple_offset;
    }
    const NullIndicatorOffset& null_indicator_offset() const {
        return _null_indicator_offset;
    }
    bool is_materialized() const {
        return _is_materialized;
    }
    bool is_nullable() const {
        return _null_indicator_offset.bit_mask != 0;
    }

    int slot_size() const {
        return _slot_size;
    }

    const std::string& col_name() const {
        return _col_name;
    }

    /// Return true if the physical layout of this descriptor matches the physical layout
    /// of other_desc, but not necessarily ids.
    bool layout_equals(const SlotDescriptor& other_desc) const;

    void to_protobuf(PSlotDescriptor* pslot) const;

    std::string debug_string() const;

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
    virtual ~TableDescriptor() {}
    int num_cols() const {
        return _num_cols;
    }
    int num_clustering_cols() const {
        return _num_clustering_cols;
    }
    virtual std::string debug_string() const;

    // The first _num_clustering_cols columns by position are clustering
    // columns.
    bool is_clustering_col(const SlotDescriptor* slot_desc) const {
        return slot_desc->col_pos() < _num_clustering_cols;
    }

    const std::string& name() const {
        return _name;
    }
    const std::string& database() const {
        return _database;
    }

private:
    std::string _name;
    std::string _database;
    TableId _id;
    int _num_cols;
    int _num_clustering_cols;
};

class OlapTableDescriptor : public TableDescriptor {
public :
    OlapTableDescriptor(const TTableDescriptor& tdesc);
    virtual std::string debug_string() const;
};

class SchemaTableDescriptor : public TableDescriptor {
public :
    SchemaTableDescriptor(const TTableDescriptor& tdesc);
    virtual ~SchemaTableDescriptor();
    virtual std::string debug_string() const;
    TSchemaTableType::type schema_table_type() const {
        return _schema_table_type;
    }
private :
    TSchemaTableType::type _schema_table_type;
};

class BrokerTableDescriptor : public TableDescriptor {
public :
    BrokerTableDescriptor(const TTableDescriptor& tdesc);
    virtual ~BrokerTableDescriptor();
    virtual std::string debug_string() const;
private :
};

class EsTableDescriptor : public TableDescriptor {
public :
    EsTableDescriptor(const TTableDescriptor& tdesc);
    virtual ~EsTableDescriptor();
    virtual std::string debug_string() const;
private :
};

class MySQLTableDescriptor : public TableDescriptor {
public:
    MySQLTableDescriptor(const TTableDescriptor& tdesc);
    virtual std::string debug_string() const;
    const std::string mysql_db() const {
        return _mysql_db;
    }
    const std::string mysql_table() const {
        return _mysql_table;
    }
    const std::string host() const {
        return _host;
    }
    const std::string port() const {
        return _port;
    }
    const std::string user() const {
        return _user;
    }
    const std::string passwd() const {
        return _passwd;
    }
private:
    std::string _mysql_db;
    std::string _mysql_table;
    std::string _host;
    std::string _port;
    std::string _user;
    std::string _passwd;
};

class TupleDescriptor {
public:
    // virtual ~TupleDescriptor() {}
    int byte_size() const {
        return _byte_size;
    }
    int num_null_slots() const {
        return _num_null_slots;
    }
    int num_null_bytes() const {
        return _num_null_bytes;
    }
    const std::vector<SlotDescriptor*>& slots() const {
        return _slots;
    }
    const std::vector<SlotDescriptor*>& string_slots() const {
        return _string_slots;
    }
    const std::vector<SlotDescriptor*>& no_string_slots() const {
        return _no_string_slots;
    }
    bool has_varlen_slots() const { {
        return _has_varlen_slots; }
    }
    const TableDescriptor* table_desc() const {
        return _table_desc;
    }

    static bool is_var_length(const std::vector<TupleDescriptor*>& descs) {
        for (auto desc : descs) {
            if (desc->string_slots().size() > 0) {
                return true;
            }
        }
        return false;
    }

    TupleId id() const {
        return _id;
    }

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
    int _byte_size;
    int _num_null_slots;
    int _num_null_bytes;
    int _num_materialized_slots;
    std::vector<SlotDescriptor*> _slots;  // contains all slots
    std::vector<SlotDescriptor*> _string_slots;  // contains only materialized string slots
    // contains only materialized slots except string slots
    std::vector<SlotDescriptor*> _no_string_slots;

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
    static Status create(ObjectPool* pool, const TDescriptorTable& thrift_tbl,
                         DescriptorTbl** tbl);

    TableDescriptor* get_table_descriptor(TableId id) const;
    TupleDescriptor* get_tuple_descriptor(TupleId id) const;
    SlotDescriptor* get_slot_descriptor(SlotId id) const;

    // return all registered tuple descriptors
    void get_tuple_descs(std::vector<TupleDescriptor*>* descs) const;

    std::string debug_string() const;

private:
    typedef std::tr1::unordered_map<TableId, TableDescriptor*> TableDescriptorMap;
    typedef std::tr1::unordered_map<TupleId, TupleDescriptor*> TupleDescriptorMap;
    typedef std::tr1::unordered_map<SlotId, SlotDescriptor*> SlotDescriptorMap;

    TableDescriptorMap _tbl_desc_map;
    TupleDescriptorMap _tuple_desc_map;
    SlotDescriptorMap _slot_desc_map;

    DescriptorTbl(): _tbl_desc_map(), _tuple_desc_map(), _slot_desc_map() {}
};

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
    RowDescriptor(const RowDescriptor& desc) :
            _tuple_desc_map(desc._tuple_desc_map),
            _tuple_idx_nullable_map(desc._tuple_idx_nullable_map),
            _tuple_idx_map(desc._tuple_idx_map),
            _has_varlen_slots(desc._has_varlen_slots) {
        _num_null_slots = 0;
        std::vector<TupleDescriptor*>::const_iterator it = desc._tuple_desc_map.begin();
        for (; it != desc._tuple_desc_map.end(); ++it)  {
            _num_null_slots += (*it)->num_null_slots();
        }
        _num_null_bytes = (_num_null_slots + 7) / 8;
    }

    RowDescriptor(TupleDescriptor* tuple_desc, bool is_nullable);

    // dummy descriptor, needed for the JNI EvalPredicate() function
    RowDescriptor() {}

    // Returns total size in bytes.
    // TODO: also take avg string lengths into account, ie, change this
    // to GetAvgRowSize()
    int get_row_size() const;

    int num_null_slots() const {
        return _num_null_slots;
    }

    int num_null_bytes() const {
        return _num_null_bytes;
    }

    static const int INVALID_IDX;

    // Returns INVALID_IDX if id not part of this row.
    int get_tuple_idx(TupleId id) const;

    // Return true if the Tuple of the given Tuple index is nullable.
    bool tuple_is_nullable(int tuple_idx) const;

    // Return true if any Tuple of the row is nullable.
    bool is_any_tuple_nullable() const;

    // Return true if any Tuple has variable length slots.
    bool has_varlen_slots() const {
        return _has_varlen_slots;
    }

    // Return descriptors for all tuples in this row, in order of appearance.
    const std::vector<TupleDescriptor*>& tuple_descriptors() const {
        return _tuple_desc_map;
    }

    // Populate row_tuple_ids with our ids.
    void to_thrift(std::vector<TTupleId>* row_tuple_ids);
    void to_protobuf(
        google::protobuf::RepeatedField<google::protobuf::int32 >* row_tuple_ids);

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

    int _num_null_slots;
    int _num_null_bytes;
};

}

#endif
