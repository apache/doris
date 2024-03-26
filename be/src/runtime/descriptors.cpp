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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/descriptors.cc
// and modified by Doris

#include "runtime/descriptors.h"

#include <gen_cpp/Types_types.h>

#include <boost/algorithm/string/join.hpp>
#include <ios>
#include <sstream>

#include "common/object_pool.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/descriptors.pb.h"
#include "runtime/primitive_type.h"
#include "util/string_util.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"

namespace doris {
using boost::algorithm::join;

const int RowDescriptor::INVALID_IDX = -1;
std::string NullIndicatorOffset::debug_string() const {
    std::stringstream out;
    out << "(offset=" << byte_offset << " mask=" << std::hex << static_cast<int>(bit_mask)
        << std::dec << ")";
    return out.str();
}

std::ostream& operator<<(std::ostream& os, const NullIndicatorOffset& null_indicator) {
    os << null_indicator.debug_string();
    return os;
}

SlotDescriptor::SlotDescriptor(const TSlotDescriptor& tdesc)
        : _id(tdesc.id),
          _type(TypeDescriptor::from_thrift(tdesc.slotType)),
          _parent(tdesc.parent),
          _col_pos(tdesc.columnPos),
          _tuple_offset(tdesc.byteOffset),
          _null_indicator_offset(tdesc.nullIndicatorByte, tdesc.nullIndicatorBit),
          _col_name(tdesc.colName),
          _col_name_lower_case(to_lower(tdesc.colName)),
          _col_unique_id(tdesc.col_unique_id),
          _col_type(thrift_to_type(tdesc.primitive_type)),
          _slot_idx(tdesc.slotIdx),
          _slot_size(_type.get_slot_size()),
          _field_idx(-1),
          _is_materialized(tdesc.isMaterialized) {}

SlotDescriptor::SlotDescriptor(const PSlotDescriptor& pdesc)
        : _id(pdesc.id()),
          _type(TypeDescriptor::from_protobuf(pdesc.slot_type())),
          _parent(pdesc.parent()),
          _col_pos(pdesc.column_pos()),
          _tuple_offset(pdesc.byte_offset()),
          _null_indicator_offset(pdesc.null_indicator_byte(), pdesc.null_indicator_bit()),
          _col_name(pdesc.col_name()),
          _col_name_lower_case(to_lower(pdesc.col_name())),
          _col_unique_id(-1),
          _col_type(static_cast<PrimitiveType>(pdesc.col_type())),
          _slot_idx(pdesc.slot_idx()),
          _slot_size(_type.get_slot_size()),
          _field_idx(-1),
          _is_materialized(pdesc.is_materialized()) {}

void SlotDescriptor::to_protobuf(PSlotDescriptor* pslot) const {
    pslot->set_id(_id);
    pslot->set_parent(_parent);
    _type.to_protobuf(pslot->mutable_slot_type());
    pslot->set_column_pos(_col_pos);
    pslot->set_byte_offset(_tuple_offset);
    pslot->set_null_indicator_byte(_null_indicator_offset.byte_offset);
    pslot->set_null_indicator_bit(_null_indicator_offset.bit_offset);
    DCHECK_LE(_null_indicator_offset.bit_offset, 8);
    pslot->set_col_name(_col_name);
    pslot->set_slot_idx(_slot_idx);
    pslot->set_is_materialized(_is_materialized);
    pslot->set_col_type(_col_type);
}

vectorized::MutableColumnPtr SlotDescriptor::get_empty_mutable_column() const {
    auto data_type = get_data_type_ptr();
    if (data_type) {
        return data_type->create_column();
    }
    return nullptr;
}

vectorized::DataTypePtr SlotDescriptor::get_data_type_ptr() const {
    return vectorized::DataTypeFactory::instance().create_data_type(type(), is_nullable());
}

std::string SlotDescriptor::debug_string() const {
    std::stringstream out;
    out << "Slot(id=" << _id << " type=" << _type << " col=" << _col_pos
        << ", colname=" << _col_name << " offset=" << _tuple_offset
        << " null=" << _null_indicator_offset.debug_string() << ")";
    return out.str();
}

TableDescriptor::TableDescriptor(const TTableDescriptor& tdesc)
        : _name(tdesc.tableName),
          _database(tdesc.dbName),
          _num_cols(tdesc.numCols),
          _num_clustering_cols(tdesc.numClusteringCols) {}

std::string TableDescriptor::debug_string() const {
    std::stringstream out;
    out << "#cols=" << _num_cols << " #clustering_cols=" << _num_clustering_cols;
    return out.str();
}

OlapTableDescriptor::OlapTableDescriptor(const TTableDescriptor& tdesc) : TableDescriptor(tdesc) {}

std::string OlapTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "OlapTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

SchemaTableDescriptor::SchemaTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc), _schema_table_type(tdesc.schemaTable.tableType) {}
SchemaTableDescriptor::~SchemaTableDescriptor() {}

std::string SchemaTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "SchemaTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

BrokerTableDescriptor::BrokerTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc) {}

BrokerTableDescriptor::~BrokerTableDescriptor() {}

std::string BrokerTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "BrokerTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

HiveTableDescriptor::HiveTableDescriptor(const TTableDescriptor& tdesc) : TableDescriptor(tdesc) {}

HiveTableDescriptor::~HiveTableDescriptor() {}

std::string HiveTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "HiveTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

IcebergTableDescriptor::IcebergTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc) {}

IcebergTableDescriptor::~IcebergTableDescriptor() {}

std::string IcebergTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "IcebergTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

EsTableDescriptor::EsTableDescriptor(const TTableDescriptor& tdesc) : TableDescriptor(tdesc) {}

EsTableDescriptor::~EsTableDescriptor() {}

std::string EsTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "EsTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

MySQLTableDescriptor::MySQLTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc),
          _mysql_db(tdesc.mysqlTable.db),
          _mysql_table(tdesc.mysqlTable.table),
          _host(tdesc.mysqlTable.host),
          _port(tdesc.mysqlTable.port),
          _user(tdesc.mysqlTable.user),
          _passwd(tdesc.mysqlTable.passwd),
          _charset(tdesc.mysqlTable.charset) {}

std::string MySQLTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "MySQLTable(" << TableDescriptor::debug_string() << " _db" << _mysql_db
        << " table=" << _mysql_table << " host=" << _host << " port=" << _port << " user=" << _user
        << " passwd=" << _passwd << " charset=" << _charset;
    return out.str();
}

ODBCTableDescriptor::ODBCTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc),
          _db(tdesc.odbcTable.db),
          _table(tdesc.odbcTable.table),
          _host(tdesc.odbcTable.host),
          _port(tdesc.odbcTable.port),
          _user(tdesc.odbcTable.user),
          _passwd(tdesc.odbcTable.passwd),
          _driver(tdesc.odbcTable.driver),
          _type(tdesc.odbcTable.type) {}

std::string ODBCTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "ODBCTable(" << TableDescriptor::debug_string() << " _db" << _db << " table=" << _table
        << " host=" << _host << " port=" << _port << " user=" << _user << " passwd=" << _passwd
        << " driver=" << _driver << " type" << _type;
    return out.str();
}

JdbcTableDescriptor::JdbcTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc),
          _jdbc_resource_name(tdesc.jdbcTable.jdbc_resource_name),
          _jdbc_driver_url(tdesc.jdbcTable.jdbc_driver_url),
          _jdbc_driver_class(tdesc.jdbcTable.jdbc_driver_class),
          _jdbc_driver_checksum(tdesc.jdbcTable.jdbc_driver_checksum),
          _jdbc_url(tdesc.jdbcTable.jdbc_url),
          _jdbc_table_name(tdesc.jdbcTable.jdbc_table_name),
          _jdbc_user(tdesc.jdbcTable.jdbc_user),
          _jdbc_passwd(tdesc.jdbcTable.jdbc_password) {}

std::string JdbcTableDescriptor::debug_string() const {
    fmt::memory_buffer buf;
    fmt::format_to(buf,
                   "JDBCTable({} ,_jdbc_resource_name={} ,_jdbc_driver_url={} "
                   ",_jdbc_driver_class={} ,_jdbc_driver_checksum={} ,_jdbc_url={} "
                   ",_jdbc_table_name={} ,_jdbc_user={} ,_jdbc_passwd={})",
                   TableDescriptor::debug_string(), _jdbc_resource_name, _jdbc_driver_url,
                   _jdbc_driver_class, _jdbc_driver_checksum, _jdbc_url, _jdbc_table_name,
                   _jdbc_user, _jdbc_passwd);
    return fmt::to_string(buf);
}

TupleDescriptor::TupleDescriptor(const TTupleDescriptor& tdesc)
        : _id(tdesc.id),
          _table_desc(nullptr),
          _byte_size(tdesc.byteSize),
          _num_null_bytes(tdesc.numNullBytes),
          _num_materialized_slots(0),
          _slots(),
          _has_varlen_slots(false) {
    if (false == tdesc.__isset.numNullSlots) {
        //be compatible for existing tables with no nullptr value
        _num_null_slots = 0;
    } else {
        _num_null_slots = tdesc.numNullSlots;
    }
}

TupleDescriptor::TupleDescriptor(const PTupleDescriptor& pdesc)
        : _id(pdesc.id()),
          _table_desc(nullptr),
          _byte_size(pdesc.byte_size()),
          _num_null_bytes(pdesc.num_null_bytes()),
          _num_materialized_slots(0),
          _slots(),
          _has_varlen_slots(false) {
    if (!pdesc.has_num_null_slots()) {
        //be compatible for existing tables with no nullptr value
        _num_null_slots = 0;
    } else {
        _num_null_slots = pdesc.num_null_slots();
    }
}

void TupleDescriptor::add_slot(SlotDescriptor* slot) {
    _slots.push_back(slot);

    if (slot->is_materialized()) {
        ++_num_materialized_slots;

        if (slot->type().is_string_type()) {
            _string_slots.push_back(slot);
            _has_varlen_slots = true;
        } else if (slot->type().is_collection_type()) {
            _collection_slots.push_back(slot);
            _has_varlen_slots = true;
        } else {
            _no_string_slots.push_back(slot);
        }
    }
}

std::vector<SlotDescriptor*> TupleDescriptor::slots_ordered_by_idx() const {
    std::vector<SlotDescriptor*> sorted_slots(slots().size());
    for (SlotDescriptor* slot : slots()) {
        sorted_slots[slot->_slot_idx] = slot;
    }
    return sorted_slots;
}

bool TupleDescriptor::layout_equals(const TupleDescriptor& other_desc) const {
    if (byte_size() != other_desc.byte_size()) return false;
    if (slots().size() != other_desc.slots().size()) return false;

    std::vector<SlotDescriptor*> slots = slots_ordered_by_idx();
    std::vector<SlotDescriptor*> other_slots = other_desc.slots_ordered_by_idx();
    for (int i = 0; i < slots.size(); ++i) {
        if (!slots[i]->layout_equals(*other_slots[i])) return false;
    }
    return true;
}

void TupleDescriptor::to_protobuf(PTupleDescriptor* ptuple) const {
    ptuple->Clear();
    ptuple->set_id(_id);
    ptuple->set_byte_size(_byte_size);
    ptuple->set_num_null_bytes(_num_null_bytes);
    ptuple->set_table_id(-1);
    ptuple->set_num_null_slots(_num_null_slots);
}

std::string TupleDescriptor::debug_string() const {
    std::stringstream out;
    out << "Tuple(id=" << _id << " size=" << _byte_size;
    if (_table_desc != nullptr) {
        //out << " " << _table_desc->debug_string();
    }

    out << " slots=[";
    for (size_t i = 0; i < _slots.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << _slots[i]->debug_string();
    }

    out << "]";
    out << " has_varlen_slots=" << _has_varlen_slots;
    out << ")";
    return out.str();
}

RowDescriptor::RowDescriptor(const DescriptorTbl& desc_tbl, const std::vector<TTupleId>& row_tuples,
                             const std::vector<bool>& nullable_tuples)
        : _tuple_idx_nullable_map(nullable_tuples) {
    DCHECK(nullable_tuples.size() == row_tuples.size())
            << "nullable_tuples size " << nullable_tuples.size() << " != row_tuples size "
            << row_tuples.size();
    DCHECK_GT(row_tuples.size(), 0);
    _num_materialized_slots = 0;
    _num_null_slots = 0;

    for (int i = 0; i < row_tuples.size(); ++i) {
        TupleDescriptor* tupleDesc = desc_tbl.get_tuple_descriptor(row_tuples[i]);
        _num_materialized_slots += tupleDesc->num_materialized_slots();
        _num_null_slots += tupleDesc->num_null_slots();
        _tuple_desc_map.push_back(tupleDesc);
        DCHECK(_tuple_desc_map.back() != nullptr);
    }
    _num_null_bytes = (_num_null_slots + 7) / 8;

    init_tuple_idx_map();
    init_has_varlen_slots();
}

RowDescriptor::RowDescriptor(TupleDescriptor* tuple_desc, bool is_nullable)
        : _tuple_desc_map(1, tuple_desc), _tuple_idx_nullable_map(1, is_nullable) {
    init_tuple_idx_map();
    init_has_varlen_slots();
}

RowDescriptor::RowDescriptor(const RowDescriptor& lhs_row_desc, const RowDescriptor& rhs_row_desc) {
    _tuple_desc_map.insert(_tuple_desc_map.end(), lhs_row_desc._tuple_desc_map.begin(),
                           lhs_row_desc._tuple_desc_map.end());
    _tuple_desc_map.insert(_tuple_desc_map.end(), rhs_row_desc._tuple_desc_map.begin(),
                           rhs_row_desc._tuple_desc_map.end());
    _tuple_idx_nullable_map.insert(_tuple_idx_nullable_map.end(),
                                   lhs_row_desc._tuple_idx_nullable_map.begin(),
                                   lhs_row_desc._tuple_idx_nullable_map.end());
    _tuple_idx_nullable_map.insert(_tuple_idx_nullable_map.end(),
                                   rhs_row_desc._tuple_idx_nullable_map.begin(),
                                   rhs_row_desc._tuple_idx_nullable_map.end());
    init_tuple_idx_map();
    init_has_varlen_slots();
}

void RowDescriptor::init_tuple_idx_map() {
    // find max id
    TupleId max_id = 0;
    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        max_id = std::max(_tuple_desc_map[i]->id(), max_id);
    }

    _tuple_idx_map.resize(max_id + 1, INVALID_IDX);
    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        _tuple_idx_map[_tuple_desc_map[i]->id()] = i;
    }
}

void RowDescriptor::init_has_varlen_slots() {
    _has_varlen_slots = false;
    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        if (_tuple_desc_map[i]->has_varlen_slots()) {
            _has_varlen_slots = true;
            break;
        }
    }
}

int RowDescriptor::get_row_size() const {
    int size = 0;

    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        size += _tuple_desc_map[i]->byte_size();
    }

    return size;
}

int RowDescriptor::get_tuple_idx(TupleId id) const {
    // comment CHECK temporarily to make fuzzy test run smoothly
    // DCHECK_LT(id, _tuple_idx_map.size()) << "RowDescriptor: " << debug_string();
    if (_tuple_idx_map.size() <= id) {
        return RowDescriptor::INVALID_IDX;
    }
    return _tuple_idx_map[id];
}

bool RowDescriptor::tuple_is_nullable(int tuple_idx) const {
    DCHECK_LT(tuple_idx, _tuple_idx_nullable_map.size()) << "RowDescriptor: " << debug_string();
    return _tuple_idx_nullable_map[tuple_idx];
}

bool RowDescriptor::is_any_tuple_nullable() const {
    for (int i = 0; i < _tuple_idx_nullable_map.size(); ++i) {
        if (_tuple_idx_nullable_map[i]) {
            return true;
        }
    }
    return false;
}

void RowDescriptor::to_thrift(std::vector<TTupleId>* row_tuple_ids) {
    row_tuple_ids->clear();

    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        row_tuple_ids->push_back(_tuple_desc_map[i]->id());
    }
}

void RowDescriptor::to_protobuf(
        google::protobuf::RepeatedField<google::protobuf::int32>* row_tuple_ids) const {
    row_tuple_ids->Clear();
    for (auto desc : _tuple_desc_map) {
        row_tuple_ids->Add(desc->id());
    }
}

bool RowDescriptor::is_prefix_of(const RowDescriptor& other_desc) const {
    if (_tuple_desc_map.size() > other_desc._tuple_desc_map.size()) {
        return false;
    }

    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        // pointer comparison okay, descriptors are unique
        if (_tuple_desc_map[i] != other_desc._tuple_desc_map[i]) {
            return false;
        }
    }

    return true;
}

bool RowDescriptor::equals(const RowDescriptor& other_desc) const {
    if (_tuple_desc_map.size() != other_desc._tuple_desc_map.size()) {
        return false;
    }

    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        // pointer comparison okay, descriptors are unique
        if (_tuple_desc_map[i] != other_desc._tuple_desc_map[i]) {
            return false;
        }
    }

    return true;
}

bool RowDescriptor::layout_is_prefix_of(const RowDescriptor& other_desc) const {
    if (_tuple_desc_map.size() > other_desc._tuple_desc_map.size()) return false;
    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        if (!_tuple_desc_map[i]->layout_equals(*other_desc._tuple_desc_map[i])) return false;
    }
    return true;
}

bool RowDescriptor::layout_equals(const RowDescriptor& other_desc) const {
    if (_tuple_desc_map.size() != other_desc._tuple_desc_map.size()) return false;
    return layout_is_prefix_of(other_desc);
}

std::string RowDescriptor::debug_string() const {
    std::stringstream ss;

    ss << "tuple_desc_map: [";
    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        ss << _tuple_desc_map[i]->debug_string();
        if (i != _tuple_desc_map.size() - 1) {
            ss << ", ";
        }
    }
    ss << "] ";

    ss << "tuple_id_map: [";
    for (int i = 0; i < _tuple_idx_map.size(); ++i) {
        ss << _tuple_idx_map[i];
        if (i != _tuple_idx_map.size() - 1) {
            ss << ", ";
        }
    }
    ss << "] ";

    ss << "tuple_is_nullable: [";
    for (int i = 0; i < _tuple_idx_nullable_map.size(); ++i) {
        ss << _tuple_idx_nullable_map[i];
        if (i != _tuple_idx_nullable_map.size() - 1) {
            ss << ", ";
        }
    }
    ss << "] ";

    return ss.str();
}

int RowDescriptor::get_column_id(int slot_id) const {
    int column_id_counter = 0;
    for (const auto tuple_desc : _tuple_desc_map) {
        for (const auto slot : tuple_desc->slots()) {
            if (slot->id() == slot_id) {
                return column_id_counter;
            }
            column_id_counter++;
        }
    }
    return -1;
}

Status DescriptorTbl::create(ObjectPool* pool, const TDescriptorTable& thrift_tbl,
                             DescriptorTbl** tbl) {
    *tbl = pool->add(new DescriptorTbl());

    // deserialize table descriptors first, they are being referenced by tuple descriptors
    for (size_t i = 0; i < thrift_tbl.tableDescriptors.size(); ++i) {
        const TTableDescriptor& tdesc = thrift_tbl.tableDescriptors[i];
        TableDescriptor* desc = nullptr;

        switch (tdesc.tableType) {
        case TTableType::MYSQL_TABLE:
            desc = pool->add(new MySQLTableDescriptor(tdesc));
            break;

        case TTableType::ODBC_TABLE:
            desc = pool->add(new ODBCTableDescriptor(tdesc));
            break;

        case TTableType::OLAP_TABLE:
            desc = pool->add(new OlapTableDescriptor(tdesc));
            break;

        case TTableType::SCHEMA_TABLE:
            desc = pool->add(new SchemaTableDescriptor(tdesc));
            break;
        case TTableType::BROKER_TABLE:
            desc = pool->add(new BrokerTableDescriptor(tdesc));
            break;
        case TTableType::ES_TABLE:
            desc = pool->add(new EsTableDescriptor(tdesc));
            break;
        case TTableType::HIVE_TABLE:
            desc = pool->add(new HiveTableDescriptor(tdesc));
            break;
        case TTableType::ICEBERG_TABLE:
            desc = pool->add(new IcebergTableDescriptor(tdesc));
            break;
        case TTableType::JDBC_TABLE:
            desc = pool->add(new JdbcTableDescriptor(tdesc));
            break;
        default:
            DCHECK(false) << "invalid table type: " << tdesc.tableType;
        }

        (*tbl)->_tbl_desc_map[tdesc.id] = desc;
    }

    for (size_t i = 0; i < thrift_tbl.tupleDescriptors.size(); ++i) {
        const TTupleDescriptor& tdesc = thrift_tbl.tupleDescriptors[i];
        TupleDescriptor* desc = pool->add(new TupleDescriptor(tdesc));

        // fix up table pointer
        if (tdesc.__isset.tableId) {
            desc->_table_desc = (*tbl)->get_table_descriptor(tdesc.tableId);
            DCHECK(desc->_table_desc != nullptr);
        }

        (*tbl)->_tuple_desc_map[tdesc.id] = desc;
        (*tbl)->_row_tuples.emplace_back(tdesc.id);
    }

    for (size_t i = 0; i < thrift_tbl.slotDescriptors.size(); ++i) {
        const TSlotDescriptor& tdesc = thrift_tbl.slotDescriptors[i];
        SlotDescriptor* slot_d = pool->add(new SlotDescriptor(tdesc));
        (*tbl)->_slot_desc_map[tdesc.id] = slot_d;

        // link to parent
        TupleDescriptorMap::iterator entry = (*tbl)->_tuple_desc_map.find(tdesc.parent);

        if (entry == (*tbl)->_tuple_desc_map.end()) {
            return Status::InternalError("unknown tid in slot descriptor msg");
        }
        entry->second->add_slot(slot_d);
    }

    return Status::OK();
}

TableDescriptor* DescriptorTbl::get_table_descriptor(TableId id) const {
    // TODO: is there some boost function to do exactly this?
    TableDescriptorMap::const_iterator i = _tbl_desc_map.find(id);

    if (i == _tbl_desc_map.end()) {
        return nullptr;
    } else {
        return i->second;
    }
}

TupleDescriptor* DescriptorTbl::get_tuple_descriptor(TupleId id) const {
    // TODO: is there some boost function to do exactly this?
    TupleDescriptorMap::const_iterator i = _tuple_desc_map.find(id);

    if (i == _tuple_desc_map.end()) {
        return nullptr;
    } else {
        return i->second;
    }
}

SlotDescriptor* DescriptorTbl::get_slot_descriptor(SlotId id) const {
    // TODO: is there some boost function to do exactly this?
    SlotDescriptorMap::const_iterator i = _slot_desc_map.find(id);

    if (i == _slot_desc_map.end()) {
        return nullptr;
    } else {
        return i->second;
    }
}

bool SlotDescriptor::layout_equals(const SlotDescriptor& other_desc) const {
    if (type().type != other_desc.type().type) return false;
    if (is_nullable() != other_desc.is_nullable()) return false;
    if (slot_size() != other_desc.slot_size()) return false;
    if (tuple_offset() != other_desc.tuple_offset()) return false;
    if (!null_indicator_offset().equals(other_desc.null_indicator_offset())) return false;
    return true;
}

std::string DescriptorTbl::debug_string() const {
    std::stringstream out;
    out << "tuples:\n";

    for (TupleDescriptorMap::const_iterator i = _tuple_desc_map.begin(); i != _tuple_desc_map.end();
         ++i) {
        out << i->second->debug_string() << '\n';
    }

    return out.str();
}

} // namespace doris
