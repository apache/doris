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

#include <fmt/format.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/descriptors.pb.h>
#include <stddef.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <boost/algorithm/string/join.hpp>

#include "common/exception.h"
#include "common/object_pool.h"
#include "util/string_util.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_nothing.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/exprs/vexpr.h"
#include "vec/functions/function_helpers.h"
#include "vec/utils/util.hpp"

namespace doris {
#include "common/compile_check_begin.h"
const int RowDescriptor::INVALID_IDX = -1;

SlotDescriptor::SlotDescriptor(const TSlotDescriptor& tdesc)
        : _id(tdesc.id),
          _type(vectorized::DataTypeFactory::instance().create_data_type(
                  tdesc.slotType, tdesc.nullIndicatorBit != -1)),
          _parent(tdesc.parent),
          _col_pos(tdesc.columnPos),
          _col_name(tdesc.colName),
          _col_name_lower_case(to_lower(tdesc.colName)),
          _col_unique_id(tdesc.col_unique_id),
          _slot_idx(tdesc.slotIdx),
          _field_idx(-1),
          _is_key(tdesc.is_key),
          _column_paths(tdesc.column_paths),
          _all_access_paths(tdesc.__isset.all_access_paths ? tdesc.all_access_paths
                                                           : TColumnAccessPaths {}),
          _predicate_access_paths(tdesc.__isset.predicate_access_paths
                                          ? tdesc.predicate_access_paths
                                          : TColumnAccessPaths {}),
          _is_auto_increment(tdesc.__isset.is_auto_increment ? tdesc.is_auto_increment : false),
          _col_default_value(tdesc.__isset.col_default_value ? tdesc.col_default_value : "") {
    if (tdesc.__isset.virtual_column_expr) {
        // Make sure virtual column is valid.
        if (tdesc.virtual_column_expr.nodes.empty()) {
            throw doris::Exception(doris::ErrorCode::FATAL_ERROR,
                                   "Virtual column expr node is empty, col_name: {}, "
                                   "col_unique_id: {}",
                                   tdesc.colName, tdesc.col_unique_id);
        }
        const auto& node = tdesc.virtual_column_expr.nodes[0];
        if (node.node_type == TExprNodeType::SLOT_REF) {
            throw doris::Exception(doris::ErrorCode::FATAL_ERROR,
                                   "Virtual column expr node is slot ref, col_name: {}, "
                                   "col_unique_id: {}",
                                   tdesc.colName, tdesc.col_unique_id);
        }
        this->virtual_column_expr = std::make_shared<doris::TExpr>(tdesc.virtual_column_expr);
    }
}

SlotDescriptor::SlotDescriptor(const PSlotDescriptor& pdesc)
        : _id(pdesc.id()),
          _type(vectorized::DataTypeFactory::instance().create_data_type(
                  pdesc.slot_type(), pdesc.null_indicator_bit() != -1)),
          _parent(pdesc.parent()),
          _col_pos(pdesc.column_pos()),
          _col_name(pdesc.col_name()),
          _col_name_lower_case(to_lower(pdesc.col_name())),
          _col_unique_id(pdesc.col_unique_id()),
          _slot_idx(pdesc.slot_idx()),
          _field_idx(-1),
          _is_key(pdesc.is_key()),
          _column_paths(pdesc.column_paths().begin(), pdesc.column_paths().end()),
          _is_auto_increment(pdesc.is_auto_increment()) {
    auto convert_to_thrift_column_access_path = [](const PColumnAccessPath& pb_path) {
        TColumnAccessPath thrift_path;
        thrift_path.type = (TAccessPathType::type)pb_path.type();
        if (pb_path.has_data_access_path()) {
            thrift_path.__isset.data_access_path = true;
            for (int i = 0; i < pb_path.data_access_path().path_size(); ++i) {
                thrift_path.data_access_path.path.push_back(pb_path.data_access_path().path(i));
            }
        }
        if (pb_path.has_meta_access_path()) {
            thrift_path.__isset.meta_access_path = true;
            for (int i = 0; i < pb_path.meta_access_path().path_size(); ++i) {
                thrift_path.meta_access_path.path.push_back(pb_path.meta_access_path().path(i));
            }
        }
        return thrift_path;
    };
    for (const auto& pb_path : pdesc.all_access_paths()) {
        _all_access_paths.push_back(convert_to_thrift_column_access_path(pb_path));
    }
    for (const auto& pb_path : pdesc.predicate_access_paths()) {
        _predicate_access_paths.push_back(convert_to_thrift_column_access_path(pb_path));
    }
}

#ifdef BE_TEST
SlotDescriptor::SlotDescriptor()
        : _id(0),
          _type(nullptr),
          _parent(0),
          _col_pos(0),
          _col_unique_id(0),
          _slot_idx(0),
          _field_idx(-1),
          _is_key(false),
          _is_auto_increment(false) {}
#endif

void SlotDescriptor::to_protobuf(PSlotDescriptor* pslot) const {
    pslot->set_id(_id);
    pslot->set_parent(_parent);
    _type->to_protobuf(pslot->mutable_slot_type());
    pslot->set_column_pos(_col_pos);
    pslot->set_byte_offset(0);
    pslot->set_null_indicator_byte(0);
    pslot->set_null_indicator_bit(_type->is_nullable() ? 0 : -1);
    pslot->set_col_name(_col_name);
    pslot->set_slot_idx(_slot_idx);
    pslot->set_col_unique_id(_col_unique_id);
    pslot->set_is_key(_is_key);
    pslot->set_is_auto_increment(_is_auto_increment);
    pslot->set_col_type(_type->get_primitive_type());
    for (const std::string& path : _column_paths) {
        pslot->add_column_paths(path);
    }
    auto convert_to_protobuf_column_access_path = [](const TColumnAccessPath& thrift_path,
                                                     doris::PColumnAccessPath* pb_path) {
        pb_path->Clear();
        pb_path->set_type((PAccessPathType)thrift_path.type); // 使用 reinterpret_cast 进行类型转换
        if (thrift_path.__isset.data_access_path) {
            auto* pb_data = pb_path->mutable_data_access_path();
            pb_data->Clear();
            for (const auto& s : thrift_path.data_access_path.path) {
                pb_data->add_path(s);
            }
        }
        if (thrift_path.__isset.meta_access_path) {
            auto* pb_meta = pb_path->mutable_meta_access_path();
            pb_meta->Clear();
            for (const auto& s : thrift_path.meta_access_path.path) {
                pb_meta->add_path(s);
            }
        }
    };
    for (const auto& path : _all_access_paths) {
        auto* pb_path = pslot->add_all_access_paths();
        convert_to_protobuf_column_access_path(path, pb_path);
    }
    for (const auto& path : _predicate_access_paths) {
        auto* pb_path = pslot->add_predicate_access_paths();
        convert_to_protobuf_column_access_path(path, pb_path);
    }
}

vectorized::DataTypePtr SlotDescriptor::get_data_type_ptr() const {
    return vectorized::get_data_type_with_default_argument(type());
}

vectorized::MutableColumnPtr SlotDescriptor::get_empty_mutable_column() const {
    if (this->get_virtual_column_expr() != nullptr) {
        return vectorized::ColumnNothing::create(0);
    }

    return type()->create_column();
}

bool SlotDescriptor::is_nullable() const {
    return _type->is_nullable();
}

PrimitiveType SlotDescriptor::col_type() const {
    return _type->get_primitive_type();
}

std::string SlotDescriptor::debug_string() const {
    const bool is_virtual = this->get_virtual_column_expr() != nullptr;
    return fmt::format(
            "SlotDescriptor(id={}, type={}, col_name={}, col_unique_id={}, "
            "is_virtual={})",
            _id, _type->get_name(), _col_name, _col_unique_id, is_virtual);
}

TableDescriptor::TableDescriptor(const TTableDescriptor& tdesc)
        : _table_type(tdesc.tableType),
          _name(tdesc.tableName),
          _database(tdesc.dbName),
          _table_id(tdesc.id),
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

DictionaryTableDescriptor::DictionaryTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc) {}

std::string DictionaryTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "Dictionary(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

SchemaTableDescriptor::SchemaTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc), _schema_table_type(tdesc.schemaTable.tableType) {}
SchemaTableDescriptor::~SchemaTableDescriptor() = default;

std::string SchemaTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "SchemaTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

BrokerTableDescriptor::BrokerTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc) {}

BrokerTableDescriptor::~BrokerTableDescriptor() = default;

std::string BrokerTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "BrokerTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

HiveTableDescriptor::HiveTableDescriptor(const TTableDescriptor& tdesc) : TableDescriptor(tdesc) {}

HiveTableDescriptor::~HiveTableDescriptor() = default;

std::string HiveTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "HiveTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

IcebergTableDescriptor::IcebergTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc) {}

IcebergTableDescriptor::~IcebergTableDescriptor() = default;

std::string IcebergTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "IcebergTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

MaxComputeTableDescriptor::MaxComputeTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc),
          _region(tdesc.mcTable.region),
          _project(tdesc.mcTable.project),
          _table(tdesc.mcTable.table),
          _odps_url(tdesc.mcTable.odps_url),
          _tunnel_url(tdesc.mcTable.tunnel_url),
          _access_key(tdesc.mcTable.access_key),
          _secret_key(tdesc.mcTable.secret_key),
          _public_access(tdesc.mcTable.public_access) {
    if (tdesc.mcTable.__isset.endpoint) {
        _endpoint = tdesc.mcTable.endpoint;
    } else {
        _init_status = Status::InvalidArgument(
                "fail to init MaxComputeTableDescriptor, missing endpoint.");
    }

    if (tdesc.mcTable.__isset.quota) {
        _quota = tdesc.mcTable.quota;
    } else {
        _init_status =
                Status::InvalidArgument("fail to init MaxComputeTableDescriptor, missing quota.");
    }
}

MaxComputeTableDescriptor::~MaxComputeTableDescriptor() = default;

std::string MaxComputeTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "MaxComputeTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

TrinoConnectorTableDescriptor::TrinoConnectorTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc) {}

TrinoConnectorTableDescriptor::~TrinoConnectorTableDescriptor() = default;

std::string TrinoConnectorTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "TrinoConnectorTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

EsTableDescriptor::EsTableDescriptor(const TTableDescriptor& tdesc) : TableDescriptor(tdesc) {}

EsTableDescriptor::~EsTableDescriptor() = default;

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

JdbcTableDescriptor::JdbcTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc),
          _jdbc_catalog_id(tdesc.jdbcTable.catalog_id),
          _jdbc_resource_name(tdesc.jdbcTable.jdbc_resource_name),
          _jdbc_driver_url(tdesc.jdbcTable.jdbc_driver_url),
          _jdbc_driver_class(tdesc.jdbcTable.jdbc_driver_class),
          _jdbc_driver_checksum(tdesc.jdbcTable.jdbc_driver_checksum),
          _jdbc_url(tdesc.jdbcTable.jdbc_url),
          _jdbc_table_name(tdesc.jdbcTable.jdbc_table_name),
          _jdbc_user(tdesc.jdbcTable.jdbc_user),
          _jdbc_passwd(tdesc.jdbcTable.jdbc_password),
          _connection_pool_min_size(tdesc.jdbcTable.connection_pool_min_size),
          _connection_pool_max_size(tdesc.jdbcTable.connection_pool_max_size),
          _connection_pool_max_wait_time(tdesc.jdbcTable.connection_pool_max_wait_time),
          _connection_pool_max_life_time(tdesc.jdbcTable.connection_pool_max_life_time),
          _connection_pool_keep_alive(tdesc.jdbcTable.connection_pool_keep_alive) {}

std::string JdbcTableDescriptor::debug_string() const {
    fmt::memory_buffer buf;
    fmt::format_to(
            buf,
            "JDBCTable({} ,_jdbc_catalog_id = {}, _jdbc_resource_name={} ,_jdbc_driver_url={} "
            ",_jdbc_driver_class={} ,_jdbc_driver_checksum={} ,_jdbc_url={} "
            ",_jdbc_table_name={} ,_jdbc_user={} ,_jdbc_passwd={} ,_connection_pool_min_size={} "
            ",_connection_pool_max_size={} ,_connection_pool_max_wait_time={} "
            ",_connection_pool_max_life_time={} ,_connection_pool_keep_alive={})",
            TableDescriptor::debug_string(), _jdbc_catalog_id, _jdbc_resource_name,
            _jdbc_driver_url, _jdbc_driver_class, _jdbc_driver_checksum, _jdbc_url,
            _jdbc_table_name, _jdbc_user, _jdbc_passwd, _connection_pool_min_size,
            _connection_pool_max_size, _connection_pool_max_wait_time,
            _connection_pool_max_life_time, _connection_pool_keep_alive);
    return fmt::to_string(buf);
}

RemoteDorisTableDescriptor::RemoteDorisTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc) {}

RemoteDorisTableDescriptor::~RemoteDorisTableDescriptor() = default;

std::string RemoteDorisTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "RemoteDorisTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

TupleDescriptor::TupleDescriptor(const TTupleDescriptor& tdesc, bool own_slots)
        : _id(tdesc.id),
          _num_materialized_slots(0),
          _has_varlen_slots(false),
          _own_slots(own_slots) {}

TupleDescriptor::TupleDescriptor(const PTupleDescriptor& pdesc, bool own_slots)
        : _id(pdesc.id()),
          _num_materialized_slots(0),
          _has_varlen_slots(false),
          _own_slots(own_slots) {}

void TupleDescriptor::add_slot(SlotDescriptor* slot) {
    _slots.push_back(slot);
    ++_num_materialized_slots;

    if (is_complex_type(slot->type()->get_primitive_type()) ||
        is_var_len_object(slot->type()->get_primitive_type()) ||
        is_string_type(slot->type()->get_primitive_type())) {
        _has_varlen_slots = true;
    }
}

void TupleDescriptor::to_protobuf(PTupleDescriptor* ptuple) const {
    ptuple->Clear();
    ptuple->set_id(_id);
    // Useless not set
    ptuple->set_byte_size(0);
    ptuple->set_table_id(-1);
    ptuple->set_num_null_bytes(0);
}

std::string TupleDescriptor::debug_string() const {
    std::stringstream out;
    out << "Tuple(id=" << _id;
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

RowDescriptor::RowDescriptor(const DescriptorTbl& desc_tbl,
                             const std::vector<TTupleId>& row_tuples) {
    DCHECK_GT(row_tuples.size(), 0);
    _num_materialized_slots = 0;
    _num_slots = 0;

    for (int row_tuple : row_tuples) {
        TupleDescriptor* tupleDesc = desc_tbl.get_tuple_descriptor(row_tuple);
        _num_materialized_slots += tupleDesc->num_materialized_slots();
        _num_slots += tupleDesc->slots().size();
        _tuple_desc_map.push_back(tupleDesc);
        DCHECK(_tuple_desc_map.back() != nullptr);
    }

    init_tuple_idx_map();
    init_has_varlen_slots();
}

RowDescriptor::RowDescriptor(TupleDescriptor* tuple_desc) : _tuple_desc_map(1, tuple_desc) {
    init_tuple_idx_map();
    init_has_varlen_slots();
    _num_slots = static_cast<int32_t>(tuple_desc->slots().size());
}

RowDescriptor::RowDescriptor(const RowDescriptor& lhs_row_desc, const RowDescriptor& rhs_row_desc) {
    _tuple_desc_map.insert(_tuple_desc_map.end(), lhs_row_desc._tuple_desc_map.begin(),
                           lhs_row_desc._tuple_desc_map.end());
    _tuple_desc_map.insert(_tuple_desc_map.end(), rhs_row_desc._tuple_desc_map.begin(),
                           rhs_row_desc._tuple_desc_map.end());
    init_tuple_idx_map();
    init_has_varlen_slots();

    _num_slots = lhs_row_desc.num_slots() + rhs_row_desc.num_slots();
}

void RowDescriptor::init_tuple_idx_map() {
    // find max id
    TupleId max_id = 0;
    for (auto& i : _tuple_desc_map) {
        max_id = std::max(i->id(), max_id);
    }

    _tuple_idx_map.resize(max_id + 1, INVALID_IDX);
    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        _tuple_idx_map[_tuple_desc_map[i]->id()] = i;
    }
}

void RowDescriptor::init_has_varlen_slots() {
    _has_varlen_slots = false;
    for (auto& i : _tuple_desc_map) {
        if (i->has_varlen_slots()) {
            _has_varlen_slots = true;
            break;
        }
    }
}

int RowDescriptor::get_tuple_idx(TupleId id) const {
    // comment CHECK temporarily to make fuzzy test run smoothly
    // DCHECK_LT(id, _tuple_idx_map.size()) << "RowDescriptor: " << debug_string();
    if (_tuple_idx_map.size() <= id) {
        return RowDescriptor::INVALID_IDX;
    }
    return _tuple_idx_map[id];
}

void RowDescriptor::to_thrift(std::vector<TTupleId>* row_tuple_ids) {
    row_tuple_ids->clear();

    for (auto& i : _tuple_desc_map) {
        row_tuple_ids->push_back(i->id());
    }
}

void RowDescriptor::to_protobuf(
        google::protobuf::RepeatedField<google::protobuf::int32>* row_tuple_ids) const {
    row_tuple_ids->Clear();
    for (auto* desc : _tuple_desc_map) {
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

    return ss.str();
}

int RowDescriptor::get_column_id(int slot_id) const {
    int column_id_counter = 0;
    for (auto* const tuple_desc : _tuple_desc_map) {
        for (auto* const slot : tuple_desc->slots()) {
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
    for (const auto& tdesc : thrift_tbl.tableDescriptors) {
        TableDescriptor* desc = nullptr;

        switch (tdesc.tableType) {
        case TTableType::MYSQL_TABLE:
            desc = pool->add(new MySQLTableDescriptor(tdesc));
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
        case TTableType::MAX_COMPUTE_TABLE:
            desc = pool->add(new MaxComputeTableDescriptor(tdesc));
            break;
        case TTableType::TRINO_CONNECTOR_TABLE:
            desc = pool->add(new TrinoConnectorTableDescriptor(tdesc));
            break;
        case TTableType::DICTIONARY_TABLE:
            desc = pool->add(new DictionaryTableDescriptor(tdesc));
            break;
        case TTableType::REMOTE_DORIS_TABLE:
            desc = pool->add(new RemoteDorisTableDescriptor(tdesc));
            break;
        default:
            DCHECK(false) << "invalid table type: " << tdesc.tableType;
        }

        (*tbl)->_tbl_desc_map[static_cast<int32_t>(tdesc.id)] = desc;
    }

    for (const auto& tdesc : thrift_tbl.tupleDescriptors) {
        TupleDescriptor* desc = pool->add(new TupleDescriptor(tdesc));

        // fix up table pointer
        if (tdesc.__isset.tableId) {
            desc->_table_desc = (*tbl)->get_table_descriptor(static_cast<int32_t>(tdesc.tableId));
            DCHECK(desc->_table_desc != nullptr);
        }

        (*tbl)->_tuple_desc_map[tdesc.id] = desc;
        (*tbl)->_row_tuples.emplace_back(tdesc.id);
    }

    for (const auto& tdesc : thrift_tbl.slotDescriptors) {
        SlotDescriptor* slot_d = pool->add(new SlotDescriptor(tdesc));
        (*tbl)->_slot_desc_map[tdesc.id] = slot_d;

        // link to parent
        auto entry = (*tbl)->_tuple_desc_map.find(tdesc.parent);

        if (entry == (*tbl)->_tuple_desc_map.end()) {
            return Status::InternalError("unknown tid in slot descriptor msg");
        }
        entry->second->add_slot(slot_d);
    }

    return Status::OK();
}

TableDescriptor* DescriptorTbl::get_table_descriptor(TableId id) const {
    // TODO: is there some boost function to do exactly this?
    auto i = _tbl_desc_map.find(id);

    if (i == _tbl_desc_map.end()) {
        return nullptr;
    } else {
        return i->second;
    }
}

TupleDescriptor* DescriptorTbl::get_tuple_descriptor(TupleId id) const {
    // TODO: is there some boost function to do exactly this?
    auto i = _tuple_desc_map.find(id);

    if (i == _tuple_desc_map.end()) {
        return nullptr;
    } else {
        return i->second;
    }
}

SlotDescriptor* DescriptorTbl::get_slot_descriptor(SlotId id) const {
    // TODO: is there some boost function to do exactly this?
    auto i = _slot_desc_map.find(id);

    if (i == _slot_desc_map.end()) {
        return nullptr;
    } else {
        return i->second;
    }
}

std::string DescriptorTbl::debug_string() const {
    std::stringstream out;
    out << "tuples:\n";

    for (auto i : _tuple_desc_map) {
        out << i.second->debug_string() << '\n';
    }

    return out.str();
}
#include "common/compile_check_end.h"
} // namespace doris
