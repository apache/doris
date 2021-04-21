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

#include "exec/tablet_info.h"

#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "util/string_parser.hpp"

namespace doris {

void OlapTableIndexSchema::to_protobuf(POlapTableIndexSchema* pindex) const {
    pindex->set_id(index_id);
    pindex->set_schema_hash(schema_hash);
    for (auto slot : slots) {
        pindex->add_columns(slot->col_name());
    }
}

Status OlapTableSchemaParam::init(const POlapTableSchemaParam& pschema) {
    _db_id = pschema.db_id();
    _table_id = pschema.table_id();
    _version = pschema.version();
    std::map<std::string, SlotDescriptor*> slots_map;
    _tuple_desc = _obj_pool.add(new TupleDescriptor(pschema.tuple_desc()));
    for (auto& p_slot_desc : pschema.slot_descs()) {
        auto slot_desc = _obj_pool.add(new SlotDescriptor(p_slot_desc));
        _tuple_desc->add_slot(slot_desc);
        slots_map.emplace(slot_desc->col_name(), slot_desc);
    }
    for (auto& p_index : pschema.indexes()) {
        auto index = _obj_pool.add(new OlapTableIndexSchema());
        index->index_id = p_index.id();
        index->schema_hash = p_index.schema_hash();
        for (auto& col : p_index.columns()) {
            auto it = slots_map.find(col);
            if (it == std::end(slots_map)) {
                std::stringstream ss;
                ss << "unknown index column, column=" << col;
                return Status::InternalError(ss.str());
            }
            index->slots.emplace_back(it->second);
        }
        _indexes.emplace_back(index);
    }

    std::sort(_indexes.begin(), _indexes.end(),
              [](const OlapTableIndexSchema* lhs, const OlapTableIndexSchema* rhs) {
                  return lhs->index_id < rhs->index_id;
              });
    return Status::OK();
}

Status OlapTableSchemaParam::init(const TOlapTableSchemaParam& tschema) {
    _db_id = tschema.db_id;
    _table_id = tschema.table_id;
    _version = tschema.version;
    std::map<std::string, SlotDescriptor*> slots_map;
    _tuple_desc = _obj_pool.add(new TupleDescriptor(tschema.tuple_desc));
    for (auto& t_slot_desc : tschema.slot_descs) {
        auto slot_desc = _obj_pool.add(new SlotDescriptor(t_slot_desc));
        _tuple_desc->add_slot(slot_desc);
        slots_map.emplace(slot_desc->col_name(), slot_desc);
    }
    for (auto& t_index : tschema.indexes) {
        auto index = _obj_pool.add(new OlapTableIndexSchema());
        index->index_id = t_index.id;
        index->schema_hash = t_index.schema_hash;
        for (auto& col : t_index.columns) {
            auto it = slots_map.find(col);
            if (it == std::end(slots_map)) {
                std::stringstream ss;
                ss << "unknown index column, column=" << col;
                return Status::InternalError(ss.str());
            }
            index->slots.emplace_back(it->second);
        }
        _indexes.emplace_back(index);
    }

    std::sort(_indexes.begin(), _indexes.end(),
              [](const OlapTableIndexSchema* lhs, const OlapTableIndexSchema* rhs) {
                  return lhs->index_id < rhs->index_id;
              });
    return Status::OK();
}

void OlapTableSchemaParam::to_protobuf(POlapTableSchemaParam* pschema) const {
    pschema->set_db_id(_db_id);
    pschema->set_table_id(_table_id);
    pschema->set_version(_version);
    _tuple_desc->to_protobuf(pschema->mutable_tuple_desc());
    for (auto slot : _tuple_desc->slots()) {
        slot->to_protobuf(pschema->add_slot_descs());
    }
    for (auto index : _indexes) {
        index->to_protobuf(pschema->add_indexes());
    }
}

std::string OlapTableSchemaParam::debug_string() const {
    std::stringstream ss;
    ss << "tuple_desc=" << _tuple_desc->debug_string();
    return ss.str();
}

std::string OlapTablePartition::debug_string(TupleDescriptor* tuple_desc) const {
    std::stringstream ss;
    ss << "(id=" << id << ",start_key=" << Tuple::to_string(start_key, *tuple_desc)
       << ",end_key=" << Tuple::to_string(end_key, *tuple_desc) << ",num_buckets=" << num_buckets
       << ",indexes=[";
    int idx = 0;
    for (auto& index : indexes) {
        if (idx++ > 0) {
            ss << ",";
        }
        ss << "(id=" << index.index_id << ",tablets=[";
        int jdx = 0;
        for (auto id : index.tablets) {
            if (jdx++ > 0) {
                ss << ",";
            }
            ss << id;
        }
        ss << "])";
    }
    ss << "])";
    return ss.str();
}

OlapTablePartitionParam::OlapTablePartitionParam(std::shared_ptr<OlapTableSchemaParam> schema,
                                                 const TOlapTablePartitionParam& t_param)
        : _schema(schema),
          _t_param(t_param),
          _mem_tracker(MemTracker::CreateTracker(-1, "OlapTablePartitionParam")),
          _mem_pool(new MemPool(_mem_tracker.get())) {}

OlapTablePartitionParam::~OlapTablePartitionParam() {}

Status OlapTablePartitionParam::init() {
    std::map<std::string, SlotDescriptor*> slots_map;
    for (auto slot_desc : _schema->tuple_desc()->slots()) {
        slots_map.emplace(slot_desc->col_name(), slot_desc);
    }
    if (_t_param.__isset.partition_column) {
        auto it = slots_map.find(_t_param.partition_column);
        if (it == std::end(slots_map)) {
            std::stringstream ss;
            ss << "partition column not found, column=" << _t_param.partition_column;
            return Status::InternalError(ss.str());
        }
        _partition_slot_descs.push_back(it->second);
    } else if (_t_param.__isset.partition_columns) {
        for (auto& part_col : _t_param.partition_columns) {
            auto it = slots_map.find(part_col);
            if (it == std::end(slots_map)) {
                std::stringstream ss;
                ss << "partition column not found, column=" << part_col;
                return Status::InternalError(ss.str());
            }
            _partition_slot_descs.push_back(it->second);
        }
    }

    _partitions_map.reset(new std::map<Tuple*, OlapTablePartition*, OlapTablePartKeyComparator>(
            OlapTablePartKeyComparator(_partition_slot_descs)));
    if (_t_param.__isset.distributed_columns) {
        for (auto& col : _t_param.distributed_columns) {
            auto it = slots_map.find(col);
            if (it == std::end(slots_map)) {
                std::stringstream ss;
                ss << "distributed column not found, columns=" << col;
                return Status::InternalError(ss.str());
            }
            _distributed_slot_descs.emplace_back(it->second);
        }
    }
    // initial partitions
    for (int i = 0; i < _t_param.partitions.size(); ++i) {
        const TOlapTablePartition& t_part = _t_param.partitions[i];
        OlapTablePartition* part = _obj_pool.add(new OlapTablePartition());
        part->id = t_part.id;

        if (t_part.__isset.start_key) {
            // deprecated, use start_keys instead
            std::vector<TExprNode> exprs = {t_part.start_key};
            RETURN_IF_ERROR(_create_partition_keys(exprs, &part->start_key));
        } else if (t_part.__isset.start_keys) {
            RETURN_IF_ERROR(_create_partition_keys(t_part.start_keys, &part->start_key));
        }
        if (t_part.__isset.end_key) {
            // deprecated, use end_keys instead
            std::vector<TExprNode> exprs = {t_part.end_key};
            RETURN_IF_ERROR(_create_partition_keys(exprs, &part->end_key));
        } else if (t_part.__isset.end_keys) {
            RETURN_IF_ERROR(_create_partition_keys(t_part.end_keys, &part->end_key));
        }

        part->num_buckets = t_part.num_buckets;
        auto num_indexes = _schema->indexes().size();
        if (t_part.indexes.size() != num_indexes) {
            std::stringstream ss;
            ss << "number of partition's index is not equal with schema's"
               << ", num_part_indexes=" << t_part.indexes.size()
               << ", num_schema_indexes=" << num_indexes;
            return Status::InternalError(ss.str());
        }
        part->indexes = t_part.indexes;
        std::sort(part->indexes.begin(), part->indexes.end(),
                  [](const OlapTableIndexTablets& lhs, const OlapTableIndexTablets& rhs) {
                      return lhs.index_id < rhs.index_id;
                  });
        // check index
        for (int j = 0; j < num_indexes; ++j) {
            if (part->indexes[j].index_id != _schema->indexes()[j]->index_id) {
                std::stringstream ss;
                ss << "partition's index is not equal with schema's"
                   << ", part_index=" << part->indexes[j].index_id
                   << ", schema_index=" << _schema->indexes()[j]->index_id;
                return Status::InternalError(ss.str());
            }
        }
        _partitions.emplace_back(part);
        _partitions_map->emplace(part->end_key, part);
    }
    return Status::OK();
}

bool OlapTablePartitionParam::find_tablet(Tuple* tuple, const OlapTablePartition** partition,
                                          uint32_t* dist_hashes) const {
    auto it = _partitions_map->upper_bound(tuple);
    if (it == _partitions_map->end()) {
        return false;
    }
    if (_part_contains(it->second, tuple)) {
        *partition = it->second;
        *dist_hashes = _compute_dist_hash(tuple);
        return true;
    }
    return false;
}

Status OlapTablePartitionParam::_create_partition_keys(const std::vector<TExprNode>& t_exprs,
                                                       Tuple** part_key) {
    Tuple* tuple = (Tuple*)_mem_pool->allocate(_schema->tuple_desc()->byte_size());
    for (int i = 0; i < t_exprs.size(); i++) {
        const TExprNode& t_expr = t_exprs[i];
        RETURN_IF_ERROR(_create_partition_key(t_expr, tuple, _partition_slot_descs[i]));
    }
    *part_key = tuple;
    return Status::OK();
}

Status OlapTablePartitionParam::_create_partition_key(const TExprNode& t_expr, Tuple* tuple,
                                                      SlotDescriptor* slot_desc) {
    void* slot = tuple->get_slot(slot_desc->tuple_offset());
    tuple->set_not_null(slot_desc->null_indicator_offset());
    switch (t_expr.node_type) {
    case TExprNodeType::DATE_LITERAL: {
        if (!reinterpret_cast<DateTimeValue*>(slot)->from_date_str(
                    t_expr.date_literal.value.c_str(), t_expr.date_literal.value.size())) {
            std::stringstream ss;
            ss << "invalid date literal in partition column, date=" << t_expr.date_literal;
            return Status::InternalError(ss.str());
        }
        break;
    }
    case TExprNodeType::INT_LITERAL: {
        switch (t_expr.type.types[0].scalar_type.type) {
        case TPrimitiveType::TINYINT:
            *reinterpret_cast<int8_t*>(slot) = t_expr.int_literal.value;
            break;
        case TPrimitiveType::SMALLINT:
            *reinterpret_cast<int16_t*>(slot) = t_expr.int_literal.value;
            break;
        case TPrimitiveType::INT:
            *reinterpret_cast<int32_t*>(slot) = t_expr.int_literal.value;
            break;
        case TPrimitiveType::BIGINT:
            *reinterpret_cast<int64_t*>(slot) = t_expr.int_literal.value;
            break;
        default:
            DCHECK(false) << "unsupported int literal type, type=" << t_expr.type.types[0].type;
            break;
        }
        break;
    }
    case TExprNodeType::LARGE_INT_LITERAL: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        __int128 val = StringParser::string_to_int<__int128>(t_expr.large_int_literal.value.c_str(),
                                                             t_expr.large_int_literal.value.size(),
                                                             &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            val = MAX_INT128;
        }
        memcpy(slot, &val, sizeof(val));
        break;
    }
    default: {
        std::stringstream ss;
        ss << "unsupported partition column node type, type=" << t_expr.node_type;
        return Status::InternalError(ss.str());
    }
    }
    return Status::OK();
}

std::string OlapTablePartitionParam::debug_string() const {
    std::stringstream ss;
    ss << "partitions=[";
    int idx = 0;
    for (auto part : _partitions) {
        if (idx++ > 0) {
            ss << ",";
        }
        ss << part->debug_string(_schema->tuple_desc());
    }
    ss << "]";
    return ss.str();
}

uint32_t OlapTablePartitionParam::_compute_dist_hash(Tuple* key) const {
    uint32_t hash_val = 0;
    for (auto slot_desc : _distributed_slot_descs) {
        void* slot = nullptr;
        if (!key->is_null(slot_desc->null_indicator_offset())) {
            slot = key->get_slot(slot_desc->tuple_offset());
        }
        if (slot != nullptr) {
            hash_val = RawValue::zlib_crc32(slot, slot_desc->type(), hash_val);
        } else {
            //NULL is treat as 0 when hash
            static const int INT_VALUE = 0;
            static const TypeDescriptor INT_TYPE(TYPE_INT);
            hash_val = RawValue::zlib_crc32(&INT_VALUE, INT_TYPE, hash_val);
        }
    }
    return hash_val;
}

} // namespace doris
