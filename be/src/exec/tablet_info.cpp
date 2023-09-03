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

#include <butil/fast_rand.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/descriptors.pb.h>
#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <ostream>

#include "olap/tablet_schema.h"
#include "runtime/descriptors.h"
#include "runtime/large_int_value.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/types.h"
#include "util/hash_util.hpp"
#include "util/string_parser.hpp"
#include "util/string_util.h"
#include "vec/common/string_ref.h"
#include "vec/exprs/vexpr.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

void OlapTableIndexSchema::to_protobuf(POlapTableIndexSchema* pindex) const {
    pindex->set_id(index_id);
    pindex->set_schema_hash(schema_hash);
    for (auto slot : slots) {
        pindex->add_columns(slot->col_name());
    }
    for (auto column : columns) {
        column->to_schema_pb(pindex->add_columns_desc());
    }
    for (auto index : indexes) {
        index->to_schema_pb(pindex->add_indexes_desc());
    }
}

Status OlapTableSchemaParam::init(const POlapTableSchemaParam& pschema) {
    _db_id = pschema.db_id();
    _table_id = pschema.table_id();
    _version = pschema.version();
    _is_partial_update = pschema.partial_update();
    _is_strict_mode = pschema.is_strict_mode();

    for (auto& col : pschema.partial_update_input_columns()) {
        _partial_update_input_columns.insert(col);
    }
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
            if (_is_partial_update && _partial_update_input_columns.count(col) == 0) {
                continue;
            }
            auto it = slots_map.find(col);
            if (it == std::end(slots_map)) {
                return Status::InternalError("unknown index column, column={}", col);
            }
            index->slots.emplace_back(it->second);
        }
        for (auto& pcolumn_desc : p_index.columns_desc()) {
            TabletColumn* tc = _obj_pool.add(new TabletColumn());
            tc->init_from_pb(pcolumn_desc);
            index->columns.emplace_back(tc);
        }
        for (auto& pindex_desc : p_index.indexes_desc()) {
            TabletIndex* ti = _obj_pool.add(new TabletIndex());
            ti->init_from_pb(pindex_desc);
            index->indexes.emplace_back(ti);
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
    _is_partial_update = tschema.is_partial_update;
    if (tschema.__isset.is_strict_mode) {
        _is_strict_mode = tschema.is_strict_mode;
    }

    for (auto& tcolumn : tschema.partial_update_input_columns) {
        _partial_update_input_columns.insert(tcolumn);
    }
    std::map<std::string, SlotDescriptor*> slots_map;
    _tuple_desc = _obj_pool.add(new TupleDescriptor(tschema.tuple_desc));
    for (auto& t_slot_desc : tschema.slot_descs) {
        auto slot_desc = _obj_pool.add(new SlotDescriptor(t_slot_desc));
        _tuple_desc->add_slot(slot_desc);
        slots_map.emplace(to_lower(slot_desc->col_name()), slot_desc);
    }

    for (auto& t_index : tschema.indexes) {
        auto index = _obj_pool.add(new OlapTableIndexSchema());
        index->index_id = t_index.id;
        index->schema_hash = t_index.schema_hash;
        for (auto& col : t_index.columns) {
            if (_is_partial_update && _partial_update_input_columns.count(col) == 0) {
                continue;
            }
            auto it = slots_map.find(to_lower(col));
            if (it == std::end(slots_map)) {
                return Status::InternalError("unknown index column, column={}", col);
            }
            index->slots.emplace_back(it->second);
        }
        if (t_index.__isset.columns_desc) {
            for (auto& tcolumn_desc : t_index.columns_desc) {
                TabletColumn* tc = _obj_pool.add(new TabletColumn());
                tc->init_from_thrift(tcolumn_desc);
                index->columns.emplace_back(tc);
            }
        }
        if (t_index.__isset.indexes_desc) {
            for (auto& tindex_desc : t_index.indexes_desc) {
                std::vector<int32_t> column_unique_ids(tindex_desc.columns.size());
                for (size_t i = 0; i < tindex_desc.columns.size(); i++) {
                    auto it = slots_map.find(to_lower(tindex_desc.columns[i]));
                    if (it != std::end(slots_map)) {
                        column_unique_ids[i] = it->second->col_unique_id();
                    }
                }
                TabletIndex* ti = _obj_pool.add(new TabletIndex());
                ti->init_from_thrift(tindex_desc, column_unique_ids);
                index->indexes.emplace_back(ti);
            }
        }
        if (t_index.__isset.where_clause) {
            RETURN_IF_ERROR(
                    vectorized::VExpr::create_expr_tree(t_index.where_clause, index->where_clause));
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
    pschema->set_partial_update(_is_partial_update);
    pschema->set_is_strict_mode(_is_strict_mode);
    for (auto col : _partial_update_input_columns) {
        *pschema->add_partial_update_input_columns() = col;
    }
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

VOlapTablePartitionParam::VOlapTablePartitionParam(std::shared_ptr<OlapTableSchemaParam>& schema,
                                                   const TOlapTablePartitionParam& t_param)
        : _schema(schema),
          _t_param(t_param),
          _slots(_schema->tuple_desc()->slots()),
          _mem_tracker(std::make_unique<MemTracker>("OlapTablePartitionParam")) {
    for (auto slot : _slots) {
        _partition_block.insert(
                {slot->get_empty_mutable_column(), slot->get_data_type_ptr(), slot->col_name()});
    }
}

VOlapTablePartitionParam::~VOlapTablePartitionParam() {
    _mem_tracker->release(_mem_usage);
}

Status VOlapTablePartitionParam::init() {
    std::vector<std::string> slot_column_names;
    for (auto slot_desc : _schema->tuple_desc()->slots()) {
        slot_column_names.emplace_back(slot_desc->col_name());
    }

    auto find_slot_locs = [&slot_column_names](const std::string& slot_name,
                                               std::vector<uint16_t>& locs,
                                               const std::string& column_type) {
        auto it = std::find(slot_column_names.begin(), slot_column_names.end(), slot_name);
        if (it == slot_column_names.end()) {
            return Status::InternalError("{} column not found, column ={}", column_type, slot_name);
        }
        locs.emplace_back(it - slot_column_names.begin());
        return Status::OK();
    };

    if (_t_param.__isset.partition_columns) {
        for (auto& part_col : _t_param.partition_columns) {
            RETURN_IF_ERROR(find_slot_locs(part_col, _partition_slot_locs, "partition"));
        }
    }

    _partitions_map.reset(
            new std::map<BlockRow*, VOlapTablePartition*, VOlapTablePartKeyComparator>(
                    VOlapTablePartKeyComparator(_partition_slot_locs)));
    if (_t_param.__isset.distributed_columns) {
        for (auto& col : _t_param.distributed_columns) {
            RETURN_IF_ERROR(find_slot_locs(col, _distributed_slot_locs, "distributed"));
        }
    }
    if (_distributed_slot_locs.empty()) {
        _compute_tablet_index = [](BlockRow* key, int64_t num_buckets) -> uint32_t {
            return butil::fast_rand() % num_buckets;
        };
    } else {
        _compute_tablet_index = [this](BlockRow* key, int64_t num_buckets) -> uint32_t {
            uint32_t hash_val = 0;
            for (int i = 0; i < _distributed_slot_locs.size(); ++i) {
                auto slot_desc = _slots[_distributed_slot_locs[i]];
                auto& column = key->first->get_by_position(_distributed_slot_locs[i]).column;
                auto val = column->get_data_at(key->second);
                if (val.data != nullptr) {
                    hash_val = RawValue::zlib_crc32(val.data, val.size, slot_desc->type().type,
                                                    hash_val);
                } else {
                    hash_val = HashUtil::zlib_crc_hash_null(hash_val);
                }
            }
            return hash_val % num_buckets;
        };
    }

    DCHECK(!_t_param.partitions.empty()) << "must have at least 1 partition";
    _is_in_partition = _t_param.partitions[0].__isset.in_keys;

    // initial partitions
    for (int i = 0; i < _t_param.partitions.size(); ++i) {
        const TOlapTablePartition& t_part = _t_param.partitions[i];
        auto part = _obj_pool.add(new VOlapTablePartition(&_partition_block));
        part->id = t_part.id;
        part->is_mutable = t_part.is_mutable;

        if (!_is_in_partition) {
            if (t_part.__isset.start_keys) {
                RETURN_IF_ERROR(_create_partition_keys(t_part.start_keys, &part->start_key));
            }

            if (t_part.__isset.end_keys) {
                RETURN_IF_ERROR(_create_partition_keys(t_part.end_keys, &part->end_key));
            }
        } else {
            for (const auto& keys : t_part.in_keys) {
                RETURN_IF_ERROR(_create_partition_keys(
                        keys, &part->in_keys.emplace_back(&_partition_block, -1)));
            }
            if (t_part.__isset.is_default_partition && t_part.is_default_partition) {
                _default_partition = part;
            }
        }

        part->num_buckets = t_part.num_buckets;
        auto num_indexes = _schema->indexes().size();
        if (t_part.indexes.size() != num_indexes) {
            return Status::InternalError(
                    "number of partition's index is not equal with schema's"
                    ", num_part_indexes={}, num_schema_indexes={}",
                    t_part.indexes.size(), num_indexes);
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
                return Status::InternalError(
                        "partition's index is not equal with schema's"
                        ", part_index={}, schema_index={}",
                        part->indexes[j].index_id, _schema->indexes()[j]->index_id);
            }
        }
        _partitions.emplace_back(part);
        if (_is_in_partition) {
            for (auto& in_key : part->in_keys) {
                _partitions_map->emplace(&in_key, part);
            }
        } else {
            _partitions_map->emplace(&part->end_key, part);
        }
    }

    _mem_usage = _partition_block.allocated_bytes();
    _mem_tracker->consume(_mem_usage);
    return Status::OK();
}

bool VOlapTablePartitionParam::find_partition(BlockRow* block_row,
                                              const VOlapTablePartition** partition) const {
    auto it = _is_in_partition ? _partitions_map->find(block_row)
                               : _partitions_map->upper_bound(block_row);
    // for list partition it might result in default partition
    if (_is_in_partition) {
        *partition = (it != _partitions_map->end()) ? it->second : _default_partition;
        it = _partitions_map->end();
    }
    if (it != _partitions_map->end() && _part_contains(it->second, block_row)) {
        *partition = it->second;
    }
    return (*partition != nullptr);
}

uint32_t VOlapTablePartitionParam::find_tablet(BlockRow* block_row,
                                               const VOlapTablePartition& partition) const {
    return _compute_tablet_index(block_row, partition.num_buckets);
}

Status VOlapTablePartitionParam::_create_partition_keys(const std::vector<TExprNode>& t_exprs,
                                                        BlockRow* part_key) {
    for (int i = 0; i < t_exprs.size(); i++) {
        RETURN_IF_ERROR(_create_partition_key(t_exprs[i], part_key, _partition_slot_locs[i]));
    }
    return Status::OK();
}

Status VOlapTablePartitionParam::_create_partition_key(const TExprNode& t_expr, BlockRow* part_key,
                                                       uint16_t pos) {
    auto column = std::move(*part_key->first->get_by_position(pos).column).mutate();
    switch (t_expr.node_type) {
    case TExprNodeType::DATE_LITERAL: {
        if (TypeDescriptor::from_thrift(t_expr.type).is_date_v2_type()) {
            vectorized::DateV2Value<doris::vectorized::DateV2ValueType> dt;
            if (!dt.from_date_str(t_expr.date_literal.value.c_str(),
                                  t_expr.date_literal.value.size())) {
                std::stringstream ss;
                ss << "invalid date literal in partition column, date=" << t_expr.date_literal;
                return Status::InternalError(ss.str());
            }
            column->insert_data(reinterpret_cast<const char*>(&dt), 0);
        } else if (TypeDescriptor::from_thrift(t_expr.type).is_datetime_v2_type()) {
            vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType> dt;
            if (!dt.from_date_str(t_expr.date_literal.value.c_str(),
                                  t_expr.date_literal.value.size())) {
                std::stringstream ss;
                ss << "invalid date literal in partition column, date=" << t_expr.date_literal;
                return Status::InternalError(ss.str());
            }
            column->insert_data(reinterpret_cast<const char*>(&dt), 0);
        } else {
            vectorized::VecDateTimeValue dt;
            if (!dt.from_date_str(t_expr.date_literal.value.c_str(),
                                  t_expr.date_literal.value.size())) {
                std::stringstream ss;
                ss << "invalid date literal in partition column, date=" << t_expr.date_literal;
                return Status::InternalError(ss.str());
            }
            column->insert_data(reinterpret_cast<const char*>(&dt), 0);
        }
        break;
    }
    case TExprNodeType::INT_LITERAL: {
        switch (t_expr.type.types[0].scalar_type.type) {
        case TPrimitiveType::TINYINT: {
            int8_t value = t_expr.int_literal.value;
            column->insert_data(reinterpret_cast<const char*>(&value), 0);
            break;
        }
        case TPrimitiveType::SMALLINT: {
            int16_t value = t_expr.int_literal.value;
            column->insert_data(reinterpret_cast<const char*>(&value), 0);
            break;
        }
        case TPrimitiveType::INT: {
            int32_t value = t_expr.int_literal.value;
            column->insert_data(reinterpret_cast<const char*>(&value), 0);
            break;
        }
        default:
            int64_t value = t_expr.int_literal.value;
            column->insert_data(reinterpret_cast<const char*>(&value), 0);
        }
        break;
    }
    case TExprNodeType::LARGE_INT_LITERAL: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        __int128 value = StringParser::string_to_int<__int128>(
                t_expr.large_int_literal.value.c_str(), t_expr.large_int_literal.value.size(),
                &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            value = MAX_INT128;
        }
        column->insert_data(reinterpret_cast<const char*>(&value), 0);
        break;
    }
    case TExprNodeType::STRING_LITERAL: {
        int len = t_expr.string_literal.value.size();
        const char* str_val = t_expr.string_literal.value.c_str();
        column->insert_data(str_val, len);
        break;
    }
    case TExprNodeType::BOOL_LITERAL: {
        column->insert_data(reinterpret_cast<const char*>(&t_expr.bool_literal.value), 0);
        break;
    }
    default: {
        return Status::InternalError("unsupported partition column node type, type={}",
                                     t_expr.node_type);
    }
    }
    part_key->second = column->size() - 1;
    return Status::OK();
}

} // namespace doris
