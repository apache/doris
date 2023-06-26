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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/descriptors.pb.h>

#include <cstdint>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
class MemTracker;
class SlotDescriptor;
class TExprNode;
class TabletColumn;
class TabletIndex;
class TupleDescriptor;

struct OlapTableIndexSchema {
    int64_t index_id;
    std::vector<SlotDescriptor*> slots;
    int32_t schema_hash;
    std::vector<TabletColumn*> columns;
    std::vector<TabletIndex*> indexes;
    vectorized::VExprContextSPtr where_clause;

    void to_protobuf(POlapTableIndexSchema* pindex) const;
};

class OlapTableSchemaParam {
public:
    OlapTableSchemaParam() = default;
    ~OlapTableSchemaParam() noexcept = default;

    Status init(const TOlapTableSchemaParam& tschema);
    Status init(const POlapTableSchemaParam& pschema);

    int64_t db_id() const { return _db_id; }
    int64_t table_id() const { return _table_id; }
    int64_t version() const { return _version; }

    TupleDescriptor* tuple_desc() const { return _tuple_desc; }
    const std::vector<OlapTableIndexSchema*>& indexes() const { return _indexes; }

    void to_protobuf(POlapTableSchemaParam* pschema) const;

    // NOTE: this function is not thread-safe.
    POlapTableSchemaParam* to_protobuf() const {
        if (_proto_schema == nullptr) {
            _proto_schema = _obj_pool.add(new POlapTableSchemaParam());
            to_protobuf(_proto_schema);
        }
        return _proto_schema;
    }

    bool is_partial_update() const { return _is_partial_update; }
    std::set<std::string> partial_update_input_columns() const {
        return _partial_update_input_columns;
    }
    bool is_strict_mode() const { return _is_strict_mode; }
    std::string debug_string() const;

private:
    int64_t _db_id;
    int64_t _table_id;
    int64_t _version;

    TupleDescriptor* _tuple_desc = nullptr;
    mutable POlapTableSchemaParam* _proto_schema = nullptr;
    std::vector<OlapTableIndexSchema*> _indexes;
    mutable ObjectPool _obj_pool;
    bool _is_partial_update = false;
    std::set<std::string> _partial_update_input_columns;
    bool _is_strict_mode = false;
};

using OlapTableIndexTablets = TOlapTableIndexTablets;
// struct TOlapTableIndexTablets {
//     1: required i64 index_id
//     2: required list<i64> tablets
// }

using BlockRow = std::pair<vectorized::Block*, int32_t>;
using VecBlock = vectorized::Block;

struct VOlapTablePartition {
    int64_t id = 0;
    BlockRow start_key;
    BlockRow end_key;
    std::vector<BlockRow> in_keys;
    int64_t num_buckets = 0;
    std::vector<OlapTableIndexTablets> indexes;
    bool is_mutable;

    VOlapTablePartition(vectorized::Block* partition_block)
            : start_key {partition_block, -1}, end_key {partition_block, -1} {}
};

class VOlapTablePartKeyComparator {
public:
    VOlapTablePartKeyComparator(const std::vector<uint16_t>& slot_locs) : _slot_locs(slot_locs) {}

    // return true if lhs < rhs
    // 'row' is -1 mean
    bool operator()(const BlockRow* lhs, const BlockRow* rhs) const {
        if (lhs->second == -1) {
            return false;
        } else if (rhs->second == -1) {
            return true;
        }

        for (auto slot_loc : _slot_locs) {
            auto res = lhs->first->get_by_position(slot_loc).column->compare_at(
                    lhs->second, rhs->second, *rhs->first->get_by_position(slot_loc).column, -1);
            if (res != 0) {
                return res < 0;
            }
        }
        // equal, return false
        return false;
    }

private:
    const std::vector<uint16_t>& _slot_locs;
};

// store an olap table's tablet information
class VOlapTablePartitionParam {
public:
    VOlapTablePartitionParam(std::shared_ptr<OlapTableSchemaParam>& schema,
                             const TOlapTablePartitionParam& param);

    ~VOlapTablePartitionParam();

    Status init();

    int64_t db_id() const { return _t_param.db_id; }
    int64_t table_id() const { return _t_param.table_id; }
    int64_t version() const { return _t_param.version; }

    // return true if we found this block_row in partition
    bool find_partition(BlockRow* block_row, const VOlapTablePartition** partition) const;

    uint32_t find_tablet(BlockRow* block_row, const VOlapTablePartition& partition) const;

    const std::vector<VOlapTablePartition*>& get_partitions() const { return _partitions; }

private:
    Status _create_partition_keys(const std::vector<TExprNode>& t_exprs, BlockRow* part_key);

    Status _create_partition_key(const TExprNode& t_expr, BlockRow* part_key, uint16_t pos);

    std::function<uint32_t(BlockRow*, int64_t)> _compute_tablet_index;

    // check if this partition contain this key
    bool _part_contains(VOlapTablePartition* part, BlockRow* key) const {
        // start_key.second == -1 means only single partition
        VOlapTablePartKeyComparator comparator(_partition_slot_locs);
        return part->start_key.second == -1 || !comparator(key, &part->start_key);
    }

    // this partition only valid in this schema
    std::shared_ptr<OlapTableSchemaParam> _schema;
    TOlapTablePartitionParam _t_param;

    const std::vector<SlotDescriptor*>& _slots;
    std::vector<uint16_t> _partition_slot_locs;
    std::vector<uint16_t> _distributed_slot_locs;

    ObjectPool _obj_pool;
    vectorized::Block _partition_block;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::vector<VOlapTablePartition*> _partitions;
    std::unique_ptr<std::map<BlockRow*, VOlapTablePartition*, VOlapTablePartKeyComparator>>
            _partitions_map;

    bool _is_in_partition = false;
    uint32_t _mem_usage = 0;
    // only works when using list partition, the resource is owned by _partitions
    VOlapTablePartition* _default_partition = nullptr;
};

using TabletLocation = TTabletLocation;
// struct TTabletLocation {
//     1: required i64 tablet_id
//     2: required list<i64> node_ids
// }

class OlapTableLocationParam {
public:
    OlapTableLocationParam(const TOlapTableLocationParam& t_param) : _t_param(t_param) {
        for (auto& location : _t_param.tablets) {
            _tablets.emplace(location.tablet_id, &location);
        }
    }

    int64_t db_id() const { return _t_param.db_id; }
    int64_t table_id() const { return _t_param.table_id; }
    int64_t version() const { return _t_param.version; }

    TabletLocation* find_tablet(int64_t tablet_id) const {
        auto it = _tablets.find(tablet_id);
        if (it != std::end(_tablets)) {
            return it->second;
        }
        return nullptr;
    }

private:
    TOlapTableLocationParam _t_param;

    std::unordered_map<int64_t, TabletLocation*> _tablets;
};

struct NodeInfo {
    int64_t id;
    int64_t option;
    std::string host;
    int32_t brpc_port;

    NodeInfo() = default;

    NodeInfo(const TNodeInfo& tnode)
            : id(tnode.id),
              option(tnode.option),
              host(tnode.host),
              brpc_port(tnode.async_internal_port) {}
};

class DorisNodesInfo {
public:
    DorisNodesInfo() = default;
    DorisNodesInfo(const TPaloNodesInfo& t_nodes) {
        for (auto& node : t_nodes.nodes) {
            _nodes.emplace(node.id, node);
        }
    }
    void setNodes(const TPaloNodesInfo& t_nodes) {
        _nodes.clear();
        for (auto& node : t_nodes.nodes) {
            _nodes.emplace(node.id, node);
        }
    }
    const NodeInfo* find_node(int64_t id) const {
        auto it = _nodes.find(id);
        if (it != std::end(_nodes)) {
            return &it->second;
        }
        return nullptr;
    }

    const std::unordered_map<int64_t, NodeInfo>& nodes_info() { return _nodes; }

private:
    std::unordered_map<int64_t, NodeInfo> _nodes;
};

} // namespace doris
