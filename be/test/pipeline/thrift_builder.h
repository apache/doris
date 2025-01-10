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

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "common/status.h"
#include "pipeline/exec/exchange_sink_buffer.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "runtime/runtime_state.h"
#include "udf/udf.h"
#include "vec/sink/writer/vhive_utils.h"

namespace doris::pipeline {

class TQueryOptionsBuilder {
public:
    explicit TQueryOptionsBuilder() : _query_options() {}

    TQueryOptionsBuilder& set_batch_size(int batch_size) {
        _query_options.batch_size = batch_size;
        return *this;
    }
    TQueryOptionsBuilder& set_execution_timeout(int execution_timeout) {
        _query_options.execution_timeout = execution_timeout;
        return *this;
    }
    TQueryOptionsBuilder& set_mem_limit(int64_t mem_limit) {
        _query_options.mem_limit = mem_limit;
        return *this;
    }
    TQueryOptionsBuilder& set_query_type(TQueryType::type& query_type) {
        _query_options.query_type = query_type;
        return *this;
    }
    TQueryOptionsBuilder& set_be_exec_version(int be_exec_version) {
        _query_options.be_exec_version = be_exec_version;
        return *this;
    }
    TQueryOptionsBuilder& set_enable_local_exchange(bool enable_local_exchange) {
        _query_options.enable_local_exchange = enable_local_exchange;
        return *this;
    }
    TQueryOptionsBuilder& set_enable_new_shuffle_hash_method(bool enable_new_shuffle_hash_method) {
        _query_options.enable_new_shuffle_hash_method = enable_new_shuffle_hash_method;
        return *this;
    }
    TQueryOptionsBuilder& set_enable_local_shuffle(bool enable_local_shuffle) {
        _query_options.enable_local_shuffle = enable_local_shuffle;
        return *this;
    }
    TQueryOptionsBuilder& set_runtime_filter_wait_infinitely(bool runtime_filter_wait_infinitely) {
        _query_options.runtime_filter_wait_infinitely = runtime_filter_wait_infinitely;
        return *this;
    }
    TQueryOptionsBuilder& set_enable_local_merge_sort(bool enable_local_merge_sort) {
        _query_options.enable_local_merge_sort = enable_local_merge_sort;
        return *this;
    }

    TQueryOptions& build() { return _query_options; }

    TQueryOptionsBuilder(const TQueryOptionsBuilder&) = delete;
    void operator=(const TQueryOptionsBuilder&) = delete;

private:
    TQueryOptions _query_options;
};

class TPlanNodeBuilder {
public:
    explicit TPlanNodeBuilder(TPlanNodeId node_id, TPlanNodeType::type node_type,
                              int num_children = 0, int64_t limit = -1, bool compact_data = true)
            : _plan_node() {
        _plan_node.node_id = node_id;
        _plan_node.node_type = node_type;
        _plan_node.num_children = num_children;
        _plan_node.limit = limit;
        _plan_node.compact_data = compact_data;
    }

    TPlanNodeBuilder& set_is_serial_operator(bool is_serial_operator) {
        _plan_node.is_serial_operator = is_serial_operator;
        return *this;
    }
    TPlanNodeBuilder& set_exchange_node(TExchangeNode& node) {
        _plan_node.exchange_node = node;
        return *this;
    }
    TPlanNodeBuilder& append_row_tuples(TTupleId tuple_id, bool nullable) {
        _plan_node.row_tuples.emplace_back(tuple_id);
        _plan_node.nullable_tuples.emplace_back(nullable);
        return *this;
    }

    TPlanNode& build() { return _plan_node; }

    TPlanNodeBuilder(const TPlanNodeBuilder&) = delete;
    void operator=(const TPlanNodeBuilder&) = delete;

private:
    TPlanNode _plan_node;
};

class TExchangeNodeBuilder {
public:
    explicit TExchangeNodeBuilder() : _plan_node() {}

    TExchangeNodeBuilder& set_partition_type(TPartitionType::type partition_type) {
        _plan_node.partition_type = partition_type;
        return *this;
    }
    TExchangeNodeBuilder& append_input_row_tuples(TTupleId tuple_id) {
        _plan_node.input_row_tuples.emplace_back(tuple_id);
        return *this;
    }
    TExchangeNode& build() { return _plan_node; }
    TExchangeNodeBuilder(const TExchangeNodeBuilder&) = delete;
    void operator=(const TExchangeNodeBuilder&) = delete;

private:
    TExchangeNode _plan_node;
};

class TDataSinkBuilder {
public:
    explicit TDataSinkBuilder(TDataSinkType::type type) : _sink() { _sink.type = type; }

    TDataSinkBuilder& set_stream_sink(TDataStreamSink& stream_sink) {
        _sink.stream_sink = stream_sink;
        return *this;
    }
    TDataSink& build() { return _sink; }

    TDataSinkBuilder(const TDataSinkBuilder&) = delete;
    void operator=(const TDataSinkBuilder&) = delete;

private:
    TDataSink _sink;
};

class TDataStreamSinkBuilder {
public:
    explicit TDataStreamSinkBuilder(TPlanNodeId dest_node_id, TDataPartition output_partition)
            : _sink() {
        _sink.dest_node_id = dest_node_id;
        _sink.output_partition = output_partition;
    }

    TDataStreamSink& build() { return _sink; }

    TDataStreamSinkBuilder(const TDataStreamSinkBuilder&) = delete;
    void operator=(const TDataStreamSinkBuilder&) = delete;

private:
    TDataStreamSink _sink;
};

class TPlanFragmentDestinationBuilder {
public:
    explicit TPlanFragmentDestinationBuilder(TUniqueId fragment_instance_id, TNetworkAddress server,
                                             TNetworkAddress brpc_server)
            : _dest() {
        _dest.fragment_instance_id = fragment_instance_id;
        _dest.server = server;
        _dest.brpc_server = brpc_server;
    }

    TPlanFragmentDestination& build() { return _dest; }

    TPlanFragmentDestinationBuilder(const TPlanFragmentDestinationBuilder&) = delete;
    void operator=(const TPlanFragmentDestinationBuilder&) = delete;

private:
    TPlanFragmentDestination _dest;
};

class TTupleDescriptorBuilder {
public:
    explicit TTupleDescriptorBuilder() : _desc() {
        _desc.byteSize = 0;
        _desc.numNullBytes = 0;
    }

    TTupleDescriptorBuilder& set_id(TTupleId id) {
        _desc.id = id;
        return *this;
    }

    TTupleDescriptor& build() { return _desc; }
    TTupleDescriptorBuilder(const TTupleDescriptorBuilder&) = delete;
    void operator=(const TTupleDescriptorBuilder&) = delete;

private:
    TTupleDescriptor _desc;
};

class TSlotDescriptorBuilder {
public:
    explicit TSlotDescriptorBuilder() : _desc() {
        _desc.columnPos = -1;
        _desc.nullIndicatorByte = 0;
    }
    TSlotDescriptorBuilder& set_id(TTupleId id) {
        _desc.id = id;
        return *this;
    }
    TSlotDescriptorBuilder& set_parent(TTupleDescriptor& parent) {
        _desc.parent = parent.id;
        return *this;
    }
    TSlotDescriptorBuilder& set_slotType(TTypeDesc& slotType) {
        _desc.slotType = slotType;
        return *this;
    }
    TSlotDescriptorBuilder& set_nullIndicatorBit(int nullIndicatorBit) {
        _desc.nullIndicatorBit = nullIndicatorBit;
        return *this;
    }
    TSlotDescriptorBuilder& set_byteOffset(int byteOffset) {
        _desc.byteOffset = byteOffset;
        return *this;
    }
    TSlotDescriptorBuilder& set_slotIdx(int slotIdx) {
        _desc.slotIdx = slotIdx;
        return *this;
    }
    TSlotDescriptorBuilder& set_isMaterialized(bool isMaterialized) {
        _desc.isMaterialized = isMaterialized;
        return *this;
    }
    TSlotDescriptorBuilder& set_colName(std::string colName) {
        _desc.colName = colName;
        return *this;
    }

    TSlotDescriptor& build() { return _desc; }
    TSlotDescriptorBuilder(const TSlotDescriptorBuilder&) = delete;
    void operator=(const TSlotDescriptorBuilder&) = delete;

private:
    TSlotDescriptor _desc;
};

class TTypeDescBuilder {
public:
    explicit TTypeDescBuilder() : _desc() {}

    TTypeDescBuilder& set_types(TTypeNode type_node) {
        _desc.types.push_back(type_node);
        return *this;
    }

    TTypeDesc& build() { return _desc; }
    TTypeDescBuilder(const TTypeDescBuilder&) = delete;
    void operator=(const TTypeDescBuilder&) = delete;

private:
    TTypeDesc _desc;
};

class TTypeNodeBuilder {
public:
    explicit TTypeNodeBuilder() : _desc() {}

    TTypeNodeBuilder& set_type(TTypeNodeType::type type) {
        _desc.type = type;
        return *this;
    }

    TTypeNodeBuilder& set_scalar_type(TPrimitiveType::type type, int len = 0, int precision = 0,
                                      int scale = 0) {
        TScalarType scalar_type;
        scalar_type.type = type;
        scalar_type.__set_len(len);
        scalar_type.__set_precision(precision);
        scalar_type.__set_scale(scale);
        _desc.__set_scalar_type(scalar_type);
        return *this;
    }

    TTypeNode& build() { return _desc; }
    TTypeNodeBuilder(const TTypeNodeBuilder&) = delete;
    void operator=(const TTypeNodeBuilder&) = delete;

private:
    TTypeNode _desc;
};

class TDescriptorTableBuilder {
public:
    explicit TDescriptorTableBuilder() : _desc() {}

    TDescriptorTableBuilder& append_slotDescriptors(TSlotDescriptor& desc) {
        _desc.slotDescriptors.push_back(desc);
        return *this;
    }
    TDescriptorTableBuilder& append_tupleDescriptors(TTupleDescriptor& desc) {
        _desc.tupleDescriptors.push_back(desc);
        return *this;
    }
    TDescriptorTableBuilder& append_tableDescriptors(TTableDescriptor& desc) {
        _desc.tableDescriptors.push_back(desc);
        return *this;
    }

    TDescriptorTable& build() { return _desc; }
    TDescriptorTableBuilder(const TDescriptorTableBuilder&) = delete;
    void operator=(const TDescriptorTableBuilder&) = delete;

private:
    TDescriptorTable _desc;
};

class TDataPartitionBuilder {
public:
    explicit TDataPartitionBuilder(TPartitionType::type type) : _partition() {
        _partition.type = type;
    }

    TDataPartitionBuilder& append_partition_exprs(TExpr expr) {
        _partition.partition_exprs.push_back(expr);
        return *this;
    }
    TDataPartitionBuilder& append_partition_infos(TRangePartition info) {
        _partition.partition_infos.push_back(info);
        return *this;
    }

    TDataPartition& build() { return _partition; }
    TDataPartitionBuilder(const TDataPartitionBuilder&) = delete;
    void operator=(const TDataPartitionBuilder&) = delete;

private:
    TDataPartition _partition;
};

} // namespace doris::pipeline
